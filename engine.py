import json
import random
import threading
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple, cast

from config import MIN_USDC_RESERVE, STATE_PATH, SYMBOL, TICK_SIZE, VERSION
from types_ import LogEntry, OrderInfo
from logger import log_event, log_fill


def _notify(msg: str) -> None:
    """Wrapper seguro para notificaciones Telegram — no falla si el bot no está activo."""
    try:
        from telegram_bot import notify
        notify(msg)
    except Exception:
        pass
from http_client import send_request
from api import (
    _parse_balances,
    _price_key,
    cancel_order,
    cancel_all_orders,
    check_balances_for_grid,
    fmt_amount,
    get_active_orders,
    get_all_balances,
    get_current_price,
    get_historical_orders,
    get_order_by_id,
    place_order as api_place_order,
)


class GridEngine:

    def __init__(
        self,
        steps_each_side: Optional[int] = None,
        step_percent: Decimal = Decimal("0"),
        base_size: Decimal = Decimal("0"),
        initial_price: Optional[Decimal] = None,
        levels_below: Optional[int] = None,
        levels_above: Optional[int] = None,
    ):
        legacy_steps = int(steps_each_side) if steps_each_side is not None else 0
        self.levels_below: int       = int(levels_below) if levels_below is not None else legacy_steps
        self.levels_above: int       = int(levels_above) if levels_above is not None else legacy_steps
        self.steps_each_side: int    = legacy_steps or max(self.levels_below, self.levels_above)
        self.step_percent: Decimal   = Decimal(str(step_percent))
        self.base_size: Decimal      = Decimal(str(base_size))
        self.initial_price: Optional[Decimal] = (
            Decimal(str(initial_price)) if initial_price is not None else None
        )

        self.center_price: Optional[Decimal] = None
        self.step: Optional[Decimal]         = None
        self.base_step: Optional[Decimal]    = None  # para restaurar el step original tras un trailing extendido
        self.levels: List[Decimal]           = []
        self.extended_levels: Dict[str, Decimal] = {}  # Niveles creados por trailing_down_extended.
        self.active_orders: Dict[str, OrderInfo] = {}
        self.last_fill_side: Optional[str]   = None  # "buy" o "sell"
        self.last_fill_price: Optional[Decimal] = None  # último nivel ejecutado
        self.current_price: Optional[Decimal] = None  # último precio conocido

        # Historial de fills en memoria para el submenú de monitorización
        # Cada entrada: {"side": str, "price": str, "order_id": str, "ts": float}
        self.fill_history: List[Dict[str, Any]] = []

        self._stop_event = threading.Event()
        self._state_lock = threading.RLock()

        # Guarda el estado de trailings
        self.trailing_up_enabled: bool = True
        self.trailing_down_mode: str = 'on'  # 'off' | 'on' | 'extended'
        self.trailing_down_enabled: bool = True  # compatibilidad con estado antiguo
        self._trailing_down_extended_drops: int = 0

    def _is_virtual_order(self, info: Optional["OrderInfo"]) -> bool:
        """Retorna True si el snapshot corresponde a un centinela virtual."""
        return bool(info) and info.get("order_id") == "virtual"

    def _record_real_fill(self, price_key: str, info: "OrderInfo") -> None:
        """
        Registra solo fills reales del exchange en memoria y en fills.csv.
        Las activaciones virtuales sirven para rebalancear, pero no cuentan como fill.
        """
        self.fill_history.append({
            "side": info["side"],
            "price": price_key,
            "order_id": info["order_id"],
            "ts": time.time(),
        })
        log_fill(info["side"], price_key, fmt_amount(self.base_size))

    def _normalise_trailing_down_mode(self, down: object) -> str:
        """Normaliza el modo de trailing down a 'off', 'on' o 'extended'."""
        if isinstance(down, bool):
            return 'on' if down else 'off'

        value = str(down).strip().lower()
        if value in {'off', 'on', 'extended', 'extendido'}:
            return 'extended' if value == 'extendido' else value
        return 'off'

    def _order_size(self, info: OrderInfo) -> Decimal:
        raw = info.get('size', self.base_size)
        try:
            return Decimal(str(raw))
        except Exception:
            return self.base_size

    def _is_extended_order(self, info: Optional[OrderInfo]) -> bool:
        """Retorna True si la orden pertenece al grid extendido inferior."""
        return bool(info) and bool(info.get("extended"))

    def _extended_order_size(self) -> Decimal:
        """Tamaño fijo de las órdenes extended: 50% del base_size."""
        return self.base_size * Decimal("0.5")

    def _get_base_step_locked(self) -> Decimal:
        """Step principal. No se muta durante trailing_down_extended."""
        step = self.base_step if self.base_step is not None else self.step
        if step is None:
            raise RuntimeError("step/base_step no inicializado")
        return Decimal(str(step))

    def _decimal_from_meta(self, value: object, default: Decimal) -> Decimal:
        try:
            parsed = Decimal(str(value))
            if parsed > 0:
                return parsed
        except Exception:
            pass
        return default

    def _price_from_meta(self, value: object, default: Decimal) -> Decimal:
        try:
            return Decimal(str(value)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        except Exception:
            return default

    def _apply_order_metadata(
        self,
        order_info: OrderInfo,
        metadata: Optional[Dict[str, Any]],
    ) -> OrderInfo:
        if not metadata:
            return order_info

        for meta_key, meta_value in metadata.items():
            if meta_value is not None:
                order_info[meta_key] = meta_value
        return order_info

    def _mark_extended_level_locked(self, price: Decimal, step_to_upper: Decimal) -> None:
        """Registra un nivel como extended y guarda su step hacia el nivel superior."""
        key = _price_key(price)
        self.extended_levels[key] = Decimal(str(step_to_upper))
        if price not in self.levels:
            self.levels.append(price)

    def _principal_levels_locked(self) -> List[Decimal]:
        """Niveles no extended; sirven para detectar el suelo/techo principal."""
        principal = [
            level for level in self.levels
            if _price_key(level) not in self.extended_levels
        ]
        return sorted(set(principal or self.levels))

    def _protected_empty_level_keys(self, snapshot_orders: Dict[str, OrderInfo]) -> set[str]:
        """
        Niveles que deben permanecer vacíos porque son la otra pata de una orden extended activa.
        Evita que la recuperación recree BUY/SELL duplicados dentro del grid extendido.
        """
        protected: set[str] = set()
        for info in snapshot_orders.values():
            for meta_key in ("paired_buy_price", "paired_sell_price"):
                raw_price = info.get(meta_key)
                if raw_price is None:
                    continue
                try:
                    protected.add(_price_key(Decimal(str(raw_price))))
                except Exception:
                    continue
        return protected

    def _get_available_usdc(self) -> Decimal:
        balances_resp, _ = get_all_balances()
        usdc_balance, _ = _parse_balances(balances_resp)
        available = usdc_balance - MIN_USDC_RESERVE
        return available if available > 0 else Decimal('0')

    def _find_highest_real_sell_order(self) -> Optional[Tuple[str, OrderInfo]]:
        """Devuelve la orden SELL real más alta para poder liberar saldo en trailing extendido."""
        with self._state_lock:
            candidates: List[Tuple[Decimal, str, OrderInfo]] = []
            for key, info in self.active_orders.items():
                if info.get('side') != 'sell':
                    continue
                order_id = str(info.get('order_id'))
                if order_id in {'virtual', 'pending_post_only', 'pending_manual', 'pending_cancel'}:
                    continue
                if self._is_extended_order(info):
                    continue
                try:
                    candidates.append((Decimal(key), key, self._clone_order_info(info)))
                except Exception:
                    continue

        if not candidates:
            return None

        _, key, info = max(candidates, key=lambda item: item[0])
        return key, info

    def _find_lowest_real_buy_order(
        self,
        exclude_keys: Optional[set[str]] = None,
    ) -> Optional[Tuple[str, OrderInfo]]:
        """Devuelve el BUY real más bajo para liberar USDC en trailing up."""
        excluded = exclude_keys or set()

        with self._state_lock:
            candidates: List[Tuple[Decimal, str, OrderInfo]] = []
            for key, info in self.active_orders.items():
                if key in excluded:
                    continue
                if info.get('side') != 'buy':
                    continue
                order_id = str(info.get('order_id'))
                if order_id in {'virtual', 'pending_post_only', 'pending_manual', 'pending_cancel'}:
                    continue
                try:
                    candidates.append((Decimal(key), key, self._clone_order_info(info)))
                except Exception:
                    continue

        if not candidates:
            return None

        _, key, info = min(candidates, key=lambda item: item[0])
        return key, info

    def _remove_lowest_virtual_buy_order(self) -> Optional[str]:
        """Elimina la virtual BUY que hace de suelo cuando trailing up desmonta BUYs bajos."""
        with self._state_lock:
            candidates: List[Tuple[Decimal, str]] = []
            for key, info in self.active_orders.items():
                if info.get('side') != 'buy':
                    continue
                if str(info.get('order_id')) != 'virtual':
                    continue
                try:
                    candidates.append((Decimal(key), key))
                except Exception:
                    continue

            if not candidates:
                return None

            _, floor_key = min(candidates, key=lambda item: item[0])
            self.active_orders.pop(floor_key, None)
            self.levels = [
                lvl for lvl in self.levels
                if _price_key(lvl) != floor_key
            ]
            self.extended_levels.pop(floor_key, None)
            return floor_key

    def _remove_highest_virtual_sell_order(self) -> Optional[str]:
        """Elimina la virtual SELL que hace de techo cuando trailing down desmonta SELLs altos."""
        with self._state_lock:
            candidates: List[Tuple[Decimal, str]] = []
            for key, info in self.active_orders.items():
                if info.get('side') != 'sell':
                    continue
                if str(info.get('order_id')) != 'virtual':
                    continue
                try:
                    candidates.append((Decimal(key), key))
                except Exception:
                    continue

            if not candidates:
                return None

            _, ceiling_key = max(candidates, key=lambda item: item[0])
            self.active_orders.pop(ceiling_key, None)
            self.levels = [
                lvl for lvl in self.levels
                if _price_key(lvl) != ceiling_key
            ]
            self.extended_levels.pop(ceiling_key, None)
            return ceiling_key

    def _release_usdc_for_trailing_up_buy(
        self,
        target_price: Decimal,
        target_size: Decimal,
        *,
        max_cancellations: Optional[int] = None,
        retry_delay: float = 1.0,
    ) -> bool:
        """
        Libera USDC para un BUY creado por activación de SELL virtual.

        Cancela BUYs reales desde la parte más baja de la rejilla, sean principales
        o extended. No cancela SELLs porque no liberan USDC. Se detiene cuando el
        saldo real o el saldo estimado liberado cubre el BUY objetivo. Si
        max_cancellations es None, no usa un límite fijo: el tope natural son los
        BUYs reales cancelables disponibles.
        """
        required = Decimal(str(target_price)) * Decimal(str(target_size))
        target_key = _price_key(target_price)
        excluded_keys: set[str] = {target_key}
        cancellations = 0
        floor_virtual_removed = False

        estimated_available = self._get_available_usdc()
        if estimated_available >= required:
            return True

        while estimated_available < required:
            if max_cancellations is not None and cancellations >= max_cancellations:
                log_event(
                    f"[ENGINE] Trailing up: USDC insuficiente para BUY {target_key} "
                    f"tras {cancellations} cancelaciones "
                    f"({_price_key(estimated_available)} < {_price_key(required)})",
                    "warning"
                )
                return False

            candidate = self._find_lowest_real_buy_order(exclude_keys=excluded_keys)
            if candidate is None:
                refreshed_available = self._get_available_usdc()
                if refreshed_available > estimated_available:
                    estimated_available = refreshed_available
                if estimated_available >= required:
                    return True

                log_event(
                    f"[ENGINE] Trailing up: no hay BUY real cancelable para liberar USDC "
                    f"({_price_key(estimated_available)} < {_price_key(required)})",
                    "warning"
                )
                return False

            cancel_level_key, cancel_info = candidate
            cancel_order_id = str(cancel_info["order_id"])
            cancel_size = self._order_size(cancel_info)
            try:
                cancel_price = Decimal(cancel_level_key)
            except Exception:
                cancel_price = Decimal(str(cancel_info["price"]))
            estimated_release = cancel_price * cancel_size

            with self._state_lock:
                current = self.active_orders.get(cancel_level_key)
                if current is None or current.get("order_id") != cancel_order_id:
                    excluded_keys.add(cancel_level_key)
                    continue
                current["order_id"] = "pending_cancel"

            log_event(
                f"[ENGINE] Trailing up: cancelando BUY bajo {cancel_level_key} "
                f"para liberar USDC antes de BUY {target_key}",
                "info"
            )
            response, cancel_logs = self.cancel_order(cancel_order_id)

            for entry in cancel_logs:
                log_event(entry["msg"], entry["level"])

            if response.get("status_code") == 204:
                cancellations += 1
                with self._state_lock:
                    removed = self.active_orders.pop(cancel_level_key, None)
                    if removed is not None:
                        self.levels = [
                            lvl for lvl in self.levels
                            if _price_key(lvl) != cancel_level_key
                        ]
                        self.extended_levels.pop(cancel_level_key, None)

                if not floor_virtual_removed:
                    removed_virtual_key = self._remove_lowest_virtual_buy_order()
                    floor_virtual_removed = True
                    if removed_virtual_key is not None:
                        log_event(
                            f"[ENGINE] Trailing up: virtual BUY de suelo {removed_virtual_key} "
                            f"eliminada tras la primera cancelacion de BUY bajo",
                            "info"
                        )

                estimated_available += estimated_release
                if estimated_available >= required:
                    log_event(
                        f"[ENGINE] Trailing up: saldo estimado liberado con BUY {cancel_level_key}; "
                        f"_place_order_safe esperará si el exchange aún no actualizó el disponible",
                        "info"
                    )
                    return True

                if retry_delay > 0:
                    time.sleep(retry_delay)
                    refreshed_available = self._get_available_usdc()
                    if refreshed_available > estimated_available:
                        estimated_available = refreshed_available
                continue

            with self._state_lock:
                current = self.active_orders.get(cancel_level_key)
                if current is not None and current.get("order_id") == "pending_cancel":
                    current["order_id"] = cancel_order_id

            err_body = response.get("body", {})
            if isinstance(err_body, dict):
                error_msg = err_body.get("message", "unknown")
                error_id = err_body.get("error_id", "")
            else:
                error_msg = str(err_body)
                error_id = ""
            log_event(
                f"[ENGINE] Trailing up: cancel fallido en {cancel_level_key}: "
                f"{error_msg} ({error_id})",
                "warning"
            )
            return False

        return True

    def set_trailing(self, up: bool, down: object) -> None:
        mode = self._normalise_trailing_down_mode(down)
        with self._state_lock:
            self.trailing_up_enabled = up
            self.trailing_down_mode = mode
            self.trailing_down_enabled = mode != 'off'
            if mode != 'extended':
                self._trailing_down_extended_drops = 0

        log_event(
            f"[ENGINE] Trailing actualizado → up: {'ON' if up else 'OFF'} | down: {mode.upper()}",
            'info'
        )

    # ----------------------------------------------------------
    # SNAPSHOTS / THREAD SAFETY
    # ----------------------------------------------------------

    def _clone_order_info(self, info: OrderInfo) -> OrderInfo:
        cloned: OrderInfo = {
            "side": str(info["side"]),
            "order_id": str(info["order_id"]),
            "price": info["price"],
            "placed_at": float(info["placed_at"]),
            "size": info.get("size", self.base_size),
        }

        if "grid_step" in info:
            cloned["grid_step"] = info["grid_step"]

        if "extended" in info:
            cloned["extended"] = info["extended"]

        if "paired_buy_price" in info:
            cloned["paired_buy_price"] = info["paired_buy_price"]

        if "paired_sell_price" in info:
            cloned["paired_sell_price"] = info["paired_sell_price"]

        return cloned

    def _serialise_order_info(self, info: OrderInfo) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "side": info["side"],
            "order_id": info["order_id"],
            "price": str(info["price"]),
            "placed_at": info["placed_at"],
            "size": str(info.get("size", self.base_size)),
        }

        if "extended" in info:
            payload["extended"] = bool(info.get("extended"))

        for meta_key in ("grid_step", "paired_buy_price", "paired_sell_price"):
            if meta_key in info and info.get(meta_key) is not None:
                payload[meta_key] = str(info.get(meta_key))

        return payload

    def _build_state_snapshot_locked(self) -> Dict[str, Any]:
        return {
            "version": VERSION,
            "steps_each_side": self.steps_each_side,
            "levels_below": self.levels_below,
            "levels_above": self.levels_above,
            "step_percent": str(self.step_percent),
            "base_size": str(self.base_size),
            "trailing_up_enabled": self.trailing_up_enabled,
            "trailing_down_mode": self.trailing_down_mode,
            "trailing_down_enabled": self.trailing_down_enabled,
            "center_price": str(self.center_price) if self.center_price else None,
            "step": str(self.step) if self.step else None,
            "base_step": str(self.base_step) if self.base_step else None,
            "levels": [str(level) for level in self.levels],
            "extended_levels": {
                key: str(step_to_upper)
                for key, step_to_upper in self.extended_levels.items()
            },
            "active_orders": {
                key: self._serialise_order_info(info)
                for key, info in self.active_orders.items()
            },
            "last_fill_side": self.last_fill_side,
            "last_fill_price": str(self.last_fill_price) if self.last_fill_price is not None else None,
            "trailing_down_extended_drops": self._trailing_down_extended_drops,
            "saved_at": int(time.time()),
        }

    def get_runtime_snapshot(self, *, fill_history_limit: Optional[int] = None) -> Dict[str, Any]:
        with self._state_lock:
            history = self.fill_history
            if fill_history_limit is not None:
                history = history[-fill_history_limit:]

            return {
                "center_price": self.center_price,
                "step": self.step,
                "current_price": self.current_price,
                "last_fill_side": self.last_fill_side,
                "last_fill_price": self.last_fill_price,
                "base_size": self.base_size,
                "trailing_up_enabled": self.trailing_up_enabled,
                "trailing_down_mode": self.trailing_down_mode,
                "trailing_down_enabled": self.trailing_down_enabled,
                "trailing_down_extended_drops": self._trailing_down_extended_drops,
                "levels": list(self.levels),
                "extended_levels": dict(self.extended_levels),
                "active_orders": {
                    key: self._clone_order_info(info)
                    for key, info in self.active_orders.items()
                },
                "fill_history": [dict(entry) for entry in history],
            }

    def get_order_info(self, key: str) -> Optional[OrderInfo]:
        with self._state_lock:
            info = self.active_orders.get(key)
            if info is None:
                return None
            return self._clone_order_info(info)

    def place_manual_order(
        self,
        price: Decimal,
        side: str,
        base_size: Optional[Decimal] = None,
    ) -> Tuple[Optional[str], List[LogEntry], Optional[str]]:
        size = Decimal(str(base_size)) if base_size is not None else self.base_size
        key = _price_key(price)

        with self._state_lock:
            existing = self.active_orders.get(key)
            if existing is not None:
                existing_oid = str(existing["order_id"])
                return None, [], (
                    f"El nivel {key} ya tiene una orden {str(existing['side']).upper()} "
                    f"({existing_oid[:8]}...)."
                )

            self.active_orders[key] = {
                "side": side,
                "order_id": "pending_manual",
                "price": price,
                "size": size,
                "placed_at": time.time(),
            }

        order_id, logs = api_place_order(side, price, size)

        if not order_id:
            with self._state_lock:
                current = self.active_orders.get(key)
                if current is not None and current["order_id"] == "pending_manual":
                    del self.active_orders[key]
            return None, logs, None

        with self._state_lock:
            self.active_orders[key] = {
                "side": side,
                "order_id": order_id,
                "price": price,
                "size": size,
                "placed_at": time.time(),
            }

        self.save_state()
        return order_id, logs, None

    def cancel_order_by_key(
        self,
        key: str,
        expected_order_id: Optional[str] = None,
    ) -> Tuple[bool, List[LogEntry], Optional[str]]:
        info = self.get_order_info(key)
        if info is None:
            return False, [], f"No hay orden en {key}."

        order_id = str(info["order_id"])
        if expected_order_id is not None and order_id != expected_order_id:
            return False, [], f"La orden en {key} cambió antes de confirmar la cancelación."

        if order_id in {"virtual", "pending_post_only", "pending_manual", "pending_cancel"}:
            return False, [], f"La orden en {key} no existe todavía en el exchange."

        response, logs = self.cancel_order(order_id)
        if isinstance(response, dict) and response.get("error"):
            return False, logs, f"No se pudo cancelar la orden en {key}."

        removed = False
        with self._state_lock:
            current = self.active_orders.get(key)
            if current is not None and current["order_id"] == order_id:
                del self.active_orders[key]
                removed = True

        if removed:
            self.save_state()

        return True, logs, None

    # ----------------------------------------------------------
    # STATE
    # ----------------------------------------------------------

    def save_state(self) -> bool:
        """
        Persiste el estado actual del motor en STATE_PATH (grid_state.json).
        Se llama automáticamente tras cada cambio relevante.
        """
        with self._state_lock:
            state = self._build_state_snapshot_locked()

        try:
            STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
            return True
        except Exception as e:
            log_event(f"[STATE] Error guardando estado: {e}", "error")
            return False

    def load_state(self) -> bool:
        """
        Carga el estado desde STATE_PATH si existe.
        Retorna True si se cargó correctamente, False si no había estado
        o era incompatible.
        """
        if not STATE_PATH.exists():
            return False

        try:
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))

            raw_steps = int(raw.get("steps_each_side", 0))
            levels_below = int(raw.get("levels_below", raw_steps))
            levels_above = int(raw.get("levels_above", raw_steps))
            steps_each_side = raw_steps or max(levels_below, levels_above)
            step_percent = Decimal(raw["step_percent"])
            base_size = Decimal(raw["base_size"])
            center_price = Decimal(raw["center_price"]) if raw.get("center_price") else None
            step = Decimal(raw["step"]) if raw.get("step") else None
            base_step = Decimal(raw["base_step"]) if raw.get("base_step") else step
            levels = [Decimal(level) for level in raw.get("levels", [])]

            def _parse_size(value: object) -> Decimal:
                try:
                    return Decimal(str(value))
                except Exception:
                    return base_size

            def _parse_optional_decimal(value: object) -> Optional[Decimal]:
                if value is None:
                    return None
                try:
                    return Decimal(str(value))
                except Exception:
                    return None

            extended_levels: Dict[str, Decimal] = {}
            raw_extended_levels = raw.get("extended_levels", {})
            if isinstance(raw_extended_levels, dict):
                for key, raw_step in raw_extended_levels.items():
                    step_value = raw_step.get("step_to_upper") if isinstance(raw_step, dict) else raw_step
                    parsed_step = _parse_optional_decimal(step_value)
                    if parsed_step is not None and parsed_step > 0:
                        extended_levels[str(key)] = parsed_step

            active_orders: Dict[str, OrderInfo] = {}
            raw_active_orders = raw.get("active_orders", {})
            if isinstance(raw_active_orders, dict):
                for key, info in raw_active_orders.items():
                    if not isinstance(info, dict):
                        continue

                    parsed_order = cast(OrderInfo, {
                        "side": info["side"],
                        "order_id": info["order_id"],
                        "price": Decimal(info["price"]),
                        "size": _parse_size(info.get("size", base_size)),
                        "placed_at": float(info.get("placed_at", 0)),
                    })

                    if bool(info.get("extended")):
                        parsed_order["extended"] = True

                    for meta_key in ("grid_step", "paired_buy_price", "paired_sell_price"):
                        parsed_meta = _parse_optional_decimal(info.get(meta_key))
                        if parsed_meta is not None:
                            parsed_order[meta_key] = parsed_meta

                    active_orders[str(key)] = parsed_order

            if not extended_levels:
                default_extended_step = base_step if base_step is not None else step
                for info in active_orders.values():
                    if not self._is_extended_order(info) or default_extended_step is None:
                        continue
                    grid_step = self._decimal_from_meta(info.get("grid_step"), default_extended_step)
                    if info.get("side") == "buy":
                        extended_levels[_price_key(info["price"])] = grid_step
                    paired_buy = info.get("paired_buy_price")
                    if paired_buy is not None:
                        try:
                            extended_levels[_price_key(Decimal(str(paired_buy)))] = grid_step
                        except Exception:
                            pass
            last_fill_side = raw.get("last_fill_side")
            last_fill_price = (
                Decimal(raw["last_fill_price"])
                if raw.get("last_fill_price") is not None else None
            )
            trailing_up_enabled = bool(raw.get("trailing_up_enabled", True))
            trailing_down_mode = self._normalise_trailing_down_mode(
                raw.get("trailing_down_mode", raw.get("trailing_down_enabled", True))
            )
            trailing_down_extended_drops = int(raw.get("trailing_down_extended_drops", 0) or 0)

            if last_fill_price is None:
                missing_levels = [
                    level for level in levels
                    if _price_key(level) not in active_orders
                ]
                if len(missing_levels) == 1:
                    last_fill_price = missing_levels[0]

            with self._state_lock:
                self.levels_below = levels_below
                self.levels_above = levels_above
                self.steps_each_side = steps_each_side
                self.step_percent = step_percent
                self.base_size = base_size
                self.center_price = center_price
                self.step = step
                self.base_step = base_step
                self.levels = levels
                self.extended_levels = extended_levels
                self.active_orders = active_orders
                self.last_fill_side = last_fill_side
                self.last_fill_price = last_fill_price
                self.trailing_up_enabled = trailing_up_enabled
                self.trailing_down_mode = trailing_down_mode
                self.trailing_down_enabled = trailing_down_mode != 'off'
                self._trailing_down_extended_drops = max(0, trailing_down_extended_drops)

            saved_at = raw.get("saved_at", 0)
            age_min = (time.time() - saved_at) / 60
            log_event(
                f"[STATE] Estado recuperado de {STATE_PATH} "
                f"(guardado hace {age_min:.1f} min, "
                f"{len(active_orders)} órdenes, "
                f"{len(levels)} niveles)",
                "info"
            )
            return True

        except Exception as e:
            log_event(f"[STATE] Error cargando estado: {e} — se iniciará desde cero.", "warning")
            return False

    def clear_state(self) -> None:
        """
        Elimina el archivo de estado tras un cierre limpio con cancelación de órdenes.
        """
        try:
            if STATE_PATH.exists():
                STATE_PATH.unlink()
                log_event(f"[STATE] Archivo de estado eliminado ({STATE_PATH})", "info")
        except Exception as e:
            log_event(f"[STATE] Error eliminando estado: {e}", "warning")

    # ----------------------------------------------------------
    # INIT
    # ----------------------------------------------------------

    def _resolve_initial_price(self) -> Decimal:
        """
        Determina el precio inicial del grid.
        Prioridad:
          1. self.initial_price si ya fue fijado desde fuera.
          2. precio actual como fallback.
        """
        if self.initial_price is not None:
            return self.initial_price.quantize(TICK_SIZE, rounding=ROUND_DOWN)

        price, _ = get_current_price()
        if price is None:
            raise RuntimeError("No se pudo obtener precio inicial")
        return price.quantize(TICK_SIZE, rounding=ROUND_DOWN)

    def initialize(self, recover_state: Optional[bool] = None) -> None:
        """
        Inicializa el grid engine.

        - Si recover_state es True y existe STATE_PATH, intenta recuperar.
        - Si recover_state es False, ignora el estado previo y arranca desde cero.
        - Si recover_state es None, pregunta interactivamente como fallback.
        """
        if STATE_PATH.exists():
            should_recover = recover_state
            if should_recover is None:
                try:
                    respuesta = input(
                        f"Se encontró estado previo en {STATE_PATH}. "
                        "¿Deseas recuperarlo? (s/n): "
                    ).strip().lower()
                except EOFError:
                    respuesta = "n"
                should_recover = respuesta.startswith("s")

            if should_recover:
                if self.load_state():
                    log_event("[ENGINE] Grid recuperado del estado anterior.", "info")
                    return
                else:
                    log_event("[ENGINE] No se pudo recuperar el estado — iniciando desde cero.", "warning")

        price = self._resolve_initial_price()

        center_price = price
        step = (price * self.step_percent).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        levels: List[Decimal] = []

        for i in range(-self.levels_below, self.levels_above + 1):
            lvl = (price + (Decimal(i) * step)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
            levels.append(lvl)

        levels = sorted(set(levels))

        with self._state_lock:
            self.center_price = center_price
            self.step = step
            self.base_step = step
            self.levels = levels
            self.extended_levels = {}

        ok, _ = check_balances_for_grid(self.base_size, levels, center_price=center_price)
        if not ok:
            raise RuntimeError("Saldos insuficientes para inicializar grid.")

        self.place_initial_orders()

        log_event(
            f"[ENGINE] Grid inicializado en {_price_key(self.center_price)} "
            f"con step {_price_key(self.step)}"
        )
        self.save_state()

    # ----------------------------------------------------------
    # ORDER PLACEMENT
    # ----------------------------------------------------------

    def place_initial_orders(self) -> None:
        """
        Coloca órdenes BUY en todos los niveles por debajo del centro
        y órdenes SELL en todos los niveles por encima. El nivel central
        se deja vacío (precio de mercado en el momento de inicialización).
        """
        if self.center_price is None:
            raise RuntimeError("center_price no inicializado")

        for price in self.levels:
            if price < self.center_price:
                self.place_order(price, "buy")
            elif price > self.center_price:
                self.place_order(price, "sell")

    def place_order(
        self,
        price: Decimal,
        side: str,
        size: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Envía una orden limit post_only a la API y la registra en active_orders.
        Si la API no devuelve venue_order_id, loguea el error y no registra la orden.
        """
        order_size = Decimal(str(size)) if size is not None else self.base_size
        body = {
            "client_order_id": str(uuid.uuid4()),
            "symbol": SYMBOL,
            "side": side,
            "order_configuration": {
                "limit": {
                    "base_size": fmt_amount(order_size),
                    "price": _price_key(price),
                    "execution_instructions": ["post_only"]
                }
            }
        }

        resp, _ = send_request("POST", "/api/1.0/orders", body=body)

        order_id: Optional[str] = None
        state: Optional[str] = None
        if isinstance(resp, dict):
            data = resp.get("data")
            if isinstance(data, dict):
                venue = data.get("venue_order_id")
                state = data.get("state")
                if isinstance(venue, str):
                    order_id = venue

        key = _price_key(price)

        if state == "rejected":
            with self._state_lock:
                order_info = cast(OrderInfo, {
                    "side": side,
                    "order_id": "pending_post_only",
                    "price": price,
                    "size": order_size,
                    "placed_at": time.time(),
                })
                self.active_orders[key] = self._apply_order_metadata(order_info, metadata)
            log_event(
                f"[ENGINE] Orden {side} en {_price_key(price)} rechazada por post_only "
                f"(venue_order_id: {order_id}) — nivel marcado como latente",
                "warning"
            )
            _notify(f"⏳ Orden latente\n{side.upper()} en {_price_key(price)} — esperando lado correcto")
            self.save_state()
            return

        if order_id is None:
            log_event(f"[ENGINE] No se encontró venue_order_id en respuesta {resp}", "error")
            _notify(f"❌ Error colocando orden {side.upper()} en {_price_key(price)}")
            return

        with self._state_lock:
            order_info = cast(OrderInfo, {
                "side": side,
                "order_id": order_id,
                "price": price,
                "size": order_size,
                "placed_at": time.time(),
            })
            self.active_orders[key] = self._apply_order_metadata(order_info, metadata)

        log_event(f"[ENGINE] Orden {side} registrada en {_price_key(price)} -> {order_id}")
        self.save_state()

    def _place_order_safe(
        self,
        price: Decimal,
        side: str,
        size: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None,
        *,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> bool:
        """
        Wrapper sobre place_order que verifica saldo antes de enviar.
        Reintenta hasta max_retries veces con backoff si el saldo no está disponible aún
        (p.ej. el exchange tarda en liberar reservas tras una cancelación reciente).
        Devuelve True si la orden fue registrada, False si falló.
        """
        if self.step is None:
            return False

        order_size = Decimal(str(size)) if size is not None else self.base_size
        key = _price_key(price)

        for attempt in range(1, max_retries + 1):
            with self._state_lock:
                if key in self.active_orders:
                    return False

            balances_resp, _ = get_all_balances()
            usdc_balance, btc_balance = _parse_balances(balances_resp)

            if side == "buy":
                required = order_size * price
                if usdc_balance < required:
                    if attempt < max_retries:
                        wait = retry_delay * attempt  # 1.0s, 2.0s, 3.0s
                        log_event(
                            f"[ENGINE] Saldo USDC insuficiente para BUY en {_price_key(price)} "
                            f"({_price_key(usdc_balance)} < {_price_key(required)}) "
                            f"— reintento {attempt}/{max_retries} en {wait:.1f}s",
                            "warning"
                        )
                        time.sleep(wait)
                        continue
                    log_event(
                        f"[ENGINE] Saldo USDC insuficiente para BUY en {_price_key(price)}: "
                        f"disponible {_price_key(usdc_balance)} < requerido {_price_key(required)}",
                        "warning"
                    )
                    return False

            elif side == "sell":
                if btc_balance < order_size:
                    if attempt < max_retries:
                        wait = retry_delay * attempt
                        log_event(
                            f"[ENGINE] Saldo BTC insuficiente para SELL en {_price_key(price)} "
                            f"({fmt_amount(btc_balance)} < {fmt_amount(order_size)}) "
                            f"— reintento {attempt}/{max_retries} en {wait:.1f}s",
                            "warning"
                        )
                        time.sleep(wait)
                        continue
                    log_event(
                        f"[ENGINE] Saldo BTC insuficiente para SELL en {_price_key(price)}: "
                        f"disponible {fmt_amount(btc_balance)} < requerido {fmt_amount(order_size)}",
                        "warning"
                    )
                    return False

            # Saldo OK en este intento
            self.place_order(price, side, order_size, metadata=metadata)
            with self._state_lock:
                return key in self.active_orders

        return False  # nunca debería llegar aquí, pero por seguridad

    def _infer_fill_empty_level_size(
        self,
        level: Decimal,
        current_price: Decimal,
        snapshot_orders: Dict[str, OrderInfo],
        levels_sorted: List[Decimal],
    ) -> Decimal:
        """Infiere el tamaño correcto para una repoblación de nivel vacío."""
        intended_side = "buy" if level < current_price else "sell"
        candidates: List[Tuple[Decimal, Decimal, OrderInfo]] = []

        for other_level in levels_sorted:
            if other_level == level:
                continue

            info = snapshot_orders.get(_price_key(other_level))
            if info is None or str(info.get("side")) != intended_side:
                continue

            try:
                distance = abs(other_level - level)
            except Exception:
                continue

            candidates.append((distance, other_level, info))

        if not candidates:
            return self.base_size

        _, _, nearest_info = min(candidates, key=lambda item: (item[0], item[1]))
        return self._order_size(nearest_info)

    def cancel_order(self, order_id: str) -> Tuple[Dict[str, Any], List[LogEntry]]:
        """Wrapper sobre api.cancel_order para uso interno del engine."""
        return cancel_order(order_id)

    # ----------------------------------------------------------
    # DETECT FILLS
    # ----------------------------------------------------------

    def detect_fills(self, current_price: Optional[Decimal] = None) -> Tuple[List[str], List[LogEntry]]:
        """
        Detecta órdenes ejecutadas consultando el historial de filled.

        Lógica por cada orden en active_orders:
          - Orden virtual: se compara precio con mercado; si se activa, rebalancea,
            pero NO se registra como fill real en métricas.
          - Aparece en historial filled  → ejecutada, rebalancear.
          - No aparece en filled ni en activas, y tiene más de 30s → se mira order by id y se decide segun el status.
          - Sigue en activas, o tiene menos de 30s → ignorar.
        """
        EXTERNAL_CANCEL_GRACE = 20  # segundos de margen para propagación de la API

        logs: List[LogEntry] = []

        # Solo consultar ticker si no se pasó precio desde fuera
        if current_price is None:
            current_price, price_logs = get_current_price()
            for l in price_logs:
                log_event(f"[DETECT_FILLS] {l['msg']}", l["level"], logs)

        # Pide historico de ordenes para detectar fills confirmados
        with self._state_lock:
            active_orders_snapshot = {
                key: self._clone_order_info(info)
                for key, info in self.active_orders.items()
            }

        hist_limit = len(active_orders_snapshot) + 50
        hist_resp, hist_logs = get_historical_orders(limit=hist_limit)
        for l in hist_logs:
            log_event(f"[DETECT_FILLS] {l['msg']}", l["level"], logs)

        confirmed_filled_ids: set[str] = set()
        if isinstance(hist_resp, dict) and isinstance(hist_resp.get("data"), list):
            for order in hist_resp["data"]:
                oid = order.get("id")
                if isinstance(oid, str) and order.get("status") == "filled":
                    confirmed_filled_ids.add(oid)
        
        # Pide ordenes activas a la API para detectar inconsistencias (órdenes desaparecidas que no están en self.active_orders).
        active_api_resp, active_logs = get_active_orders()
        for l in active_logs:
            log_event(f"[DETECT_FILLS] {l['msg']}", l["level"], logs)

        current_api_ids: set[str] = set()
        if isinstance(active_api_resp, dict) and isinstance(active_api_resp.get("data"), list):
            for order in active_api_resp["data"]:
                oid = order.get("id")
                if isinstance(oid, str):
                    current_api_ids.add(oid)
        
        # Compara self.active.orders con los datos obtenidos para detectar fills confirmados, cancelaciones externas u otras inconsistencias
        # y actualiza self.active_orders para reflejar las actualizaciones.
        filled_keys: List[str] = []
        state_changed = False

        for key, info in active_orders_snapshot.items():
            oid = info.get("order_id")
            oside = info["side"]
            if not isinstance(oid, str):
                continue

            # Orden virtual (centinela de extremo de grid)
            if oid == "virtual":
                if current_price is not None:
                    v_price = info["price"]
                    v_side = info["side"]
                    triggered = (
                        (v_side == "sell" and current_price >= v_price) or
                        (v_side == "buy" and current_price <= v_price)
                    )
                    if triggered:
                        filled_keys.append(key)
                        log_event(
                            f"[DETECT_FILLS] Orden virtual {v_side} en {key} activada "
                            f"(precio actual {_price_key(current_price)})",
                            "info", logs
                        )
                continue  # las virtuales nunca son cancelación externa

            if oid in {"pending_manual", "pending_cancel"}:
                continue

            if oid == "pending_post_only":
                if current_price is not None:
                    lvl_price = info["price"]
                    can_place = (
                        (oside == "sell" and current_price < lvl_price) or
                        (oside == "buy" and current_price > lvl_price)
                    )
                    if can_place:
                        log_event(
                            f"[DETECT_FILLS] Reintentando orden latente {oside} en {key} "
                            f"(precio actual {_price_key(current_price)})",
                            "info", logs
                        )
                        should_retry = False
                        retry_metadata: Dict[str, Any] = {}
                        with self._state_lock:
                            current = self.active_orders.get(key)
                            if current is not None and current["order_id"] == "pending_post_only":
                                retry_metadata = {}
                                for meta_key in ("extended", "grid_step", "paired_buy_price", "paired_sell_price"):
                                    if meta_key in current:
                                        retry_metadata[meta_key] = current.get(meta_key)
                                del self.active_orders[key]
                                state_changed = True
                                should_retry = True
                        if should_retry:
                            self._place_order_safe(lvl_price, oside, self._order_size(info), metadata=retry_metadata or None)
                continue

            if oid in confirmed_filled_ids:
                filled_keys.append(key)
                log_event(f"[DETECT_FILLS] {oside} confirmado para {key} (order_id: {oid})", "info", logs)
                continue

            if oid in current_api_ids:
                continue

            age = time.time() - info.get("placed_at", 0)
            if age < EXTERNAL_CANCEL_GRACE:
                continue

            ACTIVE_STATES = {"pending_new", "new", "partially_filled"}
            MAX_RETRIES = 3
            RETRY_DELAY = 2

            confirmed_status: Optional[str] = None
            for attempt in range(1, MAX_RETRIES + 1):
                order_resp, order_logs = get_order_by_id(oid)
                for l in order_logs:
                    log_event(f"[DETECT_FILLS] {l['msg']}", l["level"], logs)

                if isinstance(order_resp, dict) and not order_resp.get("error"):
                    data = order_resp.get("data", {})
                    confirmed_status = data.get("status")
                    break

                log_event(
                    f"[DETECT_FILLS] get_order_by_id intento {attempt}/{MAX_RETRIES} "
                    f"fallido para {oid} — reintentando en {RETRY_DELAY}s",
                    "warning", logs
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

            if confirmed_status == "rejected":
                with self._state_lock:
                    current = self.active_orders.get(key)
                    if current is not None and current["order_id"] == oid:
                        current["order_id"] = "pending_post_only"
                        current["placed_at"] = time.time()
                        state_changed = True
                log_event(
                    f"[DETECT_FILLS] Orden {oside} {key} (order_id: {oid}) rechazada (post_only) "
                    f"— nivel marcado como latente",
                    "warning", logs
                )

            elif confirmed_status in {"cancelled", "replaced"}:
                log_event(
                    f"[DETECT_FILLS] Orden {oside} {key} (order_id: {oid}) confirmada como "
                    f"'{confirmed_status}' — cancelación externa. Se elimina sin rebalancear.",
                    "warning", logs
                )
                with self._state_lock:
                    current = self.active_orders.get(key)
                    if current is not None and current["order_id"] == oid:
                        del self.active_orders[key]
                        state_changed = True

            elif confirmed_status == "filled":
                log_event(
                    f"[DETECT_FILLS] Orden {oside} {key} (order_id: {oid}) confirmada como "
                    f"'filled' vía get_order_by_id — añadida a fills.",
                    "info", logs
                )
                filled_keys.append(key)

            elif confirmed_status in ACTIVE_STATES:
                pass  # orden sigue viva, ignorar en silencio

            else:
                # No se pudo confirmar tras MAX_RETRIES intentos
                log_event(
                    f"[DETECT_FILLS] No se pudo confirmar estado de {oside} {key} (order_id: {oid}) "
                    f"tras {MAX_RETRIES} intentos (status: {confirmed_status!r}) — conservando orden.",
                    "warning", logs
                )

        if state_changed:
            self.save_state()

        return filled_keys, logs

    # ----------------------------------------------------------
    # REBALANCE
    # ----------------------------------------------------------

    def rebalance_after_fill(self, filled_price_key: str, info: "OrderInfo") -> None:
        """
        Rebalancea el grid tras un fill real o una activación virtual.

        trailing_down_extended se trata como una subrejilla inferior:
          - El suelo principal sigue usando base_size y el step original.
          - Cada nivel por debajo del suelo principal se marca como extended.
          - Los niveles extended usan 50% de base_size.
          - El step extended crece un 10% en cada nueva línea virtual.
          - Cada dos sells extended de 0.5 BTC se cancela un SELL principal alto de 1 BTC
            para liberar inventario: activaciones virtuales 1, 3, 5, ...
        """

        side: str = str(info["side"])
        price: Decimal = Decimal(str(info["price"])).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        order_size = self._order_size(info)
        order_id = str(info.get("order_id", ""))

        cancel_order_id: Optional[str] = None
        cancel_level_key: Optional[str] = None
        remove_ceiling_virtual_after_cancel = False
        orders_to_place: List[Tuple[Decimal, str, Decimal, Optional[Dict[str, Any]]]] = []
        virtual_orders_to_add: List[Tuple[str, OrderInfo]] = []
        trailing_up_buy_release_keys: set[str] = set()
        trailing_logs: List[str] = []

        with self._state_lock:
            self.last_fill_side = side
            self.last_fill_price = price

            if self.step is None or not self.levels:
                log_event("[ENGINE] step es None o no hay niveles para rebalancear", "error")
                return

            base_step = self._get_base_step_locked()
            self.step = base_step
            self.base_step = base_step

            levels_snapshot = sorted(set(self.levels))
            principal_levels = self._principal_levels_locked()
            lowest = min(levels_snapshot)
            highest = max(levels_snapshot)
            lowest_principal = min(principal_levels)
            max_levels = self.levels_below + self.levels_above + 2

            filled_key = _price_key(price)
            is_virtual = order_id == "virtual"
            is_extended = self._is_extended_order(info) or filled_key in self.extended_levels
            handled = False

            # --------------------------------------------------
            # Ciclo extended ya existente: SELL extended ejecutado -> BUY en la línea inferior.
            # Ejemplo: SELL 16690 ejecutado -> BUY 15359, manteniendo grid_step 1331.
            # --------------------------------------------------
            if side == "sell" and is_extended:
                grid_step = self._decimal_from_meta(
                    info.get("grid_step"),
                    self.extended_levels.get(filled_key, base_step),
                )
                default_buy_price = (price - grid_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                lower_buy_price = self._price_from_meta(info.get("paired_buy_price"), default_buy_price)

                self._mark_extended_level_locked(lower_buy_price, grid_step)
                orders_to_place.append((
                    lower_buy_price,
                    "buy",
                    order_size,
                    {
                        "extended": True,
                        "grid_step": grid_step,
                        "paired_sell_price": price,
                    },
                ))
                trailing_logs.append(
                    f"[ENGINE] Extended SELL en {filled_key}: BUY {fmt_amount(order_size)} "
                    f"en {_price_key(lower_buy_price)}"
                )
                handled = True

            # --------------------------------------------------
            # Ciclo extended inferior: BUY extended ejecutado o BUY virtual activado.
            # --------------------------------------------------
            elif side == "buy" and is_extended:
                grid_step = self._decimal_from_meta(
                    info.get("grid_step"),
                    self.extended_levels.get(filled_key, base_step),
                )
                default_sell_price = (price + grid_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                upper_sell_price = self._price_from_meta(info.get("paired_sell_price"), default_sell_price)
                self._mark_extended_level_locked(price, grid_step)

                if is_virtual:
                    if self.trailing_down_mode == "extended":
                        extended_size = self._extended_order_size()
                        new_step = (grid_step * Decimal("1.1")).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                        if new_step <= 0:
                            new_step = grid_step
                        next_buy_price = (price - new_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)

                        self._trailing_down_extended_drops += 1
                        self._mark_extended_level_locked(next_buy_price, new_step)

                        orders_to_place.append((
                            upper_sell_price,
                            "sell",
                            extended_size,
                            {
                                "extended": True,
                                "grid_step": grid_step,
                                "paired_buy_price": price,
                            },
                        ))

                        next_buy_key = _price_key(next_buy_price)
                        if next_buy_key not in self.active_orders:
                            virtual_info = cast(OrderInfo, {
                                "side": "buy",
                                "order_id": "virtual",
                                "price": next_buy_price,
                                "size": extended_size,
                                "placed_at": time.time(),
                            })
                            virtual_orders_to_add.append((
                                next_buy_key,
                                self._apply_order_metadata(virtual_info, {
                                    "extended": True,
                                    "grid_step": new_step,
                                    "paired_sell_price": price,
                                }),
                            ))

                        # Cada SELL principal de base_size libera saldo para dos sells extended de 0.5.
                        # Por eso se cancela en las activaciones virtuales 1, 3, 5, ...
                        if self._trailing_down_extended_drops % 2 == 1:
                            highest_sell = self._find_highest_real_sell_order()
                            if highest_sell is not None:
                                cancel_level_key, cancel_info = highest_sell
                                cancel_order_id = str(cancel_info["order_id"])
                                remove_ceiling_virtual_after_cancel = True
                                if cancel_level_key in self.active_orders:
                                    self.active_orders[cancel_level_key]["order_id"] = "pending_cancel"
                                trailing_logs.append(
                                    f"[ENGINE] Trailing down extended: cancelando SELL alto "
                                    f"{cancel_level_key} para liberar BTC"
                                )

                        trailing_logs.append(
                            f"[ENGINE] Trailing down extended: virtual BUY {filled_key} confirmado; "
                            f"SELL {_price_key(upper_sell_price)} size {fmt_amount(extended_size)}; "
                            f"nueva virtual BUY {next_buy_key} con step {_price_key(new_step)}"
                        )
                    else:
                        trailing_logs.append(
                            f"[ENGINE] Virtual extended BUY {filled_key} activada, pero "
                            f"trailing_down_mode={self.trailing_down_mode}; no se extiende"
                        )
                else:
                    orders_to_place.append((
                        upper_sell_price,
                        "sell",
                        order_size,
                        {
                            "extended": True,
                            "grid_step": grid_step,
                            "paired_buy_price": price,
                        },
                    ))
                    trailing_logs.append(
                        f"[ENGINE] Extended BUY en {filled_key}: SELL {fmt_amount(order_size)} "
                        f"en {_price_key(upper_sell_price)}"
                    )

                handled = True

            # --------------------------------------------------
            # Primer toque del suelo principal en modo extended.
            # Ejemplo: BUY 20000 -> SELL 21000 size 1 y virtual BUY 19000 size 0.5.
            # --------------------------------------------------
            if not handled and side == "buy" and price == lowest_principal:
                next_sell_price = (price + base_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                next_buy_price = (price - base_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)

                if self.trailing_down_mode == "extended":
                    extended_size = self._extended_order_size()
                    self._mark_extended_level_locked(next_buy_price, base_step)

                    orders_to_place.append((next_sell_price, "sell", order_size, None))

                    next_buy_key = _price_key(next_buy_price)
                    if next_buy_key not in self.active_orders:
                        virtual_info = cast(OrderInfo, {
                            "side": "buy",
                            "order_id": "virtual",
                            "price": next_buy_price,
                            "size": extended_size,
                            "placed_at": time.time(),
                        })
                        virtual_orders_to_add.append((
                            next_buy_key,
                            self._apply_order_metadata(virtual_info, {
                                "extended": True,
                                "grid_step": base_step,
                                "paired_sell_price": price,
                            }),
                        ))

                    trailing_logs.append(
                        f"[ENGINE] Trailing down extended iniciado: SELL {_price_key(next_sell_price)} "
                        f"size {fmt_amount(order_size)} y virtual BUY {next_buy_key} "
                        f"size {fmt_amount(extended_size)}"
                    )

                elif self.trailing_down_mode == "on":
                    trail_down_price = next_buy_price
                    self.levels.append(trail_down_price)
                    if len(self.levels) > max_levels:
                        self.levels.remove(highest)
                        highest_key = _price_key(highest)
                        removed = self.active_orders.pop(highest_key, None)
                        if removed is not None and removed["order_id"] not in {"virtual", "pending_post_only", "pending_manual", "pending_cancel"}:
                            cancel_order_id = str(removed["order_id"])
                            cancel_level_key = highest_key

                    trail_down_key = _price_key(trail_down_price)
                    if trail_down_key not in self.active_orders:
                        virtual_orders_to_add.append((
                            trail_down_key,
                            cast(OrderInfo, {
                                "side": "buy",
                                "order_id": "virtual",
                                "price": trail_down_price,
                                "size": order_size,
                                "placed_at": time.time(),
                            }),
                        ))
                    orders_to_place.append((next_sell_price, "sell", order_size, None))
                    trailing_logs.append(
                        f"[ENGINE] Rebalance trailing down: grid extendido a {_price_key(trail_down_price)}"
                    )
                else:
                    orders_to_place.append((next_sell_price, "sell", order_size, None))
                    trailing_logs.append("[ENGINE] Trailing down desactivado: se mantiene el grid sin extenderse")

                handled = True

            elif not handled and side == "sell" and price == highest:
                next_buy_price = (price - base_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                orders_to_place.append((next_buy_price, "buy", order_size, None))
                if is_virtual:
                    trailing_up_buy_release_keys.add(_price_key(next_buy_price))

                if self.trailing_up_enabled:
                    trail_up_price = (price + base_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                    self.levels.append(trail_up_price)

                    trail_up_key = _price_key(trail_up_price)
                    if trail_up_key not in self.active_orders:
                        virtual_orders_to_add.append((
                            trail_up_key,
                            cast(OrderInfo, {
                                "side": "sell",
                                "order_id": "virtual",
                                "price": trail_up_price,
                                "size": order_size,
                                "placed_at": time.time(),
                            }),
                        ))

                    if is_virtual:
                        trailing_logs.append(
                            f"[ENGINE] Trailing up: virtual SELL {filled_key} activada; "
                            f"BUY {_price_key(next_buy_price)} y nueva virtual SELL {trail_up_key}"
                        )
                    else:
                        trailing_logs.append(
                            f"[ENGINE] Rebalance trailing up: virtual SELL registrada en {trail_up_key}"
                        )
                else:
                    trailing_logs.append("[ENGINE] Trailing up desactivado: se mantiene el grid sin extenderse")

                handled = True

            elif not handled and side == "buy":
                next_sell_price = (price + base_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                orders_to_place.append((next_sell_price, "sell", order_size, None))
                handled = True

            elif not handled and side == "sell":
                next_buy_price = (price - base_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                orders_to_place.append((next_buy_price, "buy", order_size, None))
                handled = True

            self.levels = sorted(set(self.levels))

        if cancel_order_id is not None and cancel_level_key is not None:
            log_event(
                f"[ENGINE] Cancelando orden en {cancel_level_key} ({cancel_order_id}) — nivel eliminado por rebalance",
                "info"
            )
            response, cancel_logs = self.cancel_order(cancel_order_id)

            for entry in cancel_logs:
                log_event(entry["msg"], entry["level"])

            if response.get("status_code") == 204:
                with self._state_lock:
                    removed = self.active_orders.pop(cancel_level_key, None)
                    if removed is not None:
                        self.levels = [lvl for lvl in self.levels if _price_key(lvl) != cancel_level_key]
                        self.extended_levels.pop(cancel_level_key, None)

                if remove_ceiling_virtual_after_cancel:
                    removed_virtual_key = self._remove_highest_virtual_sell_order()
                    if removed_virtual_key is not None:
                        log_event(
                            f"[ENGINE] Trailing down extended: virtual SELL de techo {removed_virtual_key} "
                            f"eliminada tras cancelar SELL alto {cancel_level_key}",
                            "info"
                        )
            else:
                with self._state_lock:
                    if cancel_level_key in self.active_orders:
                        self.active_orders[cancel_level_key]["order_id"] = cancel_order_id

                err_body = response.get("body", {})
                if isinstance(err_body, dict):
                    error_msg = err_body.get("message", "unknown")
                    error_id = err_body.get("error_id", "")
                else:
                    error_msg = str(err_body)
                    error_id = ""
                log_event(
                    f"[ENGINE] Cancel fallido en {cancel_level_key}: {error_msg} ({error_id}) — orden restaurada",
                    "warning"
                )

        for price_to_place, side_to_place, size_to_place, metadata in orders_to_place:
            place_key = _price_key(price_to_place)
            if side_to_place == "buy" and place_key in trailing_up_buy_release_keys:
                if not self._release_usdc_for_trailing_up_buy(price_to_place, size_to_place):
                    log_event(
                        f"[ENGINE] Trailing up: BUY {place_key} omitido por USDC insuficiente",
                        "warning"
                    )
                    continue
            self._place_order_safe(price_to_place, side_to_place, size_to_place, metadata=metadata)

        for virtual_key, virtual_info in virtual_orders_to_add:
            with self._state_lock:
                if virtual_key not in self.active_orders:
                    self.active_orders[virtual_key] = virtual_info
                    added = True
                else:
                    added = False
            if added:
                log_event(
                    f"[ENGINE] Orden virtual {str(virtual_info['side']).upper()} registrada en {virtual_key}",
                    "info"
                )

        for trailing_log in trailing_logs:
            log_event(trailing_log, "info")

        log_event(f"[ENGINE] Rebalance completado tras fill en {filled_price_key}")
        self.save_state()

    def stop(self) -> None:
        """Señaliza al loop principal que debe detenerse limpiamente."""
        self._stop_event.set()
        log_event("[ENGINE] Señal de parada recibida.", "info")

    def is_running(self) -> bool:
        """Retorna True si el engine no ha recibido señal de parada."""
        return not self._stop_event.is_set()

    # ----------------------------------------------------------
    # FILL EMPTY LEVELS (recuperación)
    # ----------------------------------------------------------

    def fill_empty_levels(self, current_price: Decimal) -> None:
        """
        Repobla niveles vacíos (por cancelación externa u otros motivos).

        Prioridad para decidir qué nivel debe permanecer vacío:
          1. Si existe el último nivel ejecutado y sigue libre, se conserva ese hueco.
          2. Si no, se usa la heurística anterior basada en last_fill_side/current_price.

        Esto evita recrear una orden justo en el último fill si el precio oscila
        ligeramente antes de ejecutar la recuperación.
        """
        snapshot = self.get_runtime_snapshot()
        step = snapshot["step"]
        if step is None:
            return

        levels_sorted = sorted(snapshot["levels"])
        active_orders = snapshot["active_orders"]
        extended_levels = snapshot.get("extended_levels", {})
        protected_empty_keys = self._protected_empty_level_keys(active_orders)
        last_fill_price = snapshot["last_fill_price"]
        last_fill_side = snapshot["last_fill_side"]

        skip_level: Optional[Decimal] = None
        skip_reason = "heurística"

        if last_fill_price is not None:
            last_fill_level = last_fill_price.quantize(TICK_SIZE, rounding=ROUND_DOWN)
            last_fill_key = _price_key(last_fill_level)
            if last_fill_level in levels_sorted and last_fill_key not in active_orders:
                skip_level = last_fill_level
                skip_reason = "último fill"

        if skip_level is None:
            if last_fill_side == "buy":
                skip_level = next((level for level in levels_sorted if level > current_price), None)
            elif last_fill_side == "sell":
                skip_level = next((level for level in reversed(levels_sorted) if level < current_price), None)
            else:
                skip_level = min(levels_sorted, key=lambda level: abs(level - current_price), default=None)

        for level in levels_sorted:
            key = _price_key(level)

            with self._state_lock:
                if key in self.active_orders:
                    continue

            if key in protected_empty_keys:
                log_event(
                    f"[ENGINE] Nivel {key} dejado vacío intencionalmente "
                    f"(pata pendiente de orden extended activa)"
                )
                continue

            if skip_level is not None and level == skip_level:
                log_event(
                    f"[ENGINE] Nivel {key} dejado vacío intencionalmente "
                    f"({skip_reason}, último fill: {last_fill_side or 'desconocido'})"
                )
                continue

            order_size = self._infer_fill_empty_level_size(level, current_price, active_orders, levels_sorted)
            metadata: Optional[Dict[str, Any]] = None

            if level < current_price:
                raw_step = extended_levels.get(key) if isinstance(extended_levels, dict) else None
                if raw_step is not None:
                    grid_step = self._decimal_from_meta(raw_step, step)
                    paired_sell_price = (level + grid_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                    order_size = self._extended_order_size()
                    metadata = {
                        "extended": True,
                        "grid_step": grid_step,
                        "paired_sell_price": paired_sell_price,
                    }

                log_event(
                    f"[ENGINE] Nivel vacío detectado, colocando BUY en {key} "
                    f"(size {fmt_amount(order_size)})"
                )
                self._place_order_safe(level, "buy", order_size, metadata=metadata)
            elif level > current_price:
                if isinstance(extended_levels, dict):
                    for lower_key, raw_step in extended_levels.items():
                        try:
                            lower_price = Decimal(str(lower_key))
                            grid_step = self._decimal_from_meta(raw_step, step)
                            paired_sell_price = (lower_price + grid_step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                        except Exception:
                            continue

                        if paired_sell_price == level:
                            order_size = self._extended_order_size()
                            metadata = {
                                "extended": True,
                                "grid_step": grid_step,
                                "paired_buy_price": lower_price,
                            }
                            break

                log_event(
                    f"[ENGINE] Nivel vacío detectado, colocando SELL en {key} "
                    f"(size {fmt_amount(order_size)})"
                )
                self._place_order_safe(level, "sell", order_size, metadata=metadata)

    # ----------------------------------------------------------
    # MAIN LOOP
    # ----------------------------------------------------------

    def run(self, poll_interval: int = 2, recovery_interval: int = 600) -> None:
        """
        poll_interval:     segundos entre cada ciclo de detección de fills (flujo normal).
        recovery_interval: segundos entre cada ejecución de fill_empty_levels (recuperación).
        """
        log_event("[ENGINE] Iniciando loop principal", "info")

        last_recovery = time.time()

        self._stop_event.clear()

        try:
            while not self._stop_event.is_set():
                # Sleep en intervalos cortos para responder rápido a stop()
                for _ in range(poll_interval * 10):
                    if self._stop_event.is_set():
                        break
                    time.sleep(0.1 + random.uniform(0, 0.1))

                if self._stop_event.is_set():
                    break

                current_price, _ = get_current_price()
                if current_price is None:
                    log_event("[ENGINE] No se pudo obtener precio actual, reintentando...", "warning")
                    continue

                with self._state_lock:
                    self.current_price = current_price

                filled, _ = self.detect_fills(current_price)

                if filled:
                    fill_snapshots: Dict[str, OrderInfo] = {}
                    fill_log_entries: List[Tuple[str, str]] = []

                    with self._state_lock:
                        for key in filled:
                            info = self.active_orders.get(key)
                            if info is None:
                                continue

                            fill_snapshots[key] = self._clone_order_info(info)
                            if not self._is_virtual_order(info):
                                self.fill_history.append({
                                    "side": info["side"],
                                    "price": key,
                                    "order_id": info["order_id"],
                                    "ts": time.time(),
                                })
                                fill_log_entries.append((str(info["side"]), key))
                            else:
                                log_event(f"[ENGINE] Orden virtual activada: {key}", "info")
                            del self.active_orders[key]

                    for side, key in fill_log_entries:
                        order_size = fill_snapshots[key].get('size', self.base_size)
                        log_event(f"[ENGINE] Orden ejecutada: {fill_snapshots[key]['order_id']}")
                        log_fill(side, key, fmt_amount(Decimal(str(order_size))))

                    sells = sorted(
                        [(key, snapshot) for key, snapshot in fill_snapshots.items() if snapshot["side"] == "sell"],
                        key=lambda item: Decimal(item[0])
                    )
                    buys = sorted(
                        [(key, snapshot) for key, snapshot in fill_snapshots.items() if snapshot["side"] == "buy"],
                        key=lambda item: Decimal(item[0]),
                        reverse=True,
                    )
                    for key, snapshot in sells + buys:
                        self.rebalance_after_fill(key, snapshot)

                now = time.time()
                if now - last_recovery >= recovery_interval:
                    log_event(f"[ENGINE] Precio actual: {current_price}", "info")
                    last_recovery = now

        except KeyboardInterrupt:
            self._stop_event.set()

        except Exception as e:
            log_event(f"[ENGINE] Error inesperado: {e}", "error")
            _notify(f"💥 Engine caído\nError: {e}")
            self._stop_event.set()

        # Parada limpia (por stop() o Ctrl-C)
        log_event("[ENGINE] Detenido.", "info")
        if self.save_state():
            log_event(f"[STATE] Estado guardado al salir en {STATE_PATH}", "info")
