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
        self.levels: List[Decimal]           = []
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
        self.trailing_down_enabled: bool = True

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

    def set_trailing(self, up: bool, down: bool) -> None:
        with self._state_lock:
            self.trailing_up_enabled = up
            self.trailing_down_enabled = down

        log_event(
            f"[ENGINE] Trailing actualizado → up: {'ON' if up else 'OFF'} | down: {'ON' if down else 'OFF'}",
            "info"
        )

    # ----------------------------------------------------------
    # SNAPSHOTS / THREAD SAFETY
    # ----------------------------------------------------------

    def _clone_order_info(self, info: OrderInfo) -> OrderInfo:
        return {
            "side": str(info["side"]),
            "order_id": str(info["order_id"]),
            "price": info["price"],
            "placed_at": float(info["placed_at"]),
        }

    def _build_state_snapshot_locked(self) -> Dict[str, Any]:
        return {
            "version": VERSION,
            "steps_each_side": self.steps_each_side,
            "levels_below": self.levels_below,
            "levels_above": self.levels_above,
            "step_percent": str(self.step_percent),
            "base_size": str(self.base_size),
            "center_price": str(self.center_price) if self.center_price else None,
            "step": str(self.step) if self.step else None,
            "levels": [str(level) for level in self.levels],
            "active_orders": {
                key: {
                    "side": info["side"],
                    "order_id": info["order_id"],
                    "price": str(info["price"]),
                    "placed_at": info["placed_at"],
                }
                for key, info in self.active_orders.items()
            },
            "last_fill_side": self.last_fill_side,
            "last_fill_price": str(self.last_fill_price) if self.last_fill_price is not None else None,
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
                "levels": list(self.levels),
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

        if order_id in {"virtual", "pending_post_only", "pending_manual"}:
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

    def save_state(self) -> None:
        """
        Persiste el estado actual del motor en STATE_PATH (grid_state.json).
        Se llama automáticamente tras cada cambio relevante.
        """
        with self._state_lock:
            state = self._build_state_snapshot_locked()

        try:
            STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
        except Exception as e:
            log_event(f"[STATE] Error guardando estado: {e}", "error")

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
            levels = [Decimal(level) for level in raw.get("levels", [])]
            active_orders = {
                key: cast(OrderInfo, {
                    "side": info["side"],
                    "order_id": info["order_id"],
                    "price": Decimal(info["price"]),
                    "placed_at": float(info.get("placed_at", 0)),
                })
                for key, info in raw.get("active_orders", {}).items()
            }
            last_fill_side = raw.get("last_fill_side")
            last_fill_price = (
                Decimal(raw["last_fill_price"])
                if raw.get("last_fill_price") is not None else None
            )

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
                self.levels = levels
                self.active_orders = active_orders
                self.last_fill_side = last_fill_side
                self.last_fill_price = last_fill_price

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
            self.levels = levels

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

    def place_order(self, price: Decimal, side: str) -> None:
        """
        Envía una orden limit post_only a la API y la registra en active_orders.
        Si la API no devuelve venue_order_id, loguea el error y no registra la orden.
        """
        body = {
            "client_order_id": str(uuid.uuid4()),
            "symbol": SYMBOL,
            "side": side,
            "order_configuration": {
                "limit": {
                    "base_size": fmt_amount(self.base_size),
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
                self.active_orders[key] = {
                    "side": side,
                    "order_id": "pending_post_only",
                    "price": price,
                    "placed_at": time.time(),
                }
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
            self.active_orders[key] = {
                "side": side,
                "order_id": order_id,
                "price": price,
                "placed_at": time.time(),
            }

        log_event(f"[ENGINE] Orden {side} registrada en {_price_key(price)} -> {order_id}")
        self.save_state()

    def _place_order_safe(self, price: Decimal, side: str, *, max_retries: int = 3, retry_delay: float = 0.6) -> bool:
        """
        Wrapper sobre place_order que verifica saldo antes de enviar.
        Reintenta hasta max_retries veces con backoff si el saldo no está disponible aún
        (p.ej. el exchange tarda en liberar reservas tras una cancelación reciente).
        Devuelve True si la orden fue registrada, False si falló.
        """
        if self.step is None:
            return False

        key = _price_key(price)

        for attempt in range(1, max_retries + 1):
            with self._state_lock:
                if key in self.active_orders:
                    return False

            balances_resp, _ = get_all_balances()
            usdc_balance, btc_balance = _parse_balances(balances_resp)

            if side == "buy":
                required = self.base_size * price
                if usdc_balance < required:
                    if attempt < max_retries:
                        wait = retry_delay * attempt  # 0.6s, 1.2s, 1.8s
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
                if btc_balance < self.base_size:
                    if attempt < max_retries:
                        wait = retry_delay * attempt
                        log_event(
                            f"[ENGINE] Saldo BTC insuficiente para SELL en {_price_key(price)} "
                            f"({fmt_amount(btc_balance)} < {fmt_amount(self.base_size)}) "
                            f"— reintento {attempt}/{max_retries} en {wait:.1f}s",
                            "warning"
                        )
                        time.sleep(wait)
                        continue
                    log_event(
                        f"[ENGINE] Saldo BTC insuficiente para SELL en {_price_key(price)}: "
                        f"disponible {fmt_amount(btc_balance)} < requerido {fmt_amount(self.base_size)}",
                        "warning"
                    )
                    return False

            # Saldo OK en este intento
            self.place_order(price, side)
            with self._state_lock:
                return key in self.active_orders

        return False  # nunca debería llegar aquí, pero por seguridad

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

            if oid == "pending_manual":
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
                        with self._state_lock:
                            current = self.active_orders.get(key)
                            if current is not None and current["order_id"] == "pending_post_only":
                                del self.active_orders[key]
                                state_changed = True
                                should_retry = True
                        if should_retry:
                            self._place_order_safe(lvl_price, oside)
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
        Rebalancea el grid tras un fill. Recibe el snapshot de la orden ejecutada
        (ya eliminada de active_orders) para no depender del estado actual.

        Casos:
          - BUY en nivel más bajo  → trailing down: extiende grid hacia abajo,
                                     coloca SELL un step arriba, registra virtual BUY en el nuevo suelo.
          - SELL en nivel más alto → trailing up: extiende grid hacia arriba,
                                     coloca BUY un step abajo, registra virtual SELL en el nuevo techo.
          - BUY intermedio         → coloca SELL un step arriba.
          - SELL intermedio        → coloca BUY un step abajo.
        """

        side: str = info["side"]
        price: Decimal = info["price"]

        cancel_order_id: Optional[str] = None
        cancel_level_key: Optional[str] = None
        order_to_place: Optional[Tuple[Decimal, str]] = None
        virtual_order_to_add: Optional[Tuple[str, OrderInfo]] = None
        trailing_log: Optional[str] = None

        with self._state_lock:
            self.last_fill_side = side
            self.last_fill_price = price

            if self.step is None or not self.levels:
                log_event("[ENGINE] step es None o no hay niveles para rebalancear", "error")
                return

            step = self.step
            levels_snapshot = sorted(set(self.levels))
            lowest = min(levels_snapshot)
            highest = max(levels_snapshot)
            max_levels = self.levels_below + self.levels_above + 2

            if side == "buy" and price == lowest:
                if not self.trailing_down_enabled:
                    log_event("[ENGINE] Trailing DOWN desactivado → no se extiende grid", "info")
                else:
                    trail_down_price = (price - step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                    self.levels.append(trail_down_price)

                    if len(self.levels) > max_levels:
                        self.levels.remove(highest)
                        highest_key = _price_key(highest)
                        removed = self.active_orders.pop(highest_key, None)
                        if removed is not None and removed["order_id"] not in {"virtual", "pending_post_only", "pending_manual"}:
                            cancel_order_id = str(removed["order_id"])
                            cancel_level_key = highest_key

                    order_to_place = ((price + step).quantize(TICK_SIZE, rounding=ROUND_DOWN), "sell")
                    trail_down_key = _price_key(trail_down_price)
                    if trail_down_key not in self.active_orders:
                        virtual_order_to_add = (
                            trail_down_key,
                            {
                                "side": "buy",
                                "order_id": "virtual",
                                "price": trail_down_price,
                                "placed_at": time.time(),
                            },
                        )

                    trailing_log = f"[ENGINE] Rebalance trailing down: grid extendido a {_price_key(trail_down_price)}"

            elif side == "sell" and price == highest:
                if not self.trailing_up_enabled:
                    log_event("[ENGINE] Trailing UP desactivado → no se extiende grid", "info")
                else:
                    trail_up_price = (price + step).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                    self.levels.append(trail_up_price)

                    if len(self.levels) > max_levels:
                        self.levels.remove(lowest)
                        lowest_key = _price_key(lowest)
                        removed = self.active_orders.pop(lowest_key, None)
                        if removed is not None and removed["order_id"] not in {"virtual", "pending_post_only", "pending_manual"}:
                            cancel_order_id = str(removed["order_id"])
                            cancel_level_key = lowest_key

                    order_to_place = ((price - step).quantize(TICK_SIZE, rounding=ROUND_DOWN), "buy")
                    trail_up_key = _price_key(trail_up_price)
                    if trail_up_key not in self.active_orders:
                        virtual_order_to_add = (
                            trail_up_key,
                            {
                                "side": "sell",
                                "order_id": "virtual",
                                "price": trail_up_price,
                                "placed_at": time.time(),
                            },
                        )

                    trailing_log = f"[ENGINE] Rebalance trailing up: grid extendido a {_price_key(trail_up_price)}"

            elif side == "buy":
                order_to_place = ((price + step).quantize(TICK_SIZE, rounding=ROUND_DOWN), "sell")

            elif side == "sell":
                order_to_place = ((price - step).quantize(TICK_SIZE, rounding=ROUND_DOWN), "buy")

            self.levels = sorted(set(self.levels))

        if cancel_order_id is not None and cancel_level_key is not None:
            log_event(
                f"[ENGINE] Cancelando orden en {cancel_level_key} ({cancel_order_id}) — nivel eliminado por rebalance",
                "info"
            )
            self.cancel_order(cancel_order_id)

        if order_to_place is not None:
            self._place_order_safe(order_to_place[0], order_to_place[1])

        if virtual_order_to_add is not None:
            virtual_key, virtual_info = virtual_order_to_add
            with self._state_lock:
                if virtual_key not in self.active_orders:
                    self.active_orders[virtual_key] = virtual_info
            log_event(
                f"[ENGINE] Orden virtual {str(virtual_info['side']).upper()} registrada en {virtual_key}",
                "info"
            )

        if trailing_log:
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

            if skip_level is not None and level == skip_level:
                log_event(
                    f"[ENGINE] Nivel {key} dejado vacío intencionalmente "
                    f"({skip_reason}, último fill: {last_fill_side or 'desconocido'})"
                )
                continue

            if level < current_price:
                log_event(f"[ENGINE] Nivel vacío detectado, colocando BUY en {key}")
                self._place_order_safe(level, "buy")
            elif level > current_price:
                log_event(f"[ENGINE] Nivel vacío detectado, colocando SELL en {key}")
                self._place_order_safe(level, "sell")

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
                            self.fill_history.append({
                                "side": info["side"],
                                "price": key,
                                "order_id": info["order_id"],
                                "ts": time.time(),
                            })
                            fill_log_entries.append((str(info["side"]), key))
                            del self.active_orders[key]

                    for side, key in fill_log_entries:
                        log_event(f"[ENGINE] Orden ejecutada: {fill_snapshots[key]['order_id']}")
                        log_fill(side, key, fmt_amount(self.base_size))

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
        self.save_state()
