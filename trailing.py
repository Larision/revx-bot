"""Helpers y politica interna para configurar/ejecutar trailing."""

import time
from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from api import (
    _parse_balances,
    _price_key,
    fmt_amount,
    get_all_balances,
    replace_order as api_replace_order,
)
from config import TICK_SIZE
from logger import log_event
from types_ import LogEntry, OrderInfo


TRAILING_DOWN_MODES = ("off", "on", "extended")
TRAILING_UP_MODES = ("off", "on", "extended", "fixed_quote")
TRAILING_MODES = TRAILING_DOWN_MODES


_FIXED_QUOTE_ALIASES = {
    "fixed_quote",
    "fixed-quote",
    "fixedquote",
    "quote",
    "quote_fijo",
    "quote-fijo",
    "quotefijo",
    "fijo",
}


def _normalize_trailing_mode(
    value: object,
    *,
    true_mode: str = "on",
    valid_modes: tuple[str, ...] = TRAILING_DOWN_MODES,
) -> str:
    """Normaliza valores legacy o de entrada de usuario a un modo valido."""
    if isinstance(value, bool):
        return true_mode if value else "off"

    mode = str(value).strip().lower()
    if mode in valid_modes:
        return mode
    if mode == "extendido":
        return "extended"
    if "fixed_quote" in valid_modes and mode in _FIXED_QUOTE_ALIASES:
        return "fixed_quote"
    return "off"


def normalize_trailing_down_mode(value: object) -> str:
    """Convierte valores legacy o de entrada de usuario al modo de trailing down."""
    return _normalize_trailing_mode(
        value,
        true_mode="on",
        valid_modes=TRAILING_DOWN_MODES,
    )


def normalize_trailing_up_mode(value: object) -> str:
    """Convierte valores legacy o de entrada de usuario al modo de trailing up.

    Los estados antiguos guardaban trailing_up_enabled=True para el comportamiento
    actualmente implementado, que ahora se considera "extended".
    """
    return _normalize_trailing_mode(
        value,
        true_mode="extended",
        valid_modes=TRAILING_UP_MODES,
    )


def _parse_trailing_mode(
    value: str,
    *,
    valid_modes: tuple[str, ...] = TRAILING_DOWN_MODES,
) -> Optional[str]:
    """Parsea texto de usuario y devuelve un modo valido, o None si no encaja."""
    normalized = value.strip().lower()
    if normalized in valid_modes:
        return normalized
    if normalized == "extendido":
        return "extended"
    if "fixed_quote" in valid_modes and normalized in _FIXED_QUOTE_ALIASES:
        return "fixed_quote"
    return None


def parse_trailing_down_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing down recibido por el usuario."""
    return _parse_trailing_mode(value, valid_modes=TRAILING_DOWN_MODES)


def parse_trailing_up_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing up recibido por el usuario."""
    return _parse_trailing_mode(value, valid_modes=TRAILING_UP_MODES)


def trailing_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado."""
    return {
        "off": "OFF",
        "on": "ON",
        "extended": "EXTENDIDO",
        "fixed_quote": "QUOTE FIJO",
    }.get(mode, mode.upper())


def trailing_down_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado de trailing down."""
    return {
        "off": "OFF",
        "on": "ON (BUY REAL -5%)",
        "extended": "EXTENDIDO (BUY VIRTUAL)",
    }.get(mode, mode.upper())


def trailing_up_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado de trailing up."""
    return trailing_mode_label(mode)


class TrailingPolicyMixin:
    """Mixin con helpers de trailing usados por GridEngine."""

    base_size: Decimal
    center_price: Optional[Decimal]
    levels_above: int
    levels: List[Decimal]
    active_orders: Dict[str, OrderInfo]
    extended_levels: Dict[str, Decimal]
    reserve_usdc: Decimal
    step: Optional[Decimal]
    trailing_up_mode: str
    trailing_up_enabled: bool
    trailing_down_mode: str
    trailing_down_enabled: bool
    trailing_up_ext_reduction_per_level: Decimal
    trailing_up_ext_min_factor: Decimal
    _trailing_up_ext_steps: int
    _trailing_up_fixed_quote_anchor: Optional[Decimal]
    _trailing_down_extended_drops: int
    _state_lock: Any

    if TYPE_CHECKING:
        def _order_size(self, info: OrderInfo) -> Decimal: ...
        def _get_base_step_locked(self) -> Decimal: ...
        def _get_available_usdc(self) -> Decimal: ...
        def _get_available_btc(self) -> Decimal: ...
        def _clone_order_info(self, info: OrderInfo) -> OrderInfo: ...
        def cancel_order(self, order_id: str) -> Tuple[Dict[str, Any], List[LogEntry]]: ...
        def save_state(self) -> bool: ...

    def _normalise_trailing_down_mode(self, down: object) -> str:
        """Normaliza el modo de trailing down a 'off', 'on' o 'extended'."""
        return normalize_trailing_down_mode(down)

    def _normalise_trailing_up_mode(self, up: object) -> str:
        """Normaliza el modo de trailing up a 'off', 'on', 'extended' o 'fixed_quote'."""
        return normalize_trailing_up_mode(up)

    def _is_extended_down_order(self, info: Optional[OrderInfo]) -> bool:
        """Retorna True si la orden pertenece al grid extendido inferior."""
        return bool(info) and bool(info.get("extended"))

    def _extended_down_order_size(self) -> Decimal:
        """Tamano fijo de las ordenes down extended: 50% del base_size."""
        return self.base_size * Decimal("0.5")

    def _extended_up_factor_for_steps(self, steps: Optional[int] = None) -> Decimal:
        """Factor de tamano para el trailing up extended segun el contador actual."""
        safe_steps = max(0, self._trailing_up_ext_steps if steps is None else int(steps))
        factor = Decimal("1") - (self.trailing_up_ext_reduction_per_level * Decimal(safe_steps))
        if factor < self.trailing_up_ext_min_factor:
            return self.trailing_up_ext_min_factor
        return factor

    def _extended_up_size_for_steps(self, steps: Optional[int] = None) -> Decimal:
        """Tamano base dinamico para nuevos niveles de trailing up extended."""
        return self.base_size * self._extended_up_factor_for_steps(steps)

    def _current_extended_up_size(self) -> Decimal:
        """Tamano que corresponde al contador actual de trailing up extended."""
        return self._extended_up_size_for_steps(self._trailing_up_ext_steps)

    def _current_trailing_up_fixed_quote_anchor_locked(self) -> Optional[Decimal]:
        """Valor central de toda la rejilla actual usado al iniciar fixed_quote."""
        levels = sorted(set(self.levels))
        if not levels:
            return (
                Decimal(str(self.center_price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                if self.center_price is not None else None
            )

        midpoint = len(levels) // 2
        if len(levels) % 2 == 1:
            anchor = levels[midpoint]
        else:
            anchor = (levels[midpoint - 1] + levels[midpoint]) / Decimal("2")

        return Decimal(str(anchor)).quantize(TICK_SIZE, rounding=ROUND_DOWN)

    def _lock_trailing_up_fixed_quote_anchor_locked(self) -> Optional[Decimal]:
        """Fija el ancla de fixed_quote en el valor central de toda la rejilla."""
        anchor = self._current_trailing_up_fixed_quote_anchor_locked()
        self._trailing_up_fixed_quote_anchor = anchor
        return anchor

    def _trailing_up_fixed_quote_anchor_locked(self) -> Optional[Decimal]:
        """Devuelve el ancla fijada de fixed_quote o un fallback actual."""
        if self._trailing_up_fixed_quote_anchor is not None:
            return Decimal(str(self._trailing_up_fixed_quote_anchor)).quantize(
                TICK_SIZE,
                rounding=ROUND_DOWN,
            )
        return self._current_trailing_up_fixed_quote_anchor_locked()

    def _trailing_up_fixed_quote_locked(self) -> Decimal:
        """Quote fijo del trailing up: ancla fixed_quote * base_size."""
        anchor = self._trailing_up_fixed_quote_anchor_locked()
        if anchor is None or self.base_size <= 0:
            return Decimal("0")
        return Decimal(str(anchor)) * self.base_size

    def _trailing_up_fixed_quote_size_locked(self, price: Decimal) -> Decimal:
        """Calcula size para fixed_quote usando quote_fijo / precio."""
        price_dec = Decimal(str(price))
        quote = self._trailing_up_fixed_quote_locked()
        if quote <= 0 or price_dec <= 0:
            return self.base_size

        calculated = quote / price_dec
        capped = min(calculated, self.base_size)
        return capped.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    def _refresh_trailing_up_fixed_quote_after_resize_locked(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Recalcula el ancla fixed_quote tras devolver ordenes a base_size."""
        previous_anchor = self._trailing_up_fixed_quote_anchor_locked()
        new_anchor = self._lock_trailing_up_fixed_quote_anchor_locked()
        return previous_anchor, new_anchor

    def _trailing_up_anchor_high_locked(self) -> Optional[Decimal]:
        """Techo original del grid principal, usado como ancla del trailing up."""
        if self.center_price is None:
            return None

        try:
            base_step = self._get_base_step_locked()
        except Exception:
            return None

        return (
            Decimal(str(self.center_price))
            + (Decimal(self.levels_above) * base_step)
        ).quantize(TICK_SIZE, rounding=ROUND_DOWN)

    def _trailing_up_price_step_locked(self, price: Decimal) -> int:
        """Numero de lineas que un precio esta por encima del techo principal."""
        anchor_high = self._trailing_up_anchor_high_locked()
        if anchor_high is None:
            return 0

        try:
            base_step = self._get_base_step_locked()
        except Exception:
            return 0

        if base_step <= 0 or price <= anchor_high:
            return 0

        raw_steps = (Decimal(str(price)) - anchor_high) / base_step
        try:
            return max(0, int(raw_steps.to_integral_value(rounding=ROUND_DOWN)))
        except Exception:
            return 0

    def _trailing_up_step_from_size(self, size: Decimal) -> int:
        """Infiere el step por size para estados antiguos sin metadata."""
        if self.base_size <= 0 or self.trailing_up_ext_reduction_per_level <= 0:
            return 0

        try:
            factor = Decimal(str(size)) / self.base_size
        except Exception:
            return 0

        if factor >= Decimal("1"):
            return 0

        if factor <= self.trailing_up_ext_min_factor:
            min_steps = (
                (Decimal("1") - self.trailing_up_ext_min_factor)
                / self.trailing_up_ext_reduction_per_level
            )
            return max(0, int(min_steps.to_integral_value(rounding=ROUND_DOWN)))

        inferred = (Decimal("1") - factor) / self.trailing_up_ext_reduction_per_level
        try:
            return max(0, int(inferred.to_integral_value(rounding=ROUND_DOWN)))
        except Exception:
            return 0

    def _trailing_up_step_from_order_locked(
        self,
        price: Decimal,
        side: str,
        info: Optional[OrderInfo] = None,
    ) -> int:
        """Devuelve el step logico de trailing up asociado a una orden."""
        if info is not None:
            raw_step = info.get("trailing_up_step")
            if raw_step is not None:
                try:
                    parsed = int(raw_step)
                    if parsed > 0:
                        return parsed
                except Exception:
                    pass

            size_step = self._trailing_up_step_from_size(self._order_size(info))
            if size_step > 0:
                return size_step

        price_step = self._trailing_up_price_step_locked(price)
        if side == "buy" and price_step > 0:
            return price_step + 1
        return price_step

    def _trailing_up_metadata_for_step(self, step: int) -> Optional[Dict[str, Any]]:
        """Metadata comun para ordenes que pertenecen al trailing up hibrido."""
        if step <= 0:
            return None
        return {"trailing_up_step": int(step)}

    def _trailing_up_size_for_step(self, step: int) -> Decimal:
        """Tamano que corresponde a un trailing_up_step concreto."""
        if step <= 0:
            return self.base_size
        return self._extended_up_size_for_steps(step)

    def _trailing_up_size_from_metadata(
        self,
        metadata: Optional[Dict[str, Any]],
        default_size: Decimal,
    ) -> Decimal:
        """Calcula el tamano efectivo a partir de metadata de trailing up."""
        if not metadata:
            return default_size

        raw_step = metadata.get("trailing_up_step")
        if raw_step is None:
            return default_size

        try:
            step = int(raw_step)
        except Exception:
            return default_size

        return self._trailing_up_size_for_step(step)

    def _update_trailing_up_steps_after_buy_locked(
        self,
        filled_key: str,
        price: Decimal,
        info: OrderInfo,
        logs: List[str],
    ) -> None:
        """Actualiza el contador al bajar una linea de trailing up."""
        filled_step = self._trailing_up_step_from_order_locked(price, "buy", info)
        next_steps = max(0, filled_step - 1)
        previous_steps = self._trailing_up_ext_steps
        self._trailing_up_ext_steps = next_steps

        if previous_steps != next_steps:
            logs.append(
                f"[ENGINE] Trailing up: contador ajustado {previous_steps} -> "
                f"{next_steps} tras BUY en {filled_key}; "
                f"size actual {fmt_amount(self._current_extended_up_size())}"
            )

    def _update_trailing_up_steps_after_sell_locked(
        self,
        filled_key: str,
        price: Decimal,
        info: OrderInfo,
        logs: List[str],
    ) -> int:
        """Actualiza el contador al subir por niveles ya existentes de trailing up."""
        filled_step = self._trailing_up_step_from_order_locked(price, "sell", info)
        if filled_step <= 0:
            return 0

        previous_steps = self._trailing_up_ext_steps
        self._trailing_up_ext_steps = filled_step
        if previous_steps != filled_step:
            logs.append(
                f"[ENGINE] Trailing up: contador ajustado {previous_steps} -> "
                f"{filled_step} tras SELL en {filled_key}; "
                f"size actual {fmt_amount(self._current_extended_up_size())}"
            )
        return filled_step

    # ----------------------------------------------------------------------
    #  Seleccion y mantenimiento de centinelas virtuales de trailing
    # ----------------------------------------------------------------------

    def _find_highest_real_sell_order(
        self,
        *,
        include_extended: bool = False,
    ) -> Optional[Tuple[str, OrderInfo]]:
        """Devuelve la orden SELL real más alta para liberar BTC.

        Por defecto mantiene el comportamiento anterior y excluye SELL extended.
        Cuando include_extended=True también permite cancelar SELL extended.
        """
        with self._state_lock:
            candidates: List[Tuple[Decimal, str, OrderInfo]] = []
            for key, info in self.active_orders.items():
                if info.get('side') != 'sell':
                    continue
                order_id = str(info.get('order_id'))
                if order_id in {'virtual', 'pending_post_only', 'pending_manual', 'pending_cancel', 'pending_replace'}:
                    continue
                if not include_extended and self._is_extended_down_order(info):
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
                if order_id in {'virtual', 'pending_post_only', 'pending_manual', 'pending_cancel', 'pending_replace'}:
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
        """Elimina una virtual BUY antigua del suelo del grid."""
        with self._state_lock:
            removed_keys = self._prune_floor_virtual_buys_locked(keep_key=None)
            return removed_keys[0] if removed_keys else None

    def _prune_floor_virtual_buys_locked(self, keep_key: Optional[str] = None) -> List[str]:
        """
        Deja una sola BUY virtual de suelo y elimina las demas.

        La BUY virtual representa el siguiente centinela inferior del grid. Cuando
        trailing up cancela BUYs bajos para liberar USDC, ese centinela debe
        desplazarse al ultimo BUY cancelado, no acumular todos los anteriores.
        Debe llamarse con self._state_lock adquirido.
        """
        candidates: List[Tuple[Decimal, str]] = []
        for key, info in self.active_orders.items():
            if info.get("side") != "buy":
                continue
            if str(info.get("order_id")) != "virtual":
                continue
            try:
                candidates.append((Decimal(key), key))
            except Exception:
                continue

        if not candidates:
            return []

        if keep_key is None:
            # Si no se indica una clave concreta, conserva la virtual mas cercana
            # al grid real: la de mayor precio entre las BUY virtuales existentes.
            _, keep_key = max(candidates, key=lambda item: item[0])

        keys_to_remove = {key for _, key in candidates if key != keep_key}
        if not keys_to_remove:
            return []

        for key in keys_to_remove:
            self.active_orders.pop(key, None)
            self.extended_levels.pop(key, None)

        self.levels = [
            lvl for lvl in self.levels
            if _price_key(lvl) not in keys_to_remove
        ]
        return sorted(keys_to_remove, key=lambda value: Decimal(value))

    def _metadata_for_virtual_from_cancelled_order(self, info: OrderInfo) -> Dict[str, Any]:
        """Conserva metadata relevante al convertir una BUY real cancelada en virtual."""
        metadata: Dict[str, Any] = {}
        for meta_key in (
            "extended",
            "grid_step",
            "paired_buy_price",
            "paired_sell_price",
            "trailing_up_step",
        ):
            if meta_key in info and info.get(meta_key) is not None:
                metadata[meta_key] = info.get(meta_key)
        return metadata

    def _metadata_for_ceiling_virtual_from_cancelled_sell(self, info: OrderInfo) -> Dict[str, Any]:
        """Metadata segura para una SELL virtual creada al cancelar una SELL real.

        No se copia metadata de trailing_down/extended, porque esta virtual actua
        como centinela de rebote superior. Si se marca como extended, al activarse
        entraria en la rama de subrejilla inferior y no reactivaria trailing up.
        """
        metadata: Dict[str, Any] = {}
        raw_step = info.get("trailing_up_step")
        if raw_step is not None:
            try:
                step = int(raw_step)
                if step > 0:
                    metadata["trailing_up_step"] = step
            except Exception:
                pass
        return metadata

    def _replace_floor_virtual_after_cancel(
        self,
        canceled_price: Decimal,
        size: Decimal,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Recoloca la virtual BUY de suelo tras cancelar un BUY real bajo.

        Corrige el caso en el que cada cancelacion iba dejando una BUY virtual
        antigua en active_orders/levels. La funcion crea o actualiza el nuevo
        centinela y elimina el resto de BUYs virtuales inferiores.
        """
        key = _price_key(canceled_price)
        removed_virtuals: List[str]
        with self._state_lock:
            current = self.active_orders.get(key)
            if current is not None and str(current.get("order_id")) != "virtual":
                return []

            self.active_orders[key] = cast(OrderInfo, {
                "side": "buy",
                "order_id": "virtual",
                "price": canceled_price,
                "size": size,
                "placed_at": time.time(),
                **(metadata or {}),
            })
            self.levels.append(canceled_price)
            self.levels = sorted(set(self.levels))
            removed_virtuals = self._prune_floor_virtual_buys_locked(keep_key=key)

        return removed_virtuals

    def _prune_ceiling_virtual_sells_locked(self, keep_key: Optional[str] = None) -> List[str]:
        """
        Deja una sola SELL virtual de techo y elimina las demas.

        La SELL virtual representa el siguiente centinela superior del grid. Cuando
        trailing down cancela SELLs altos para liberar BTC, ese centinela debe
        desplazarse al ultimo SELL cancelado para que el grid pueda enganchar un
        rebote posterior. Debe llamarse con self._state_lock adquirido.
        """
        candidates: List[Tuple[Decimal, str]] = []
        for key, info in self.active_orders.items():
            if info.get("side") != "sell":
                continue
            if str(info.get("order_id")) != "virtual":
                continue
            try:
                candidates.append((Decimal(key), key))
            except Exception:
                continue

        if not candidates:
            return []

        if keep_key is None:
            # Si no se indica una clave concreta, conserva la virtual mas cercana
            # al grid real: la de menor precio entre las SELL virtuales existentes.
            _, keep_key = min(candidates, key=lambda item: item[0])

        keys_to_remove = {key for _, key in candidates if key != keep_key}
        if not keys_to_remove:
            return []

        for key in keys_to_remove:
            self.active_orders.pop(key, None)
            self.extended_levels.pop(key, None)

        self.levels = [
            lvl for lvl in self.levels
            if _price_key(lvl) not in keys_to_remove
        ]
        return sorted(keys_to_remove, key=lambda value: Decimal(value), reverse=True)

    def _replace_ceiling_virtual_after_cancel(
        self,
        canceled_price: Decimal,
        size: Decimal,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Recoloca la virtual SELL de techo tras cancelar una SELL real alta.

        Es el equivalente superior de _replace_floor_virtual_after_cancel: evita
        que trailing down deje el grid sin centinela de subida tras desmontar
        ordenes SELL para liberar BTC.
        """
        key = _price_key(canceled_price)
        removed_virtuals: List[str]
        with self._state_lock:
            current = self.active_orders.get(key)
            if current is not None and str(current.get("order_id")) != "virtual":
                return []

            self.active_orders[key] = cast(OrderInfo, {
                "side": "sell",
                "order_id": "virtual",
                "price": canceled_price,
                "size": size,
                "placed_at": time.time(),
                **(metadata or {}),
            })
            self.levels.append(canceled_price)
            self.levels = sorted(set(self.levels))
            removed_virtuals = self._prune_ceiling_virtual_sells_locked(keep_key=key)

        return removed_virtuals

    def _remove_highest_virtual_sell_order(self) -> Optional[str]:
        """Elimina la virtual SELL mas alta del techo."""
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

    # ----------------------------------------------------------------------
    #  Gestión de trailing (liberación de USDC o BTC para BUY/SELL activados por trailing)
    # ----------------------------------------------------------------------

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

        Usa primero el USDC libre por encima de la reserva configurada y cancela
        BUYs reales desde la parte mas baja de la rejilla solo hasta cubrir el
        deficit restante. Si max_cancellations es None, no usa un limite fijo:
        el tope natural son los BUYs reales cancelables disponibles.
        """
        required = Decimal(str(target_price)) * Decimal(str(target_size))
        target_key = _price_key(target_price)
        excluded_keys: set[str] = {target_key}
        cancellations = 0

        estimated_available = self._get_available_usdc()
        if estimated_available >= required:
            return True

        while estimated_available < required:
            if max_cancellations is not None and cancellations >= max_cancellations:
                log_event(
                    f"[ENGINE] Trailing up: USDC insuficiente para BUY {target_key} "
                    f"tras {cancellations} cancelaciones "
                    f"({_price_key(estimated_available)} disponible < {_price_key(required)})",
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
                    f"({_price_key(estimated_available)} disponible < {_price_key(required)})",
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

                removed_floor_virtuals: List[str] = []
                if self._normalise_trailing_down_mode(self.trailing_down_mode) == "extended":
                    removed_floor_virtuals = self._replace_floor_virtual_after_cancel(
                        canceled_price=cancel_price,
                        size=cancel_size,
                        metadata=self._metadata_for_virtual_from_cancelled_order(cancel_info),
                    )
                if removed_floor_virtuals:
                    log_event(
                        f"[ENGINE] Trailing up: virtuales BUY antiguas eliminadas "
                        f"tras mover suelo a {cancel_level_key}: "
                        + ", ".join(removed_floor_virtuals),
                        "info",
                    )

                estimated_available += estimated_release
                if estimated_available >= required:
                    log_event(
                        f"[ENGINE] Trailing up: saldo estimado disponible con BUY {cancel_level_key}; "
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

    def _release_btc_for_trailing_down_sell(
        self,
        target_price: Decimal,
        target_size: Decimal,
        *,
        max_cancellations: Optional[int] = None,
        retry_delay: float = 1.0,
    ) -> bool:
        """
        Libera BTC para un SELL creado por trailing_down_extended.

        Con trailing up hibrido, las SELL altas pueden tener sizes distintos.
        Por eso no sirve cancelar una orden cada dos drops: se cancelan tantas
        SELL reales altas como haga falta, sumando su size real.
        """
        required = Decimal(str(target_size))
        target_key = _price_key(target_price)
        cancellations = 0

        estimated_available = self._get_available_btc()
        if estimated_available >= required:
            return True

        while estimated_available < required:
            if max_cancellations is not None and cancellations >= max_cancellations:
                log_event(
                    f"[ENGINE] Trailing down: BTC insuficiente para SELL {target_key} "
                    f"tras {cancellations} cancelaciones "
                    f"({fmt_amount(estimated_available)} < {fmt_amount(required)})",
                    "warning"
                )
                return False

            candidate = self._find_highest_real_sell_order(include_extended=True)
            if candidate is None:
                refreshed_available = self._get_available_btc()
                if refreshed_available > estimated_available:
                    estimated_available = refreshed_available
                if estimated_available >= required:
                    return True

                log_event(
                    f"[ENGINE] Trailing down: no hay SELL real alto cancelable "
                    f"para liberar BTC ({fmt_amount(estimated_available)} < {fmt_amount(required)})",
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

            with self._state_lock:
                current = self.active_orders.get(cancel_level_key)
                if current is None or current.get("order_id") != cancel_order_id:
                    continue
                current["order_id"] = "pending_cancel"

            log_event(
                f"[ENGINE] Trailing down: cancelando SELL alto {cancel_level_key} "
                f"size {fmt_amount(cancel_size)} para liberar BTC antes de SELL {target_key} "
                f"size {fmt_amount(required)}",
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

                if cancel_level_key != target_key:
                    removed_ceiling_virtuals = self._replace_ceiling_virtual_after_cancel(
                        canceled_price=cancel_price,
                        size=cancel_size,
                        metadata=self._metadata_for_ceiling_virtual_from_cancelled_sell(cancel_info),
                    )
                    log_event(
                        f"[ENGINE] Trailing down: SELL virtual de techo movida a "
                        f"{cancel_level_key} tras cancelar SELL alto para liberar BTC",
                        "info",
                    )
                    if removed_ceiling_virtuals:
                        log_event(
                            f"[ENGINE] Trailing down: virtuales SELL antiguas eliminadas "
                            f"tras mover techo a {cancel_level_key}: "
                            + ", ".join(removed_ceiling_virtuals),
                            "info",
                        )

                estimated_available += cancel_size
                if estimated_available >= required:
                    log_event(
                        f"[ENGINE] Trailing down: BTC estimado liberado con SELL "
                        f"{cancel_level_key}; _place_order_safe esperara si el exchange "
                        f"aun no actualizo el disponible",
                        "info"
                    )
                    return True

                if retry_delay > 0:
                    time.sleep(retry_delay)
                    refreshed_available = self._get_available_btc()
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
                f"[ENGINE] Trailing down: cancel fallido en {cancel_level_key}: "
                f"{error_msg} ({error_id})",
                "warning"
            )
            return False

        return True

    # ----------------------------------------------------------
    #  Trailing up fixed_quote: resize a base_size
    # ----------------------------------------------------------

    def _fixed_quote_resize_candidates_locked(
        self,
    ) -> Tuple[
        List[Tuple[str, OrderInfo, Decimal, Decimal]],
        List[Tuple[str, OrderInfo, Decimal, Decimal]],
    ]:
        """Detecta ordenes trailing_up fixed_quote que pueden volver a base_size.

        Retorna dos listas con tuplas (price_key, snapshot_info, price, current_size):
        - reales: requieren replace_order y saldo disponible para el incremento.
          SELL requiere BTC extra; BUY requiere USDC extra.
        - state_only: virtuales/latentes que solo existen en el estado local.
        """
        default_size = Decimal(str(self.base_size))
        if default_size <= 0:
            return [], []

        anchor = self._trailing_up_fixed_quote_anchor_locked()
        if anchor is None:
            return [], []

        real_candidates: List[Tuple[str, OrderInfo, Decimal, Decimal]] = []
        state_only_candidates: List[Tuple[str, OrderInfo, Decimal, Decimal]] = []
        ignored_ids = {"pending_manual", "pending_cancel", "pending_replace"}

        for key, info in sorted(
            self.active_orders.items(),
            key=lambda item: Decimal(str(item[0])),
            reverse=True,
        ):
            side = str(info.get("side")).lower()
            if side not in {"sell", "buy"}:
                continue
            if self._is_extended_down_order(info):
                continue

            try:
                price = Decimal(str(info.get("price", key))).quantize(TICK_SIZE, rounding=ROUND_DOWN)
            except Exception:
                try:
                    price = Decimal(str(key)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
                except Exception:
                    continue

            # En fixed_quote, cualquier orden por encima del ancla puede quedar
            # por debajo del base_size: tanto SELLs superiores como BUYs pareja
            # creados durante el trailing up. Los niveles en/ancla o por debajo
            # ya deben estar capados a base_size y no se tocan.
            if price <= anchor:
                continue

            current_size = self._order_size(info)
            if current_size >= default_size:
                continue

            order_id = str(info.get("order_id"))
            if order_id in ignored_ids:
                continue

            cloned = self._clone_order_info(info)
            if order_id in {"virtual", "pending_post_only"}:
                state_only_candidates.append((key, cloned, price, current_size))
            else:
                real_candidates.append((key, cloned, price, current_size))

        return real_candidates, state_only_candidates

    def preview_resize_trailing_up_fixed_quote_to_default(self) -> Dict[str, Any]:
        """Previsualiza que ordenes fixed_quote se redimensionarian a base_size."""
        with self._state_lock:
            mode = self._normalise_trailing_up_mode(self.trailing_up_mode)
            default_size = Decimal(str(self.base_size))
            anchor = self._trailing_up_fixed_quote_anchor_locked()

            if mode != "fixed_quote":
                return {
                    "enabled": False,
                    "reason": "trailing_up no está en fixed_quote",
                    "default_size": default_size,
                    "anchor": anchor,
                    "real_orders": [],
                    "state_only_orders": [],
                    "required_btc": Decimal("0"),
                    "required_usdc": Decimal("0"),
                }

            real_candidates, state_only_candidates = self._fixed_quote_resize_candidates_locked()
            required_btc = sum(
                (
                    default_size - current_size
                    for _, info, _, current_size in real_candidates
                    if str(info.get("side")).lower() == "sell"
                ),
                Decimal("0"),
            )
            required_usdc = sum(
                (
                    (default_size - current_size) * price
                    for _, info, price, current_size in real_candidates
                    if str(info.get("side")).lower() == "buy"
                ),
                Decimal("0"),
            )

            def _items(candidates: List[Tuple[str, OrderInfo, Decimal, Decimal]]) -> List[Dict[str, Any]]:
                rows: List[Dict[str, Any]] = []
                for key, info, price, current_size in candidates:
                    side = str(info.get("side")).lower()
                    delta = default_size - current_size
                    rows.append({
                        "price_key": key,
                        "price": price,
                        "side": side,
                        "order_id": str(info.get("order_id")),
                        "current_size": current_size,
                        "target_size": default_size,
                        "delta": delta,
                        "required_btc_delta": delta if side == "sell" else Decimal("0"),
                        "required_usdc_delta": (delta * price) if side == "buy" else Decimal("0"),
                    })
                return rows

            return {
                "enabled": True,
                "reason": None,
                "default_size": default_size,
                "anchor": anchor,
                "real_orders": _items(real_candidates),
                "state_only_orders": _items(state_only_candidates),
                "required_btc": required_btc,
                "required_usdc": required_usdc,
            }

    def resize_trailing_up_fixed_quote_to_default(
        self,
    ) -> Tuple[bool, List[LogEntry], Optional[str], Dict[str, Any]]:
        """Redimensiona las ordenes trailing_up fixed_quote al base_size del grid.

        Las ordenes reales se actualizan con api.replace_order, porque el exchange
        cambia el venue_order_id al reemplazarlas. Las virtuales o latentes solo
        se actualizan en el estado local, ya que no existen todavia en el exchange.

        Para el incremento de tamaño:
        - SELL requiere BTC disponible.
        - BUY requiere USDC disponible por el coste extra de la orden.
        """
        logs: List[LogEntry] = []
        summary: Dict[str, Any] = {
            "resized_real": 0,
            "updated_state_only": 0,
            "skipped": 0,
            "failed": [],
            "required_btc": Decimal("0"),
            "available_btc": Decimal("0"),
            "required_usdc": Decimal("0"),
            "available_usdc": Decimal("0"),
            "previous_anchor": None,
            "new_anchor": None,
            "new_fixed_quote": Decimal("0"),
        }

        with self._state_lock:
            mode = self._normalise_trailing_up_mode(self.trailing_up_mode)
            if mode != "fixed_quote":
                return False, logs, "Resize to default solo está disponible con trailing_up=fixed_quote.", summary

            default_size = Decimal(str(self.base_size))
            real_candidates, state_only_candidates = self._fixed_quote_resize_candidates_locked()
            required_btc = sum(
                (
                    default_size - current_size
                    for _, info, _, current_size in real_candidates
                    if str(info.get("side")).lower() == "sell"
                ),
                Decimal("0"),
            )
            required_usdc = sum(
                (
                    (default_size - current_size) * price
                    for _, info, price, current_size in real_candidates
                    if str(info.get("side")).lower() == "buy"
                ),
                Decimal("0"),
            )
            summary["required_btc"] = required_btc
            summary["required_usdc"] = required_usdc

        if not real_candidates and not state_only_candidates:
            return True, logs, None, summary

        if required_btc > 0 or required_usdc > 0:
            balances_resp, balance_logs = get_all_balances()
            logs.extend(balance_logs)
            usdc_balance, available_btc = _parse_balances(balances_resp)
            available_usdc = usdc_balance - self.reserve_usdc
            if available_usdc < 0:
                available_usdc = Decimal("0")

            summary["available_btc"] = available_btc
            summary["available_usdc"] = available_usdc

            insufficient_parts: List[str] = []
            if available_btc < required_btc:
                insufficient_parts.append(
                    f"BTC {fmt_amount(available_btc)} < {fmt_amount(required_btc)}"
                )
            if available_usdc < required_usdc:
                insufficient_parts.append(
                    f"USDC {_price_key(available_usdc)} < {_price_key(required_usdc)}"
                )

            if insufficient_parts:
                msg = (
                    "Saldo disponible insuficiente para redimensionar fixed_quote: "
                    + " | ".join(insufficient_parts)
                )
                log_event(f"[ENGINE] {msg}", "warning")
                return False, logs, msg, summary

        for key, info_snapshot, price, current_size in real_candidates:
            old_order_id = str(info_snapshot.get("order_id"))
            side_label = str(info_snapshot.get("side")).upper()
            target_size = Decimal(str(self.base_size))

            with self._state_lock:
                current = self.active_orders.get(key)
                if (
                    current is None
                    or str(current.get("order_id")) != old_order_id
                    or self._order_size(current) != current_size
                ):
                    summary["skipped"] += 1
                    continue
                current["order_id"] = "pending_replace"
                current["placed_at"] = time.time()

            new_order_id, replace_logs = api_replace_order(
                old_order_id,
                price=price,
                base_size=target_size,
            )
            logs.extend(replace_logs)

            if new_order_id:
                with self._state_lock:
                    current = self.active_orders.get(key)
                    if current is not None and str(current.get("order_id")) == "pending_replace":
                        current["order_id"] = new_order_id
                        current["size"] = target_size
                        current["price"] = price
                        current["placed_at"] = time.time()
                        summary["resized_real"] += 1

                log_event(
                    f"[ENGINE] Resize fixed_quote: {side_label} {key} "
                    f"{fmt_amount(current_size)} -> {fmt_amount(target_size)} "
                    f"({old_order_id} -> {new_order_id})",
                    "info",
                )
                continue

            with self._state_lock:
                current = self.active_orders.get(key)
                if current is not None and str(current.get("order_id")) == "pending_replace":
                    current["order_id"] = old_order_id
                    current["size"] = current_size
                    current["price"] = price
                    current["placed_at"] = info_snapshot.get("placed_at", time.time())

            failure = f"{side_label} {key} ({old_order_id})"
            cast(List[str], summary["failed"]).append(failure)
            log_event(
                f"[ENGINE] Resize fixed_quote fallido en {failure}",
                "warning",
            )

        for key, info_snapshot, price, current_size in state_only_candidates:
            target_size = Decimal(str(self.base_size))
            old_order_id = str(info_snapshot.get("order_id"))
            side_label = str(info_snapshot.get("side")).upper()
            with self._state_lock:
                current = self.active_orders.get(key)
                if (
                    current is None
                    or str(current.get("order_id")) != old_order_id
                    or self._order_size(current) != current_size
                ):
                    summary["skipped"] += 1
                    continue

                current["size"] = target_size
                current["price"] = price
                current["placed_at"] = time.time()
                summary["updated_state_only"] += 1

            log_event(
                f"[ENGINE] Resize fixed_quote local: {side_label} {key} "
                f"{fmt_amount(current_size)} -> {fmt_amount(target_size)} ({old_order_id})",
                "info",
            )

        changed = bool(summary["resized_real"] or summary["updated_state_only"])
        if changed:
            with self._state_lock:
                previous_anchor, new_anchor = self._refresh_trailing_up_fixed_quote_after_resize_locked()
                summary["previous_anchor"] = previous_anchor
                summary["new_anchor"] = new_anchor
                summary["new_fixed_quote"] = self._trailing_up_fixed_quote_locked()

            if previous_anchor != new_anchor:
                log_event(
                    "[ENGINE] Resize fixed_quote: ancla recalculada "
                    f"{_price_key(previous_anchor) if previous_anchor is not None else 'N/A'} -> "
                    f"{_price_key(new_anchor) if new_anchor is not None else 'N/A'}; "
                    f"quote {_price_key(cast(Decimal, summary['new_fixed_quote']))}",
                    "info",
                )
            self.save_state()

        failed = cast(List[str], summary["failed"])
        if failed:
            return False, logs, "Algunas órdenes no se pudieron redimensionar.", summary

        return True, logs, None, summary

    # ----------------------------------------------------------

    def set_trailing(self, up: object, down: object) -> None:
        """Actualiza la configuracion de trailing sin reiniciar el engine."""
        up_mode = self._normalise_trailing_up_mode(up)
        down_mode = self._normalise_trailing_down_mode(down)
        removed_virtuals: List[str] = []
        fixed_quote_log = ""

        with self._state_lock:
            previous_up_mode = self.trailing_up_mode
            self.trailing_up_mode = up_mode
            self.trailing_up_enabled = up_mode != "off"
            self.trailing_down_mode = down_mode
            self.trailing_down_enabled = down_mode != "off"
            if up_mode != "extended":
                self._trailing_up_ext_steps = 0
            if up_mode == "fixed_quote":
                current_anchor = self._current_trailing_up_fixed_quote_anchor_locked()
                stored_anchor = self._trailing_up_fixed_quote_anchor_locked()
                should_relock_anchor = (
                    previous_up_mode != "fixed_quote"
                    or self._trailing_up_fixed_quote_anchor is None
                    or (
                        current_anchor is not None
                        and stored_anchor is not None
                        and stored_anchor != current_anchor
                    )
                )
                if should_relock_anchor:
                    anchor = self._lock_trailing_up_fixed_quote_anchor_locked()
                else:
                    anchor = stored_anchor
                quote = self._trailing_up_fixed_quote_locked()
                if anchor is not None and quote > 0:
                    fixed_quote_log = (
                        f" | anchor {_price_key(anchor)} | quote {_price_key(quote)}"
                    )
            else:
                self._trailing_up_fixed_quote_anchor = None
            if down_mode != "extended":
                self._trailing_down_extended_drops = 0

            keys_to_remove: set[str] = set()
            for key, info in list(self.active_orders.items()):
                order_id = str(info.get("order_id"))
                if order_id != "virtual":
                    continue

                side = str(info.get("side"))
                if up_mode == "off" and side == "sell":
                    keys_to_remove.add(key)
                    removed_virtuals.append(f"SELL {key}")
                elif down_mode != "extended" and side == "buy":
                    keys_to_remove.add(key)
                    removed_virtuals.append(f"BUY {key}")

            if keys_to_remove:
                for key in keys_to_remove:
                    self.active_orders.pop(key, None)
                    self.extended_levels.pop(key, None)

                self.levels = [
                    level for level in self.levels
                    if _price_key(level) not in keys_to_remove
                ]

        log_event(
            f"[ENGINE] Trailing actualizado -> up: {up_mode.upper()} | "
            f"down: {down_mode.upper()}{fixed_quote_log}",
            "info",
        )
        if removed_virtuals:
            log_event(
                "[ENGINE] Virtuales eliminadas por modo trailing: "
                + ", ".join(sorted(removed_virtuals)),
                "info",
            )

        with self._state_lock:
            should_save_state = (
                self.center_price is not None
                and self.step is not None
                and bool(self.levels)
            )
        if should_save_state:
            self.save_state()
