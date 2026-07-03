"""Helpers y politica interna para configurar/ejecutar trailing."""

from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from api import _price_key, fmt_amount
from config import TICK_SIZE
from logger import log_event
from types_ import OrderInfo


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
    return trailing_mode_label(mode)


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
                elif down_mode == "off" and side == "buy":
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
                "[ENGINE] Virtuales eliminadas por trailing OFF: "
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
