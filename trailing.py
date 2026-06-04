"""Helpers compartidos para configurar y mostrar los modos de trailing."""

from typing import Optional


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
