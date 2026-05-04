"""Helpers compartidos para configurar y mostrar los modos de trailing."""

from typing import Optional


TRAILING_MODES = ("off", "on", "extended")
TRAILING_DOWN_MODES = TRAILING_MODES
TRAILING_UP_MODES = TRAILING_MODES


def _normalize_trailing_mode(value: object, *, true_mode: str = "on") -> str:
    """Normaliza valores legacy o de entrada de usuario a off/on/extended."""
    if isinstance(value, bool):
        return true_mode if value else "off"

    mode = str(value).strip().lower()
    if mode in TRAILING_MODES:
        return mode
    if mode == "extendido":
        return "extended"
    return "off"


def normalize_trailing_down_mode(value: object) -> str:
    """Convierte valores legacy o de entrada de usuario al modo de trailing down."""
    return _normalize_trailing_mode(value, true_mode="on")


def normalize_trailing_up_mode(value: object) -> str:
    """Convierte valores legacy o de entrada de usuario al modo de trailing up.

    Los estados antiguos guardaban trailing_up_enabled=True para el comportamiento
    actualmente implementado, que ahora se considera "extended".
    """
    return _normalize_trailing_mode(value, true_mode="extended")


def _parse_trailing_mode(value: str) -> Optional[str]:
    """Parsea texto de usuario y devuelve un modo valido, o None si no encaja."""
    normalized = value.strip().lower()
    if normalized in TRAILING_MODES:
        return normalized
    if normalized == "extendido":
        return "extended"
    return None


def parse_trailing_down_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing down recibido por el usuario."""
    return _parse_trailing_mode(value)


def parse_trailing_up_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing up recibido por el usuario."""
    return _parse_trailing_mode(value)


def trailing_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado."""
    return {
        "off": "OFF",
        "on": "ON",
        "extended": "EXTENDIDO",
    }.get(mode, mode.upper())


def trailing_down_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado de trailing down."""
    return trailing_mode_label(mode)


def trailing_up_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado de trailing up."""
    return trailing_mode_label(mode)
