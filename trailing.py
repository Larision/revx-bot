"""Helpers compartidos para configurar y mostrar los modos de trailing."""

from typing import Optional


TRAILING_DOWN_MODES = ("off", "on", "extended")


def normalize_trailing_down_mode(value: object) -> str:
    """
    Convierte valores legacy o de entrada de usuario al modo interno del engine.

    Returns:
        "off", "on" o "extended". Los valores no reconocidos se tratan como "off".
    """
    if isinstance(value, bool):
        return "on" if value else "off"

    mode = str(value).strip().lower()
    if mode in TRAILING_DOWN_MODES:
        return mode
    if mode == "extendido":
        return "extended"
    return "off"


def parse_trailing_down_mode(value: str) -> Optional[str]:
    """Parsea texto de usuario y devuelve un modo valido, o None si no encaja."""
    normalized = value.strip().lower()
    if normalized in TRAILING_DOWN_MODES:
        return normalized
    if normalized == "extendido":
        return "extended"
    return None


def trailing_down_mode_label(mode: str) -> str:
    """Devuelve la etiqueta visible para un modo normalizado de trailing down."""
    return {
        "off": "OFF",
        "on": "ON",
        "extended": "EXTENDIDO",
    }.get(mode, mode.upper())
