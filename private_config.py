"""
private_config.py — Lectura centralizada de configuración privada.

Lee private_config.ini con el siguiente formato:

    [revolut]
    api_key = TU_API_KEY

    [telegram]
    enabled = true
    token   = TU_TOKEN
    chat_id = 123456789

Los archivos individuales (api.key, telegramapi.token, telegram_chatid.txt)
siguen siendo compatibles como fallback si no existe private_config.ini.
Si [telegram] enabled = false, el programa no cargará ni arrancará el bot.
"""

import configparser
from pathlib import Path
from typing import Optional

PRIVATE_CONFIG_PATH = Path("private_config.ini")

_config: Optional[configparser.ConfigParser] = None


def _load() -> configparser.ConfigParser:
    global _config
    if _config is None:
        _config = configparser.ConfigParser()
        if PRIVATE_CONFIG_PATH.exists():
            _config.read(PRIVATE_CONFIG_PATH, encoding="utf-8")
    return _config


def get_revolut_api_key() -> Optional[str]:
    """Lee api_key de [revolut] en private_config.ini."""
    cfg = _load()
    val = cfg.get("revolut", "api_key", fallback=None)
    return val.strip() if val else None


def get_telegram_token() -> Optional[str]:
    """Lee token de [telegram] en private_config.ini."""
    cfg = _load()
    val = cfg.get("telegram", "token", fallback=None)
    return val.strip() if val else None


def get_telegram_enabled(default: bool = True) -> bool:
    """Lee enabled de [telegram] en private_config.ini. Por defecto: True."""
    cfg = _load()
    if not cfg.has_section("telegram"):
        return default

    raw = cfg.get("telegram", "enabled", fallback=None)
    if raw is None:
        return default

    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on", "si", "sí", "s"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def get_telegram_chat_id() -> Optional[int]:
    """Lee chat_id de [telegram] en private_config.ini."""
    cfg = _load()
    val = cfg.get("telegram", "chat_id", fallback=None)
    if val:
        try:
            return int(val.strip())
        except ValueError:
            pass
    return None
