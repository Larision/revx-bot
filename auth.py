import os
import base64
from typing import Optional

from nacl.signing import SigningKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from private_config import get_revolut_api_key, get_signing_private_pem_path
from logger import log_event


def _read_api_key() -> Optional[str]:
    """
    Lee la API key en este orden:
    1. Variable de entorno REVX_API_KEY
    2. private_config.ini [revolut] api_key
    """
    k = os.environ.get("REVX_API_KEY")
    if k:
        return k.strip()

    try:
        k = get_revolut_api_key()
        if k:
            return k
    except Exception as e:
        log_event(f"Error leyendo api_key desde private_config.ini: {e}", "error")

    log_event("No se encontró API key (env REVX_API_KEY o private_config.ini).", "warning")
    return None


def _load_signing_key() -> tuple[Optional[bytes], bool]:
    """
    Carga la clave privada Ed25519 desde entorno o desde la ruta
    indicada en private_config.ini.

    Retorna (raw_private_bytes, signing_available).
    """
    pem_data: Optional[bytes] = None

    env_pem = os.environ.get("REVX_PRIVATE_PEM")
    if env_pem:
        pem_data = env_pem.encode("utf-8")
    else:
        try:
            pem_path = get_signing_private_pem_path()
            if pem_path is not None:
                if pem_path.exists():
                    pem_data = pem_path.read_bytes()
                else:
                    log_event(
                        f"La ruta de clave privada no existe: {pem_path}",
                        "warning",
                    )
        except Exception as e:
            log_event(f"Error leyendo PEM desde private_config.ini: {e}", "warning")

    if not pem_data:
        log_event(
            "No se encontró clave privada (env REVX_PRIVATE_PEM o private_config.ini [signing] private_pem_path).",
            "warning",
        )
        return None, False

    try:
        private_key_obj = load_pem_private_key(
            pem_data,
            password=None,
            backend=default_backend(),
        )
        raw = private_key_obj.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        log_event("Clave privada Ed25519 Raw cargada correctamente.", "info")
        return raw, True

    except Exception as e:
        log_event(f"No se pudo parsear la clave privada: {e}; firma desactivada.", "warning")
        return None, False


API_KEY: Optional[str] = _read_api_key()
_raw_private, SIGNING_AVAILABLE = _load_signing_key()


def sign_request(
    timestamp: str,
    method: str,
    path: str,
    query: str = "",
    body: str = "",
) -> str:
    """
    Firma el mensaje de la request con la clave privada Ed25519.
    El mensaje es la concatenación de timestamp + method + path + query + body.
    Retorna la firma en base64. Lanza RuntimeError si la firma no está disponible.
    """
    if not SIGNING_AVAILABLE or _raw_private is None:
        raise RuntimeError("Firma Ed25519 no disponible.")
    msg = f"{timestamp}{method}{path}{query}{body}".encode("utf-8")
    signed = SigningKey(_raw_private).sign(msg)
    return base64.b64encode(signed.signature).decode()
