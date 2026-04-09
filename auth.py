import os
import base64
from typing import Optional

from nacl.signing import SigningKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from config import API_KEY_PATH, PRIVATE_PEM_PATH
from logger import log_event


def _read_api_key() -> Optional[str]:
    """
    Lee la API key en este orden:
    1. Variable de entorno REVX_API_KEY
    2. private_config.ini [revolut] api_key
    3. Archivo api.key (legacy)
    """
    k = os.environ.get("REVX_API_KEY")
    if k:
        return k.strip()

    try:
        from private_config import get_revolut_api_key
        k = get_revolut_api_key()
        if k:
            return k
    except Exception:
        pass

    if API_KEY_PATH.exists():
        try:
            return API_KEY_PATH.read_text().strip()
        except Exception as e:
            log_event(f"Error leyendo {API_KEY_PATH}: {e}", "error")

    log_event("No se encontró API key (env REVX_API_KEY, private_config.ini o api.key).", "warning")
    return None


def _load_signing_key() -> tuple[Optional[bytes], bool]:
    """
    Carga la clave privada Ed25519 desde entorno o fichero.
    Retorna (raw_private_bytes, signing_available).
    """
    pem_data: Optional[bytes] = None

    env_pem = os.environ.get("REVX_PRIVATE_PEM")
    if env_pem is not None:
        pem_data = env_pem.encode("utf-8")
    elif PRIVATE_PEM_PATH.exists():
        try:
            pem_data = PRIVATE_PEM_PATH.read_bytes()
        except Exception as e:
            log_event(f"Error leyendo {PRIVATE_PEM_PATH}: {e}", "warning")

    if not pem_data:
        log_event("No se encontró private.pem; firma desactivada.", "warning")
        return None, False

    try:
        private_key_obj = load_pem_private_key(
            pem_data,
            password=None,
            backend=default_backend()
        )
        raw = private_key_obj.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )
        log_event("Clave privada Ed25519 Raw cargada correctamente.", "info")
        return raw, True

    except Exception as e:
        log_event(f"No se pudo parsear private.pem: {e}; firma desactivada.", "warning")
        return None, False


API_KEY: Optional[str]   = _read_api_key()
_raw_private, SIGNING_AVAILABLE = _load_signing_key()


def sign_request(
    timestamp: str,
    method: str,
    path: str,
    query: str = "",
    body: str = ""
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
