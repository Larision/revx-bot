import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import BASE_URL
from types_ import LogEntry
from logger import log_event
from auth import API_KEY, sign_request


SESSION = requests.Session()

_retries = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "PUT"]  # POST y DELETE excluidos: no son idempotentes
)
SESSION.mount("https://", HTTPAdapter(max_retries=_retries))
SESSION.mount("http://",  HTTPAdapter(max_retries=_retries))

# Offset en ms entre reloj local y servidor (local + _server_offset = servidor).
# Se actualiza con cada respuesta que incluya un campo timestamp.
_server_offset: int = 0


def _synced_timestamp() -> str:
    """Devuelve el timestamp actual en ms corregido con el offset del servidor."""
    return str(int(time.time() * 1000) + _server_offset)


def _update_server_offset(response_body: Any) -> None:
    """
    Actualiza _server_offset a partir del timestamp devuelto por el servidor.
    Solo actúa si la respuesta contiene un campo 'timestamp' numérico y
    la diferencia con el offset actual supera OFFSET_UPDATE_THRESHOLD ms.
    """
    global _server_offset
    OFFSET_UPDATE_THRESHOLD = 50  # ms — filtra jitter de red normal

    if not isinstance(response_body, dict):
        return
    server_ts = response_body.get("timestamp") or response_body.get("metadata", {}).get("timestamp")
    if not isinstance(server_ts, (int, float)):
        return
    local_ts   = int(time.time() * 1000)
    new_offset = int(server_ts) - local_ts
    if abs(new_offset - _server_offset) >= OFFSET_UPDATE_THRESHOLD:
        log_event(
            f"[HTTP] Server offset actualizado: {_server_offset:+d}ms -> {new_offset:+d}ms",
            "info"
        )
        _server_offset = new_offset


def send_request(
    method: str,
    path: str,
    query: str = "",
    body: Optional[Dict[str, Any]] = None
) -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Envía una request autenticada a la API de Revolut.
    Gestiona firma Ed25519, reintentos automáticos y parsing de la respuesta.
    Retorna (response_dict, logs). En caso de error retorna un dict con 'error': True.
    """

    logs: List[LogEntry] = []

    if not API_KEY:
        log_event("API_KEY no disponible. Configura REVX_API_KEY o el archivo api.key.", "error", logs)
        return {"error": True, "status_code": None, "body": "missing api key"}, logs

    url       = f"{BASE_URL}{path}" + (f"?{query}" if query else "")
    body_str  = json.dumps(body, separators=(",", ":")) if body else ""

    def _perform_request():
        timestamp = _synced_timestamp()
        signature = sign_request(timestamp, method, path, query, body_str)

        headers = {
            "Accept": "application/json",
            "X-Revx-Timestamp": timestamp,
            "X-Revx-Signature": signature,
            "X-Revx-API-Key": API_KEY
        }
        if method.upper() == "POST":
            headers["Content-Type"] = "application/json"

        if method.upper() == "GET":
            return SESSION.get(url, headers=headers, timeout=10)
        if method.upper() == "DELETE":
            return SESSION.delete(url, headers=headers, timeout=10)
        if method.upper() == "POST":
            return SESSION.post(url, headers=headers, timeout=10, data=body_str)

        raise ValueError(f"Método HTTP no soportado: {method}")

    try:
        r = _perform_request()
    except requests.RequestException as exc:
        log_event(f"Request exception: {exc}", "error", logs)
        return {"error": True, "status_code": None, "body": str(exc)}, logs
    except Exception as exc:
        log_event(f"Error preparando request: {exc}", "error", logs)
        return {"error": True, "status_code": None, "body": str(exc)}, logs

    if r.status_code == 409:
        try:
            err = r.json()
        except ValueError:
            err = r.text

        _update_server_offset(err)
        log_event(f"API error {r.status_code} -> {err}", "error", logs)

        try:
            r = _perform_request()
        except requests.RequestException as exc:
            log_event(f"Retry request exception: {exc}", "error", logs)
            return {"error": True, "status_code": None, "body": str(exc)}, logs

    if not r.ok:
        try:
            err = r.json()
        except ValueError:
            err = r.text
        _update_server_offset(err)
        log_event(f"API error {r.status_code} -> {err}", "error", logs)
        return {"error": True, "status_code": r.status_code, "body": err}, logs

    try:
        response_json = r.json()
        _update_server_offset(response_json)
        return response_json, logs
    except ValueError:
        if r.status_code == 204:
            log_event(f"Respuesta vacía (status {r.status_code}), operación exitosa.", "info", logs)
            return {"status_code": r.status_code, "text": ""}, logs
        log_event(f"Respuesta vacía. Comprobar manualmente.(status {r.status_code})", "warning", logs)
        return {"status_code": r.status_code, "text": r.text}, logs
