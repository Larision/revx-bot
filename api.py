import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from config import SYMBOL, TICK_SIZE
from types_ import LogEntry
from logger import log_event
from http_client import send_request


# =========================================================
# ========================= UTILITIES =====================
# =========================================================

def fmt_amount(d: Decimal) -> str:
    """Formatea un Decimal eliminando ceros finales y punto decimal innecesario."""
    s = format(d.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _price_key(price: Decimal) -> str:
    """
    Convierte un precio Decimal a string con precisión TICK_SIZE, redondeando hacia abajo.
    Se usa como clave en active_orders y para formatear precios en logs y requests.
    """
    from decimal import ROUND_DOWN
    q = price.quantize(TICK_SIZE, rounding=ROUND_DOWN)
    return format(q, "f")


def _parse_balances(balances_resp: Any) -> Tuple[Decimal, Decimal]:
    """
    Extrae saldos disponibles de USDC y BTC desde la respuesta de la API.
    Soporta múltiples formatos de respuesta.
    Retorna (usdc_balance, btc_balance).
    """
    usdc_balance = Decimal("0")
    btc_balance  = Decimal("0")
    data_list: List[Any] = []

    if isinstance(balances_resp, dict):
        if "balances" in balances_resp and isinstance(balances_resp["balances"], list):
            data_list = balances_resp["balances"]
        elif "data" in balances_resp and isinstance(balances_resp["data"], list):
            data_list = balances_resp["data"]
        else:
            for v in balances_resp.values():
                if isinstance(v, list):
                    data_list = v
                    break
    elif isinstance(balances_resp, list):
        data_list = balances_resp

    for entry in data_list:
        if not isinstance(entry, dict):
            continue
        symbol    = (entry.get("currency") or entry.get("symbol") or "").upper()
        available = entry.get("available") or entry.get("balance") or "0"
        try:
            amount = Decimal(str(available))
        except Exception:
            amount = Decimal("0")
        if symbol.startswith("USDC"):
            usdc_balance = amount
        elif symbol.startswith("BTC"):
            btc_balance = amount

    return usdc_balance, btc_balance


# =========================================================
# ========================= API CALLS =====================
# =========================================================

def get_active_orders() -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Retrieve all active orders (new, pending_new, partially_filled).

    Response example:
    {
        "data": [
            {
                "id": "039fe856-...",
                "symbol": "BTC/USDC",
                "side": "buy",
                "price": "60000",
                "status": "new",
                ...
            }
        ],
        "metadata": { "next_cursor": "", "timestamp": 1772907590741 }
    }
    """
    response, logs = send_request(
        "GET",
        "/api/1.0/orders/active",
        query="order_states=new&order_states=pending_new&order_states=partially_filled"
    )
    return response, logs

def get_order_by_id(order_id: str) -> Tuple[Dict[str, Any], List[LogEntry]]:
    """Recupera una orden por ID.
    
    Ejemplo de respuesta:
    {
        "data": {
            "id": "7a52e92e-8639-4fe1-abaa-68d3a2d5234b",
            "client_order_id": "984a4d8a-2a9b-4950-822f-2a40037f02bd",
            "symbol": "BTC/USD",
            "side": "buy",
            "type": "limit",
            "quantity": "0.002",
            "filled_quantity": "0",
            "leaves_quantity": "0.002",
            "price": "98745",
            "average_fill_price": "89794.51",
            "status": "new",
            "time_in_force": "gtc",
            "execution_instructions": [
            "allow_taker"
            ],
            "created_date": 3318215482991,
            "updated_date": 3318215482991
        }
    }
    """
    response, logs = send_request("GET", f"/api/1.0/orders/{order_id}")
    return response, logs

def get_all_balances() -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Recupera todos los saldos de la cuenta.

    Response example:
    [
        { "currency": "BTC",  "available": "1.25", "reserved": "0.10", "total": "1.35" },
        { "currency": "USDC", "available": "5400", "reserved": "100",  "total": "5500" }
    ]
    """
    response, logs = send_request("GET", "/api/1.0/balances")
    return response, logs


def cancel_order(order_id: str) -> Tuple[Dict[str, Any], List[LogEntry]]:
    """Cancela una orden por ID. Returns 204 No Content on success."""
    response, logs = send_request("DELETE", f"/api/1.0/orders/{order_id}")
    return response, logs


def cancel_all_orders() -> Tuple[Dict[str, Any], List[LogEntry]]:
    """Cancela todas las órdenes activas. Returns 204 No Content on success."""
    response, logs = send_request("DELETE", "/api/1.0/orders")
    return response, logs


def place_order(
    side: str,
    price: Decimal,
    base_size: Decimal
) -> Tuple[Optional[str], List[LogEntry]]:
    """
    Envía una orden limit post_only a la API.
    Retorna (order_id, logs) donde order_id es el venue_order_id o None si falló.
    """
    import uuid
    logs: List[LogEntry] = []

    body = {
        "client_order_id": str(uuid.uuid4()),
        "symbol": SYMBOL,
        "side": side,
        "order_configuration": {
            "limit": {
                "base_size": fmt_amount(base_size),
                "price": _price_key(price),
                "execution_instructions": ["post_only"]
            }
        }
    }

    resp, req_logs = send_request("POST", "/api/1.0/orders", body=body)
    logs.extend(req_logs)

    order_id: Optional[str] = None
    if isinstance(resp, dict) and not resp.get("error"):
        data = resp.get("data")
        if isinstance(data, dict):
            venue = data.get("venue_order_id")
            if isinstance(venue, str):
                order_id = venue

    if not order_id:
        log_event(f"[API] place_order: no se obtuvo venue_order_id. Respuesta: {resp}", "error", logs)

    return order_id, logs


def get_historical_orders(limit: int = 50) -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Retrieve filled orders from the historical endpoint.

    Args:
        limit: número de órdenes a recuperar. Se calcula dinámicamente en
               detect_fills como len(active_orders) + margen para garantizar
               cobertura completa sin depender de start_date, que la API
               filtra por created_date y no por updated_date — lo cual
               causa falsos negativos en órdenes antiguas que se ejecutan tarde.

    Response example:
    {
        "data": [
            { "id": "...", "status": "filled", "price": "60000.00", ... }
        ],
        "metadata": { "next_cursor": "", "timestamp": 1772907590741 }
    }
    """
    response, logs = send_request(
        "GET",
        "/api/1.0/orders/historical",
        query=f"order_states=filled&symbols={SYMBOL}&limit={limit}"
    )
    return response, logs


MAX_TRADES_HISTORY_LIMIT = 1900


def get_market_trades_page(
    symbol: str,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
    cursor: Optional[str] = None,
    limit: int = MAX_TRADES_HISTORY_LIMIT,
) -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Recupera una pagina de trades publicos de mercado para un simbolo.

    El endpoint pagina con cursor y acepta fechas epoch en milisegundos. El
    limite se acota al maximo admitido para evitar rechazos de API.
    """
    params: Dict[str, Any] = {
        "limit": max(1, min(int(limit), MAX_TRADES_HISTORY_LIMIT))
    }

    if start_date is not None:
        params["start_date"] = int(start_date)
    if end_date is not None:
        params["end_date"] = int(end_date)
    if cursor:
        params["cursor"] = cursor

    query = urlencode(params)

    response, logs = send_request(
        "GET",
        f"/api/1.0/trades/all/{symbol}",
        query=query
    )

    return response, logs


def get_candles(
    symbol: str,
    interval: int,
    since: Optional[int] = None,
    until: Optional[int] = None,
) -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Recupera candles publicas para un simbolo e intervalo.

    Args:
        symbol: Par de mercado en formato de la API.
        interval: Duracion de cada vela segun el endpoint.
        since: Inicio opcional en epoch milliseconds.
        until: Fin opcional en epoch milliseconds.
    """
    params = {"interval": interval}

    if since:
        params["since"] = since
    if until:
        params["until"] = until

    query = urlencode(params)

    response, logs = send_request(
        "GET",
        f"/api/1.0/candles/{symbol}",
        query=query
    )

    return response, logs


def get_current_price() -> Tuple[Optional[Decimal], List[LogEntry]]:
    """
    Obtiene el mid price a partir del order book público (best bid + best ask).
    Más reactivo que el ticker — se actualiza con cada cambio en el book.
    No requiere autenticación.

    Nota: asks vienen ordenados de mayor a menor, por lo que:
      - best_bid = bids[0]  (el más alto)
      - best_ask = asks[-1] (el más bajo)
    """
    response, logs = send_request(
        "GET",
        f"/api/1.0/public/order-book/{SYMBOL}",
    )

    price: Optional[Decimal] = None

    if isinstance(response, dict):
        data = response.get("data", response)
        try:
            best_bid = Decimal(str(data["bids"][0]["p"]))
            best_ask = Decimal(str(data["asks"][-1]["p"]))  # ← último, no primero
            price = (best_bid + best_ask) / 2
        except (KeyError, IndexError, TypeError):
            price = None

    return price, logs

def get_ticker_price() -> Tuple[Dict[str, Any], List[LogEntry]]:
    """
    Obtiene el mid price a partir del ticker (last_price). Menos reactivo que el order book, se actualiza solo con trades.

    Response example:
    {
        "data": [
            {
      "symbol": "ETH/USD",
      "bid": "0.02",
      "ask": "0.02",
      "mid": "0.02",
      "last_price": "0.02"
            }
        ],
        "metadata": {
            "timestamp": 1770201294631
        }
    }
    """
    response, logs = send_request(
        "GET",
        f"/api/1.0/tickers",
        query=f"symbols={SYMBOL}"
    )
    

    return response, logs

def check_balances_for_grid(
    base_size: Decimal,
    grid: List[Decimal],
    center_price: Optional[Decimal] = None,
) -> Tuple[bool, List[LogEntry]]:
    """Verifica que hay saldo suficiente para cubrir todos los niveles del grid."""
    logs: List[LogEntry] = []
    balances_resp, blogs = get_all_balances()
    logs.extend(blogs)

    usdc_balance, btc_balance = _parse_balances(balances_resp)

    if center_price is None:
        mid_idx     = len(grid) // 2
        buy_prices  = grid[:mid_idx]
        sell_prices = grid[mid_idx + 1:]
    else:
        buy_prices  = [p for p in grid if p < center_price]
        sell_prices = [p for p in grid if p > center_price]

    required_usdc = sum((base_size * p for p in buy_prices), Decimal("0"))
    required_btc  = base_size * Decimal(len(sell_prices))

    if usdc_balance < required_usdc:
        log_event(
            f"Saldo USDC insuficiente: {_price_key(usdc_balance)} < {_price_key(required_usdc)}",
            "warning", logs
        )
        return False, logs

    if btc_balance < required_btc:
        log_event(
            f"Saldo BTC insuficiente: {_price_key(btc_balance)} < {fmt_amount(required_btc)}",
            "warning", logs
        )
        return False, logs

    log_event("Saldos suficientes para grid.", "info", logs)
    return True, logs
