from decimal import Decimal
from typing import TypedDict


class LogEntry(TypedDict):
    """Entrada de log estructurada para pasar entre funciones via collector."""
    level: str
    msg: str


class OrderInfo(TypedDict):
    """Representa una orden activa registrada en el grid engine."""
    side: str        # "buy" o "sell"
    order_id: str    # venue_order_id de la API, o "virtual" para centinelas
    price: Decimal   # precio de la orden
    placed_at: float # timestamp Unix de cuando se registró
