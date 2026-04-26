from decimal import Decimal
from typing import TypedDict, NotRequired


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
    size: Decimal    # tamaño de la orden
    
    # trailing extended
    grid_step: NotRequired[Decimal]
    extended: NotRequired[bool]
    paired_buy_price: NotRequired[Decimal]
    paired_sell_price: NotRequired[Decimal]
