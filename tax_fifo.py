from __future__ import annotations

"""Ledger fiscal FIFO para compras, ventas y reportes de BTC/USDC.

El modulo mantiene lotes abiertos en JSON, registra tramos de venta en CSV y
guarda incidencias cuando una venta real no queda totalmente cubierta por lotes
fiscales conocidos.
"""

import csv
import json
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from config import SYMBOL

TAX_LOTS_PATH = Path("tax_lots.json")
TAX_SALES_PATH = Path("tax_sales.csv")
TAX_UNMATCHED_SALES_PATH = Path("tax_unmatched_sales.csv")

IGNORED_ORDER_IDS = {"", "virtual", "pending_post_only", "pending_manual", "pending_cancel"}


@dataclass
class TaxLot:
    """Lote fiscal de compra pendiente de consumir por ventas FIFO."""

    lot_id: str
    symbol: str
    buy_ts: str
    buy_price: str
    qty_total: str
    qty_open: str
    cost_total: str
    source_order_id: str
    source: str = "bot"


@dataclass
class TaxSaleMatch:
    """Tramo de venta emparejado contra un lote de compra FIFO."""

    sell_ts: str
    symbol: str
    sell_price: str
    qty: str
    buy_lot_id: str
    buy_ts: str
    buy_price: str
    cost_basis: str
    proceeds: str
    pnl: str
    source_order_id: str


@dataclass
class TaxUnmatchedSale:
    """Incidencia de venta real sin lotes fiscales suficientes."""

    sell_ts: str
    symbol: str
    sell_price: str
    quantity: str
    matched_qty: str
    missing_qty: str
    available_qty: str
    source_order_id: str
    reason: str


def _decimal_text(value: Decimal) -> str:
    """Formatea un Decimal como texto estable para JSON/CSV."""
    return format(Decimal(str(value)).normalize(), "f")


def _parse_ts(value: str) -> datetime:
    """Convierte timestamps de lotes a datetime para ordenacion FIFO."""
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return datetime.max


def _sort_lots_fifo(lots: list[TaxLot]) -> list[TaxLot]:
    """Ordena lotes por fecha de compra conservando el orden relativo original."""
    indexed = list(enumerate(lots))
    indexed.sort(key=lambda item: (_parse_ts(item[1].buy_ts), item[0]))
    return [lot for _, lot in indexed]


def _is_trackable_order_id(order_id: str) -> bool:
    """Indica si un order_id pertenece a una orden real registrable."""
    return str(order_id).strip() not in IGNORED_ORDER_IDS


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Lee filas CSV como diccionarios y devuelve lista vacia si no existe."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _sale_order_already_recorded(order_id: str, symbol: str) -> bool:
    """Comprueba si una venta real ya esta registrada o marcada como incidencia."""
    if not _is_trackable_order_id(order_id):
        return False

    for row in _read_csv_rows(TAX_SALES_PATH):
        if row.get("source_order_id") == order_id and row.get("symbol") == symbol:
            return True

    for row in _read_csv_rows(TAX_UNMATCHED_SALES_PATH):
        if row.get("source_order_id") == order_id and row.get("symbol") == symbol:
            return True

    return False


def _unmatched_sale_already_recorded(order_id: str, symbol: str) -> bool:
    """Comprueba si una incidencia de venta ya existe para evitar duplicados."""
    if not _is_trackable_order_id(order_id):
        return False

    for row in _read_csv_rows(TAX_UNMATCHED_SALES_PATH):
        if row.get("source_order_id") == order_id and row.get("symbol") == symbol:
            return True

    return False


def _find_existing_lot_by_order(lots: list[TaxLot], order_id: str, symbol: str) -> Optional[TaxLot]:
    """Busca un lote existente por order_id y symbol para idempotencia."""
    if not _is_trackable_order_id(order_id):
        return None

    for lot in lots:
        if lot.symbol == symbol and lot.source_order_id == order_id:
            return lot
    return None


def record_tax_fill(
    *,
    side: str,
    price: Decimal,
    quantity: Decimal,
    order_id: str,
    ts: Optional[str] = None,
    symbol: str = SYMBOL,
) -> None:
    """Registra un fill real de exchange como compra o venta fiscal FIFO."""
    if ts is None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")

    normalized_side = side.strip().lower()
    price_dec = Decimal(str(price))
    quantity_dec = Decimal(str(quantity))

    if price_dec <= 0:
        raise ValueError(f"FIFO fiscal: precio invalido: {price}")
    if quantity_dec <= 0:
        raise ValueError(f"FIFO fiscal: cantidad invalida: {quantity}")

    if normalized_side == "buy":
        add_buy_lot(
            symbol=symbol,
            buy_ts=ts,
            buy_price=price_dec,
            quantity=quantity_dec,
            order_id=str(order_id),
        )
    elif normalized_side == "sell":
        consume_fifo_sell(
            symbol=symbol,
            sell_ts=ts,
            sell_price=price_dec,
            quantity=quantity_dec,
            order_id=str(order_id),
        )
    else:
        raise ValueError(f"FIFO fiscal: lado invalido: {side}")


def add_buy_lot(
    *,
    symbol: str,
    buy_ts: str,
    buy_price: Decimal,
    quantity: Decimal,
    order_id: str,
    source: str = "bot",
) -> TaxLot:
    """Anade un lote de compra si no existe ya para la orden indicada."""
    lots = load_lots()

    existing = _find_existing_lot_by_order(lots, str(order_id), symbol)
    if existing is not None:
        return existing

    lot = TaxLot(
        lot_id=str(uuid.uuid4()),
        symbol=symbol,
        buy_ts=buy_ts,
        buy_price=_decimal_text(buy_price),
        qty_total=_decimal_text(quantity),
        qty_open=_decimal_text(quantity),
        cost_total=_decimal_text(buy_price * quantity),
        source_order_id=str(order_id),
        source=source,
    )

    lots.append(lot)
    save_lots(lots)
    return lot


def consume_fifo_sell(
    *,
    symbol: str,
    sell_ts: str,
    sell_price: Decimal,
    quantity: Decimal,
    order_id: str,
) -> list[TaxSaleMatch]:
    """Consume lotes abiertos en orden FIFO y registra los tramos de venta."""
    order_id = str(order_id)
    if _sale_order_already_recorded(order_id, symbol):
        return []

    lots = load_lots()
    remaining = Decimal(str(quantity))
    matches: list[TaxSaleMatch] = []

    for lot in lots:
        if remaining <= 0:
            break

        if lot.symbol != symbol:
            continue

        qty_open = Decimal(lot.qty_open)
        if qty_open <= 0:
            continue

        qty_used = min(qty_open, remaining)
        buy_price = Decimal(lot.buy_price)

        cost_basis = buy_price * qty_used
        proceeds = sell_price * qty_used
        pnl = proceeds - cost_basis

        match = TaxSaleMatch(
            sell_ts=sell_ts,
            symbol=symbol,
            sell_price=_decimal_text(sell_price),
            qty=_decimal_text(qty_used),
            buy_lot_id=lot.lot_id,
            buy_ts=lot.buy_ts,
            buy_price=lot.buy_price,
            cost_basis=_decimal_text(cost_basis),
            proceeds=_decimal_text(proceeds),
            pnl=_decimal_text(pnl),
            source_order_id=order_id,
        )
        matches.append(match)

        lot.qty_open = _decimal_text(qty_open - qty_used)
        remaining -= qty_used

    if remaining > 0:
        matched_qty = Decimal(str(quantity)) - remaining

        # La venta real ya ha ocurrido. Si habia lotes disponibles, se consumen
        # y se registran sus tramos FIFO; la parte no cubierta queda como
        # incidencia para importar/reconstruir lotes iniciales después.
        save_lots(lots)
        append_sale_matches(matches)
        append_unmatched_sale(
            TaxUnmatchedSale(
                sell_ts=sell_ts,
                symbol=symbol,
                sell_price=_decimal_text(sell_price),
                quantity=_decimal_text(quantity),
                matched_qty=_decimal_text(matched_qty),
                missing_qty=_decimal_text(remaining),
                available_qty=_decimal_text(matched_qty),
                source_order_id=order_id,
                reason="missing_fifo_lots",
            )
        )
        raise RuntimeError(
            f"FIFO fiscal: venta de {quantity} {symbol} no cubierta. "
            f"Faltan {remaining} BTC en lotes fiscales. "
            f"Parte cubierta registrada y parte faltante registrada en {TAX_UNMATCHED_SALES_PATH}."
        )

    save_lots(lots)
    append_sale_matches(matches)
    return matches


def load_lots() -> list[TaxLot]:
    """Carga lotes fiscales desde disco y los devuelve en orden FIFO."""
    if not TAX_LOTS_PATH.exists():
        return []

    raw = json.loads(TAX_LOTS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"{TAX_LOTS_PATH} no contiene una lista de lotes")

    lots: list[TaxLot] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        payload = dict(item)
        payload.setdefault("source", "bot")
        lots.append(TaxLot(**payload))

    return _sort_lots_fifo(lots)


def save_lots(lots: list[TaxLot]) -> None:
    """Persiste lotes fiscales en disco ordenados por FIFO."""
    ordered = _sort_lots_fifo(lots)
    TAX_LOTS_PATH.write_text(
        json.dumps([asdict(lot) for lot in ordered], indent=2),
        encoding="utf-8",
    )


def append_sale_matches(matches: list[TaxSaleMatch]) -> None:
    """Anade tramos de venta FIFO al CSV de ventas realizadas."""
    if not matches:
        return

    file_exists = TAX_SALES_PATH.exists()

    with TAX_SALES_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sell_ts",
                "symbol",
                "sell_price",
                "qty",
                "buy_lot_id",
                "buy_ts",
                "buy_price",
                "cost_basis",
                "proceeds",
                "pnl",
                "source_order_id",
            ],
        )

        if not file_exists:
            writer.writeheader()

        for match in matches:
            writer.writerow(asdict(match))


def append_unmatched_sale(sale: TaxUnmatchedSale) -> None:
    """Registra una venta no cubierta por lotes fiscales suficientes."""
    if _unmatched_sale_already_recorded(sale.source_order_id, sale.symbol):
        return

    file_exists = TAX_UNMATCHED_SALES_PATH.exists()

    with TAX_UNMATCHED_SALES_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sell_ts",
                "symbol",
                "sell_price",
                "quantity",
                "matched_qty",
                "missing_qty",
                "available_qty",
                "source_order_id",
                "reason",
            ],
        )

        if not file_exists:
            writer.writeheader()
        writer.writerow(asdict(sale))


def import_manual_lot(
    *,
    buy_ts: str,
    buy_price: Decimal,
    quantity: Decimal,
    symbol: str = SYMBOL,
    note: str = "manual",
) -> TaxLot:
    """Importa un lote inicial/manual con un identificador determinista."""
    manual_id = (
        f"manual-{buy_ts}-"
        f"{_decimal_text(Decimal(str(buy_price)))}-"
        f"{_decimal_text(Decimal(str(quantity)))}-"
        f"{str(note).strip() or 'manual'}"
    )
    return add_buy_lot(
        symbol=symbol,
        buy_ts=buy_ts,
        buy_price=buy_price,
        quantity=quantity,
        order_id=manual_id,
        source="manual",
    )


def _decimal_from_text(value: object, default: Decimal = Decimal("0")) -> Decimal:
    """Convierte texto numerico externo a Decimal sin romper los reportes."""
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _sum_tax_sales_pnl(symbol: str = SYMBOL) -> tuple[Decimal, Decimal, Decimal, int]:
    """Devuelve PnL total, PnL de hoy, PnL del mes y numero de tramos de venta."""
    rows = _read_csv_rows(TAX_SALES_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    current_month = datetime.now().strftime("%Y-%m")

    total = Decimal("0")
    today_total = Decimal("0")
    month_total = Decimal("0")
    count = 0

    for row in rows:
        if row.get("symbol", symbol) != symbol:
            continue
        pnl = _decimal_from_text(row.get("pnl"))
        sell_ts = str(row.get("sell_ts", ""))
        total += pnl
        count += 1
        if sell_ts.startswith(today):
            today_total += pnl
        if sell_ts.startswith(current_month):
            month_total += pnl

    return total, today_total, month_total, count


def build_tax_status(symbol: str = SYMBOL) -> str:
    """Construye un resumen legible del ledger FIFO fiscal."""
    lots = load_lots()
    open_lots = [lot for lot in lots if lot.symbol == symbol and _decimal_from_text(lot.qty_open) > 0]

    qty_open = sum((_decimal_from_text(lot.qty_open) for lot in open_lots), Decimal("0"))
    cost_open = sum(
        (_decimal_from_text(lot.qty_open) * _decimal_from_text(lot.buy_price) for lot in open_lots),
        Decimal("0"),
    )
    avg_cost = cost_open / qty_open if qty_open > 0 else Decimal("0")

    pnl_total, pnl_today, pnl_month, sale_matches = _sum_tax_sales_pnl(symbol)
    unmatched_rows = [row for row in _read_csv_rows(TAX_UNMATCHED_SALES_PATH) if row.get("symbol", symbol) == symbol]

    return "\n".join([
        f"Symbol             : {symbol}",
        f"Lotes abiertos     : {len(open_lots)}",
        f"BTC abierto        : {_decimal_text(qty_open)}",
        f"Coste pendiente    : {_decimal_text(cost_open)} USDC",
        f"Coste medio FIFO   : {_decimal_text(avg_cost)} USDC/BTC",
        f"PnL realizado hoy  : {_decimal_text(pnl_today)} USDC",
        f"PnL realizado mes  : {_decimal_text(pnl_month)} USDC",
        f"PnL realizado total: {_decimal_text(pnl_total)} USDC",
        f"Tramos de venta    : {sale_matches}",
        f"Incidencias        : {len(unmatched_rows)}",
    ])


def build_tax_lots_text(limit: int = 20, symbol: str = SYMBOL) -> str:
    """Lista los lotes FIFO abiertos en orden de consumo."""
    safe_limit = max(1, int(limit))
    lots = [
        lot for lot in load_lots()
        if lot.symbol == symbol and _decimal_from_text(lot.qty_open) > 0
    ]

    if not lots:
        return "Sin lotes FIFO abiertos."

    lines: list[str] = []
    for lot in lots[:safe_limit]:
        qty_open = _decimal_from_text(lot.qty_open)
        buy_price = _decimal_from_text(lot.buy_price)
        cost_open = qty_open * buy_price
        lines.append(
            f"{lot.buy_ts} | {_decimal_text(qty_open)} BTC @ {_decimal_text(buy_price)} | "
            f"coste {_decimal_text(cost_open)} | {lot.source_order_id}"
        )

    remaining = len(lots) - safe_limit
    if remaining > 0:
        lines.append(f"... {remaining} lote(s) mas")

    return "\n".join(lines)


def build_tax_unmatched_text(limit: int = 20, symbol: str = SYMBOL) -> str:
    """Lista ventas no cubiertas por lotes FIFO."""
    safe_limit = max(1, int(limit))
    rows = [row for row in _read_csv_rows(TAX_UNMATCHED_SALES_PATH) if row.get("symbol", symbol) == symbol]

    if not rows:
        return "Sin incidencias FIFO."

    rows = rows[-safe_limit:]
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"{row.get('sell_ts', '')} | SELL {row.get('quantity', '?')} BTC @ "
            f"{row.get('sell_price', '?')} | missing {row.get('missing_qty', '?')} | "
            f"{row.get('source_order_id', '')}"
        )

    return "\n".join(lines)


def simulate_fifo_sell(
    *,
    price: Decimal,
    quantity: Decimal,
    symbol: str = SYMBOL,
) -> str:
    """Simula una venta FIFO sin modificar tax_lots.json ni tax_sales.csv."""
    sell_price = Decimal(str(price))
    remaining = Decimal(str(quantity))

    if sell_price <= 0:
        raise ValueError("precio debe ser mayor que cero")
    if remaining <= 0:
        raise ValueError("cantidad debe ser mayor que cero")

    lots = [
        lot for lot in load_lots()
        if lot.symbol == symbol and _decimal_from_text(lot.qty_open) > 0
    ]

    total_qty = Decimal("0")
    total_cost = Decimal("0")
    total_proceeds = Decimal("0")
    lines = [
        f"Symbol      : {symbol}",
        f"Precio venta: {_decimal_text(sell_price)}",
        f"Cantidad    : {_decimal_text(remaining)} BTC",
        "",
        "MATCHES FIFO",
    ]

    for lot in lots:
        if remaining <= 0:
            break

        qty_open = _decimal_from_text(lot.qty_open)
        qty_used = min(qty_open, remaining)
        buy_price = _decimal_from_text(lot.buy_price)
        cost_basis = qty_used * buy_price
        proceeds = qty_used * sell_price
        pnl = proceeds - cost_basis

        total_qty += qty_used
        total_cost += cost_basis
        total_proceeds += proceeds
        remaining -= qty_used

        lines.append(
            f"{lot.buy_ts} | {_decimal_text(qty_used)} BTC | "
            f"buy {_decimal_text(buy_price)} -> sell {_decimal_text(sell_price)} | "
            f"PnL {_decimal_text(pnl)}"
        )

    total_pnl = total_proceeds - total_cost
    lines.extend([
        "",
        "RESUMEN",
        f"Cantidad cubierta : {_decimal_text(total_qty)} BTC",
        f"Coste FIFO        : {_decimal_text(total_cost)} USDC",
        f"Venta bruta       : {_decimal_text(total_proceeds)} USDC",
        f"PnL estimado      : {_decimal_text(total_pnl)} USDC",
    ])

    if remaining > 0:
        lines.append(f"Cantidad sin cubrir: {_decimal_text(remaining)} BTC")

    return "\n".join(lines)
