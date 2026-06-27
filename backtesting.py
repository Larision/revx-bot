from __future__ import annotations

import csv
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Optional, Sequence, cast

from api import _price_key, fmt_amount, get_historic_market_trades, get_candles
from cli import _epoch_ms_to_iso, input_with_esc
from config import SYMBOL, TICK_SIZE, WINDOW_MS
from engine import GridEngine
from logger import log_event
from types_ import LogEntry, OrderInfo


BACKTEST_OUTPUT_DIR = Path("backtesting")


class BacktestCancelled(Exception):
    """Cancelación controlada del backtest desde la CLI."""
    pass

@dataclass
class BacktestResult:
    lines: int
    lines_above: int
    lines_below: int
    market_trades: int
    fills: int
    buys: int
    sells: int
    realized_profit: Decimal
    grid_profit: Decimal
    start_equity: Decimal
    end_equity: Decimal
    end_usdc: Decimal
    end_btc: Decimal
    open_orders: int
    open_buys: int
    open_sells: int
    open_virtual_buys: int
    open_virtual_sells: int
    virtual_fills: int
    first_trade_time: str
    last_trade_time: str
    last_price: Decimal
    output_path: Path


class PaperGridEngine(GridEngine):
    """GridEngine real, con exchange y saldos sustituidos por una cartera simulada."""

    def __init__(
        self,
        *,
        saldo: Decimal,
        levels_above: int,
        levels_below: int,
        step_usdc: Decimal,
        base_size: Decimal,
        initial_price: Decimal,
    ) -> None:
        step_percent = Decimal(str(step_usdc)) / Decimal(str(initial_price))
        super().__init__(
            levels_below=levels_below,
            levels_above=levels_above,
            step_percent=step_percent,
            base_size=base_size,
            initial_price=initial_price,
        )
        self.paper_usdc: Decimal = Decimal(str(saldo))
        self.paper_btc: Decimal = Decimal("0")
        # PnL real FIFO de la cartera simulada. No usa precios virtuales.
        self.realized_profit: Decimal = Decimal("0")
        # Profit teorico de grid basado en parejas BUY/SELL. Es una metrica separada.
        self.grid_profit: Decimal = Decimal("0")
        self._paper_btc_lots: list[tuple[Decimal, Decimal]] = []
        self._paper_order_index: dict[str, str] = {}
        self._paper_order_snapshot: dict[str, OrderInfo] = {}
        self._paper_last_fill: Optional[OrderInfo] = None
        # Habilita trailing up y down extendidos durante el backtest
        self.trailing_up_mode   = 'extended'
        self.trailing_up_enabled = True
        self.trailing_down_mode = 'extended'
        self.trailing_down_enabled = True
        self._trailing_up_ext_steps = 0
        self._trailing_down_extended_drops = 0

    def initialize(self, recover_state: Optional[bool] = None) -> None:
        del recover_state

        center_price = Decimal(str(self.initial_price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        step = (center_price * self.step_percent).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        if step <= 0:
            raise RuntimeError("Step invalido para inicializar backtest.")

        levels = []
        for i in range(-self.levels_below, self.levels_above + 1):
            levels.append((center_price + (Decimal(i) * step)).quantize(TICK_SIZE, rounding=ROUND_DOWN))

        with self._state_lock:
            self.center_price = center_price
            self.current_price = center_price
            self.step = step
            self.base_step = step
            self.levels = sorted(set(levels))
            self.extended_levels = {}
            self.active_orders = {}

        # Compra inventario inicial para poder simular las SELL superiores del grid.
        initial_btc = self.base_size * Decimal(self.levels_above)
        initial_btc_cost = initial_btc * center_price
        if self.paper_usdc < initial_btc_cost:
            raise RuntimeError("Saldo insuficiente para comprar BTC inicial de las lineas SELL.")

        self.paper_usdc -= initial_btc_cost
        self.paper_btc += initial_btc
        self._add_paper_btc_lot(center_price, initial_btc)
        self.place_initial_orders()

    def save_state(self) -> bool:
        return True

    def clear_state(self) -> None:
        return

    def _get_available_usdc(self) -> Decimal:
        return self.paper_usdc

    def _get_available_btc(self) -> Decimal:
        return self.paper_btc

    def _add_paper_btc_lot(self, price: Decimal, size: Decimal) -> None:
        """Registra BTC comprado para calcular PnL real por FIFO."""
        lot_size = Decimal(str(size))
        if lot_size <= 0:
            return
        lot_price = Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        self._paper_btc_lots.append((lot_price, lot_size))

    def _consume_paper_btc_lots(self, sell_price: Decimal, size: Decimal) -> Decimal:
        """Consume BTC vendido por FIFO y devuelve PnL realizado real."""
        remaining = Decimal(str(size))
        price = Decimal(str(sell_price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        pnl = Decimal("0")

        while remaining > 0 and self._paper_btc_lots:
            lot_price, lot_size = self._paper_btc_lots[0]
            used = min(lot_size, remaining)
            pnl += (price - lot_price) * used
            remaining -= used
            lot_size -= used
            if lot_size <= 0:
                self._paper_btc_lots.pop(0)
            else:
                self._paper_btc_lots[0] = (lot_price, lot_size)

        if remaining > 0:
            log_event(
                f"[BACKTEST] FIFO sin BTC suficiente para valorar SELL "
                f"{fmt_amount(remaining)} en {_price_key(price)}; PnL parcial omitido",
                "warning",
            )

        return pnl

    def place_order(
        self,
        price: Decimal,
        side: str,
        size: Optional[Decimal] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        order_size = Decimal(str(size)) if size is not None else self.base_size
        key = _price_key(price)

        with self._state_lock:
            if key in self.active_orders:
                return

        if side == "buy":
            required = Decimal(str(price)) * order_size
            if self.paper_usdc < required:
                log_event(
                    f"[BACKTEST] BUY {_price_key(price)} omitido por USDC insuficiente "
                    f"({_price_key(self.paper_usdc)} < {_price_key(required)})",
                    "warning",
                )
                return
            self.paper_usdc -= required
        elif side == "sell":
            if self.paper_btc < order_size:
                log_event(
                    f"[BACKTEST] SELL {_price_key(price)} omitido por BTC insuficiente "
                    f"({fmt_amount(self.paper_btc)} < {fmt_amount(order_size)})",
                    "warning",
                )
                return
            self.paper_btc -= order_size
        else:
            return

        order_id = f"paper-{uuid.uuid4()}"
        order_info = cast(OrderInfo, {
            "side": side,
            "order_id": order_id,
            "price": Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_DOWN),
            "size": order_size,
            "placed_at": time.time(),
        })

        if side == "sell":
            metadata_has_paired = (
                isinstance(metadata, dict) and "paired_buy_price" in metadata
            )
            if not metadata_has_paired:
                if self._paper_last_fill is not None and self._paper_last_fill["side"] == "buy":
                    order_info["paired_buy_price"] = Decimal(str(self._paper_last_fill["price"]))
                elif self.center_price is not None:
                    order_info["paired_buy_price"] = Decimal(str(self.center_price))

        order_info = self._apply_order_metadata(order_info, metadata)

        with self._state_lock:
            self.active_orders[key] = order_info
            self._paper_order_index[order_id] = key
            self._paper_order_snapshot[order_id] = self._clone_order_info(order_info)

    def _place_order_safe(
        self,
        price: Decimal,
        side: str,
        size: Optional[Decimal] = None,
        metadata: Optional[dict[str, Any]] = None,
        *,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> bool:
        del max_retries, retry_delay
        key = _price_key(price)
        before = key in self.active_orders
        self.place_order(price, side, size, metadata=metadata)
        return (not before) and key in self.active_orders

    def cancel_order(self, order_id: str) -> tuple[dict[str, Any], list[LogEntry]]:
        key = self._paper_order_index.pop(order_id, None)

        info: Optional[OrderInfo] = None

        if key is not None:
            info = self.active_orders.get(key)

        if info is None:
            info = self._paper_order_snapshot.pop(order_id, None)
        else:
            self._paper_order_snapshot.pop(order_id, None)

        if info is None:
            return {"status_code": 204}, []

        order_size = self._order_size(info)

        if info["side"] == "buy":
            self.paper_usdc += Decimal(str(info["price"])) * order_size
        elif info["side"] == "sell":
            self.paper_btc += order_size

        return {"status_code": 204}, []

    def execute_fill(self, key: str) -> Optional[dict[str, str]]:
        with self._state_lock:
            info = self.active_orders.get(key)
            if info is None:
                return None
            snapshot = self._clone_order_info(info)
            del self.active_orders[key]

        order_id = str(snapshot.get("order_id", ""))
        if order_id != "virtual":
            self._paper_order_index.pop(order_id, None)
            self._paper_order_snapshot.pop(order_id, None)

        side = str(snapshot["side"])
        price = Decimal(str(snapshot["price"]))
        order_size = self._order_size(snapshot)

        if order_id != "virtual":
            if side == "buy":
                self.paper_btc += order_size
                self._add_paper_btc_lot(price, order_size)
            elif side == "sell":
                self.paper_usdc += price * order_size
                self.realized_profit += self._consume_paper_btc_lots(price, order_size)

                paired_buy_price = snapshot.get("paired_buy_price")
                if paired_buy_price is not None:
                    self.grid_profit += (price - Decimal(str(paired_buy_price))) * order_size
                elif self.center_price is not None:
                    self.grid_profit += (price - Decimal(str(self.center_price))) * order_size

            self.fill_history.append({
                "side": side,
                "price": key,
                "order_id": order_id,
                "ts": time.time(),
            })

        self._paper_last_fill = snapshot
        self.rebalance_after_fill(key, snapshot)

        return {
            "time": "",
            "side": side,
            "price": key,
            "size": fmt_amount(order_size),
            "realized_profit": fmt_amount(self.realized_profit),
            "grid_profit": fmt_amount(self.grid_profit),
            "virtual": "yes" if order_id == "virtual" else "no",
        }

    def final_balances(self) -> tuple[Decimal, Decimal]:
        """Balance final real por moneda, sin valorar BTC en USDC.

        Al terminar el backtest hay fondos libres y fondos reservados en
        ordenes abiertas. Para saber con que acaba realmente la simulacion,
        se suman ambos por moneda:
        - USDC libre + USDC bloqueado en BUY abiertas.
        - BTC libre + BTC bloqueado en SELL abiertas.

        Las ordenes virtuales no reservan saldo real, por eso no se incluyen.
        """
        reserved_usdc = sum(
            Decimal(str(info["price"])) * self._order_size(info)
            for info in self.active_orders.values()
            if info["side"] == "buy" and info["order_id"] != "virtual"
        )
        reserved_btc = sum(
            self._order_size(info)
            for info in self.active_orders.values()
            if info["side"] == "sell" and info["order_id"] != "virtual"
        )
        return self.paper_usdc + reserved_usdc, self.paper_btc + reserved_btc

    def equity(self, mark_price: Decimal) -> Decimal:
        final_usdc, final_btc = self.final_balances()
        return final_usdc + (final_btc * mark_price)


def _parse_date_to_ms(date_str: str, *, end_of_day: bool = False) -> int:
    dt = datetime.strptime(date_str, "%Y%m%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _decimal_from_item(item: dict[str, Any], *names: str) -> Decimal:
    for name in names:
        value = item.get(name)
        if value is not None:
            return Decimal(str(value))
    raise ValueError(f"Registro sin campos esperados: {', '.join(names)}")


def _item_time(item: dict[str, Any]) -> str:
    value = (
        item.get("tdt")
        or item.get("timestamp")
        or item.get("time")
        or item.get("created_date")
        or item.get("start")
        or ""
    )
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
    return str(value)


def _item_time_ms(item: dict[str, Any]) -> int:
    value = (
        item.get("tdt")
        or item.get("timestamp")
        or item.get("time")
        or item.get("created_date")
        or item.get("start")
        or 0
    )
    try:
        return int(value)
    except Exception:
        return 0


def _trade_price(trade: dict[str, Any]) -> Decimal:
    return _decimal_from_item(trade, "price", "p", "rate")


def _extract_candle_default_price(candles_resp: Any, *, since: Optional[int] = None, until: Optional[int] = None) -> Optional[Decimal]:
    """
    Extrae un precio por defecto a partir de una vela diaria.

    Usa la media entre open y close de la vela que caiga dentro del rango
    [since, until]. Si hay varias, prioriza la más cercana a since.
    Soporta respuestas en forma de lista o de dict con claves habituales.
    """
    rows: list[dict[str, Any]] = []

    if isinstance(candles_resp, dict):
        for key in ("data", "candles", "result"):
            value = candles_resp.get(key)
            if isinstance(value, list):
                rows = [row for row in value if isinstance(row, dict)]
                break
        if not rows:
            for value in candles_resp.values():
                if isinstance(value, list):
                    rows = [row for row in value if isinstance(row, dict)]
                    if rows:
                        break
    elif isinstance(candles_resp, list):
        rows = [row for row in candles_resp if isinstance(row, dict)]

    if not rows:
        return None

    if since is not None or until is not None:
        filtered: list[dict[str, Any]] = []
        for row in rows:
            start_value = row.get("start")
            if start_value is None:
                continue
            try:
                start_ms = int(start_value)
            except Exception:
                continue
            if since is not None and start_ms < since:
                continue
            if until is not None and start_ms > until:
                continue
            filtered.append(row)
        if filtered:
            rows = sorted(filtered, key=lambda row: int(row.get("start") or 0))

    candle = rows[0]
    try:
        open_price = _decimal_from_item(candle, "open", "o", "op", "open_price")
        close_price = _decimal_from_item(candle, "close", "c", "cl", "close_price")
    except Exception:
        return None

    return (open_price + close_price) / Decimal("2")


def _default_initial_price_from_date(date_str: str, fallback: Decimal) -> Decimal:
    """
    Calcula el precio inicial por defecto usando la vela diaria del día elegido.
    Si la API falla o no devuelve datos útiles, usa el fallback.
    """
    try:
        since = _parse_date_to_ms(date_str)
        until = _parse_date_to_ms(date_str, end_of_day=True)
        response, logs = get_candles(SYMBOL, 1440, since=since, until=until)
        for l in logs:
            log_event(f"[LOG] {l['msg']}", l.get("level", "info"))
        if isinstance(response, dict) and response.get("error"):
            return fallback
        price = _extract_candle_default_price(response, since=since, until=until)
        if price is None or price <= 0:
            return fallback
        return price.quantize(TICK_SIZE, rounding=ROUND_DOWN)
    except Exception:
        return fallback


def _quantize_price(price: Decimal) -> Decimal:
    return Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)


def _decimal_slug(value: Decimal) -> str:
    """Formato estable para diferenciar CSVs de backtest."""
    text = format(Decimal(str(value)).normalize(), "f")
    return text.replace("-", "m").replace(".", "p")


def _required_balance_for_grid(
    *,
    levels_above: int,
    levels_below: int,
    size: Decimal,
    step: Decimal,
    center: Decimal,
) -> Decimal:
    above = Decimal(levels_above)
    below = Decimal(levels_below)
    btc_for_sells = above * size * center
    usdc_for_buys = size * ((below * center) - (step * below * (below + Decimal("1")) / Decimal("2")))
    return btc_for_sells + usdc_for_buys


def _calculate_max_buy_only_lines(*, saldo: Decimal, size: Decimal, step: Decimal, center: Decimal) -> int:
    def required_for_lines(lines: int) -> Decimal:
        n = Decimal(lines)
        return size * ((n * center) - (step * n * (n + Decimal("1")) / Decimal("2")))

    max_by_price = int(((center - TICK_SIZE) / step).to_integral_value(rounding=ROUND_DOWN))
    if max_by_price <= 0:
        return 0

    low = 0
    high = max_by_price
    while low < high:
        mid = (low + high + 1) // 2
        if required_for_lines(mid) <= saldo:
            low = mid
        else:
            high = mid - 1

    return low


def _select_trade_fill_keys(
    engine: PaperGridEngine,
    previous_price: Decimal,
    trade_price: Decimal,
) -> list[str]:
    low = min(previous_price, trade_price)
    high = max(previous_price, trade_price)
    snapshot = engine.get_runtime_snapshot()["active_orders"]

    if trade_price > previous_price:
        return sorted(
            [
                key
                for key, info in snapshot.items()
                if info["side"] == "sell" and low < Decimal(key) <= high
            ],
            key=Decimal,
        )

    if trade_price < previous_price:
        # Exclusivo en el extremo superior (previous_price) por la misma razón.
        return sorted(
            [
                key
                for key, info in snapshot.items()
                if info["side"] == "buy" and low <= Decimal(key) < high
            ],
            key=Decimal,
            reverse=True,
        )

    return []


def _load_market_trades(symbol: str, since: int, until: int) -> list[dict[str, Any]]:
    symbol = SYMBOL
    all_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    window_start = since

    # Descargar por ventanas de WINDOW_MS días
    while window_start <= until:
        window_end = min(window_start + WINDOW_MS - 1, until)
        print(f"\n--- Descargando ventana ---")
        print(f"Desde: {datetime.fromtimestamp(window_start/1000)}")
        print(f"Hasta: {datetime.fromtimestamp(window_end/1000)}")

        cursor = None
        while True:
            response, logs = get_historic_market_trades(
                symbol=symbol,
                start_date=window_start,
                end_date=window_end,
                cursor=cursor
            )
            for l in logs:
                log_event(f"[LOG] {l['msg']}", l.get("level", "info"))

            if not isinstance(response, dict) or response.get("error"):
                print("Error obteniendo datos de mercado")
                raise RuntimeError("No se pudieron obtener trades de mercado para el backtest.")

            data = response.get("data", [])
            if isinstance(data, list):
                for row in data:
                    tid = row.get("tid")
                    if tid and tid in seen_ids:
                        continue
                    if tid:
                        seen_ids.add(tid)
                    row["tdt_iso"] = _epoch_ms_to_iso(row.get("tdt"))
                    all_rows.append(row)

            metadata = response.get("metadata", {})
            cursor = metadata.get("next_cursor") if isinstance(metadata, dict) else None
            if not cursor:
                break

        window_start = window_end + 1

    # Guardar CSV for DEBUG

    import csv
    from pathlib import Path

    # Ordenar antes de exportar
    sorted_rows = sorted(all_rows, key=_item_time_ms)

    csv_path = Path("market_trades_dump.csv")

    if sorted_rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(sorted_rows[0].keys()))
            writer.writeheader()
            writer.writerows(sorted_rows)


    print(f"Total trades descargados: {len(all_rows)}")

    #--------------------------------------

    return sorted(all_rows, key=_item_time_ms)


def _write_backtest_summary(
    *,
    saldos: Sequence[Decimal],
    initial_price: Decimal,
    size_line_config: dict[tuple[Decimal, Decimal], tuple[int, int]],
    start_date: str,
    end_date: str,
    results: list[tuple[Decimal, Decimal, Decimal, int, int, str, str, BacktestResult]],
) -> None:
    summary_file = BACKTEST_OUTPUT_DIR / "resumen_resultados.txt"
    lines: list[str] = []
    lines.append("=== BACKTEST RESUMEN ===")
    lines.append(f"Fecha UTC      : {datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()}")
    lines.append(f"Símbolo        : {SYMBOL}")
    lines.append(f"Rango          : {start_date} -> {end_date}")
    lines.append(f"Saldos USDC    : {', '.join(_price_key(saldo) for saldo in saldos)}")
    lines.append(f"Precio inicial : {_price_key(initial_price)}")
    lines.append(f"Combinaciones  : {len(results)}")
    lines.append("-")

    if len(size_line_config) == 1:
        (saldo, size), (above, below) = next(iter(size_line_config.items()))
        lines.append(
            f"Lineas         : saldo {_price_key(saldo)} | size {fmt_amount(size)} | "
            f"{above} arriba / {below} abajo"
        )
    else:
        lines.append("Lineas por saldo/size:")
        for (saldo, size), (above, below) in sorted(size_line_config.items(), key=lambda item: (item[0][0], item[0][1])):
            lines.append(
                f"  - saldo {_price_key(saldo)} | size {fmt_amount(size)}: "
                f"{above} arriba / {below} abajo"
            )

    lines.append(
        f"  {'Saldo':>10} {'Size':>12} {'Step':>10} {'Up':>5} {'Down':>5} "
        f"{'T.Up':>8} {'T.Down':>8} {'Fills':>8} {'Realized':>12} "
        f"{'GridProf':>12} {'PnL MTM':>12} {'PnL %':>10} "
        f"{'Final USDC':>12} {'Final BTC':>14} {'CSV'}"
    )

    best_abs: Optional[tuple[Decimal, Decimal, Decimal, int, int, str, str, BacktestResult, Decimal]] = None
    best_pct: Optional[tuple[Decimal, Decimal, Decimal, int, int, str, str, BacktestResult, Decimal]] = None

    for saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result in results:
        pnl = result.end_equity - result.start_equity
        pnl_pct = (
            pnl / result.start_equity * Decimal("100")
            if result.start_equity
            else Decimal("0")
        )
        if best_abs is None or pnl > best_abs[8]:
            best_abs = (saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result, pnl)
        if best_pct is None or pnl_pct > best_pct[8]:
            best_pct = (saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result, pnl_pct)

        lines.append(
            f"  {_price_key(saldo):>10} {fmt_amount(size):>12} {_price_key(step):>10} "
            f"{levels_above:>5} {levels_below:>5} "
            f"{trailing_up:>8} {trailing_down:>8} "
            f"{result.fills:>8} {_price_key(result.realized_profit):>12} "
            f"{_price_key(result.grid_profit):>12} {_price_key(pnl):>12} "
            f"{fmt_amount(pnl_pct):>10} "
            f"{_price_key(result.end_usdc):>12} {fmt_amount(result.end_btc):>14} "
            f"{result.output_path}"
        )

    if best_abs is not None:
        best_saldo, best_size, best_step, best_above, best_below, best_trailing_up, best_trailing_down, best_result, best_pnl = best_abs
        best_abs_pct = (
            best_pnl / best_result.start_equity * Decimal("100")
            if best_result.start_equity
            else Decimal("0")
        )
        lines.append("-")
        lines.append(
            f"Mejor PnL MTM: saldo {_price_key(best_saldo)}, size {fmt_amount(best_size)}, "
            f"step {_price_key(best_step)}, lineas {best_above}/{best_below}, "
            f"trailing {best_trailing_up}/{best_trailing_down}, "
            f"{_price_key(best_pnl)} USDC ({fmt_amount(best_abs_pct)}%) | "
            f"balance final {_price_key(best_result.end_usdc)} USDC + {fmt_amount(best_result.end_btc)} BTC | "
            f"realized FIFO {_price_key(best_result.realized_profit)} | grid {_price_key(best_result.grid_profit)}"
        )

    if best_pct is not None:
        best_saldo, best_size, best_step, best_above, best_below, best_trailing_up, best_trailing_down, best_result, best_pnl_pct = best_pct
        best_pnl = best_result.end_equity - best_result.start_equity
        lines.append(
            f"Mejor PnL %  : saldo {_price_key(best_saldo)}, size {fmt_amount(best_size)}, "
            f"step {_price_key(best_step)}, lineas {best_above}/{best_below}, "
            f"trailing {best_trailing_up}/{best_trailing_down}, "
            f"{fmt_amount(best_pnl_pct)}% ({_price_key(best_pnl)} USDC) | "
            f"balance final {_price_key(best_result.end_usdc)} USDC + {fmt_amount(best_result.end_btc)} BTC | "
            f"realized FIFO {_price_key(best_result.realized_profit)} | grid {_price_key(best_result.grid_profit)}"
        )

    lines.append("")

    summary_file.parent.mkdir(parents=True, exist_ok=True)
    with summary_file.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def prompt_backtest() -> None:
    try:
        print("\n=== Backtesting Grid ===")
        print("Usa la logica de GridEngine con exchange simulado. Fechas en formato YYYYMMDD.")
        print("Presiona ESC en cualquier momento para cancelar.")

        default_saldo = "1000"
        default_size = "0.0005"
        default_step = "200"
        default_price = "75000"
        default_trailing_up = "extended"
        default_trailing_down = "on"

        today = datetime.utcnow().date()
        start_default = (today - timedelta(days=29)).strftime("%Y%m%d")
        end_default = today.strftime("%Y%m%d")

        def ask_decimal(label: str, default: str) -> Decimal:
            while True:
                raw = input_with_esc(f"{label} [{default}]: ").strip()
                if not raw:
                    raw = default
                try:
                    return Decimal(raw)
                except Exception:
                    print("Valor invalido. Introduce un numero.")

        def ask_decimal_list(label: str, default: str) -> list[Decimal]:
            while True:
                raw = input_with_esc(f"{label} [{default}]: ").strip()
                if not raw:
                    raw = default

                values: list[Decimal] = []
                invalid = False
                for part in raw.split(","):
                    item = part.strip()
                    if not item:
                        invalid = True
                        break
                    try:
                        value = Decimal(item)
                    except Exception:
                        invalid = True
                        break
                    if value <= 0:
                        invalid = True
                        break
                    values.append(value)

                if values and not invalid:
                    return values

                print("Valor invalido. Introduce uno o varios numeros separados por comas.")

        def ask_date(label: str, default: str) -> str:
            raw = input_with_esc(f"{label} [{default}]: ").strip()
            return raw or default

        def _normalize_trailing_alias(value: str) -> str:
            normalized = value.strip().lower()
            if normalized in {"quote", "quote_fijo", "fixed-quote", "fixedquote"}:
                return "fixed_quote"
            return normalized

        def ask_trailing_mode(label: str, default: str, valid_modes: set[str]) -> str:
            options = "/".join(sorted(valid_modes))
            while True:
                raw = input_with_esc(f"{label} ({options}) [{default}]: ").strip().lower()
                if not raw:
                    return default
                normalized = _normalize_trailing_alias(raw)
                if normalized in valid_modes:
                    return normalized
                print(f"Opción inválida. Debe ser uno de: {options}.")

        def ask_trailing_modes(label: str, default: str, valid_modes: set[str]) -> list[str]:
            """Pregunta por uno o varios modos de trailing separados por comas."""
            options = "/".join(sorted(valid_modes))
            while True:
                raw = input_with_esc(f"{label} ({options}, separados por comas) [{default}]: ").strip().lower()
                if not raw:
                    raw = default

                values = [_normalize_trailing_alias(part) for part in raw.split(",") if part.strip()]
                if values and all(value in valid_modes for value in values):
                    return values

                print(f"Opcion invalida. Usa {options} o una lista separada por comas.")

        def ask_non_negative_int(label: str, default: int) -> int:
            while True:
                raw = input_with_esc(f"{label} [{default}]: ").strip()
                if not raw:
                    return default
                try:
                    value = int(raw)
                except Exception:
                    print("Valor invalido. Introduce un entero.")
                    continue
                if value < 0:
                    print("Valor invalido. Debe ser cero o mayor.")
                    continue
                return value

        start_date = ask_date("Fecha inicio (YYYYMMDD)", start_default)
        suggested_initial_price = _default_initial_price_from_date(start_date, Decimal(default_price))
        end_date = ask_date("Fecha final (YYYYMMDD)", end_default)

        saldos = ask_decimal_list("Saldo USDC. Admite varios saldos separados por comas.", default_saldo)
        # Permite ingresar listas de saldos, sizes y steps para ejecutar multiples combinaciones de backtest.
        sizes = ask_decimal_list("Size BTC por orden. Admite varios sizes separados por comas.", default_size)
        steps = ask_decimal_list("Step USDC entre lineas. Admite varios steps separados por comas.", default_step)

        initial_price = ask_decimal("Precio inicial", fmt_amount(suggested_initial_price))

        center = _quantize_price(initial_price)

        def max_lines_for_saldo_size(saldo: Decimal, size: Decimal) -> int:
            max_lines_by_step = [
                _calculate_max_buy_only_lines(
                    saldo=saldo,
                    size=size,
                    step=_quantize_price(step),
                    center=center,
                )
                for step in steps
            ]
            return min(max_lines_by_step) if max_lines_by_step else 0

        size_line_config: dict[tuple[Decimal, Decimal], tuple[int, int]] = {}

        print(
            f"\nBacktests base a configurar: {len(saldos) * len(sizes) * len(steps)} "
            f"({len(saldos)} saldo(s) x {len(sizes)} size(s) x {len(steps)} step(s))."
        )
        print("La distribucion arriba/abajo cambia el saldo necesario.")

        for saldo in saldos:
            print(f"\nSaldo: {_price_key(saldo)} USDC")
            for size in sizes:
                max_total_lines = max_lines_for_saldo_size(saldo, size)
                if max_total_lines <= 0:
                    print(
                        f"[!] El saldo {_price_key(saldo)} con size {fmt_amount(size)} "
                        "no permite abrir ninguna linea con esos steps y precio inicial. Se omite esta combinacion."
                    )
                    continue

                mitad_lines = max_total_lines // 2

                print(f"\nSaldo: {_price_key(saldo)} USDC | Size: {fmt_amount(size)}")
                print(f"Lineas maximas estimadas para este saldo/size: {mitad_lines} por lado (puede variar segun el caso).")

                while True:
                    levels_above = ask_non_negative_int("Cuantas lineas quieres poner arriba", mitad_lines)
                    levels_below = ask_non_negative_int("Cuantas lineas quieres poner abajo", mitad_lines)

                    if levels_above + levels_below <= 0:
                        print("[!] Debes colocar al menos una linea.")
                        continue

                    valid_for_this_config = True
                    for step in steps:
                        required_balance = _required_balance_for_grid(
                            levels_above=levels_above,
                            levels_below=levels_below,
                            size=size,
                            step=_quantize_price(step),
                            center=center,
                        )
                        if required_balance > saldo:
                            valid_for_this_config = False
                            print(
                                f"[!] Esa distribucion no cabe para saldo {_price_key(saldo)}, "
                                f"size {fmt_amount(size)} y step {_price_key(step)}. "
                                f"Necesitas {_price_key(required_balance)} USDC y tienes {_price_key(saldo)} USDC."
                            )
                            break

                    if not valid_for_this_config:
                        continue

                    size_line_config[(saldo, size)] = (levels_above, levels_below)
                    break

        if not size_line_config:
            raise ValueError("No hay ninguna configuracion valida para los saldos/sizes indicados.")

        trailing_ups = ask_trailing_modes(
            "Trailing up",
            default_trailing_up,
            {"off", "on", "extended", "fixed_quote"},
        )
        trailing_downs = ask_trailing_modes(
            "Trailing down",
            default_trailing_down,
            {"off", "on", "extended"},
        )

        combos = [(saldo, size, step) for saldo, size in size_line_config for step in steps]

        since = _parse_date_to_ms(start_date)
        until = _parse_date_to_ms(end_date, end_of_day=True)
        if since > until:
            raise ValueError("La fecha de inicio no puede ser mayor que la fecha final.")

        market_trades = _load_market_trades(SYMBOL, since, until)

        valid_combos: list[tuple[Decimal, Decimal, Decimal]] = []
        skipped_combos: list[tuple[Decimal, Decimal, Decimal, Decimal]] = []

        for saldo, size, step in combos:
            levels_above, levels_below = size_line_config[(saldo, size)]
            grid_step = _quantize_price(step)
            required_balance = _required_balance_for_grid(
                levels_above=levels_above,
                levels_below=levels_below,
                size=size,
                step=grid_step,
                center=center,
            )
            if required_balance > saldo:
                skipped_combos.append((saldo, size, step, required_balance))
            else:
                valid_combos.append((saldo, size, step))

        if not valid_combos:
            min_required = min(required for _, _, _, required in skipped_combos)
            print(
                f"[!] Ninguna combinacion cabe en los saldos indicados. "
                f"La mas barata requiere {_price_key(min_required)} USDC."
            )
            return

        if skipped_combos:
            print(f"[!] Se omitiran {len(skipped_combos)} combinacion(es) que no caben en su saldo.")

        run_combos = [
            (saldo, size, step, trailing_up, trailing_down)
            for saldo, size, step in valid_combos
            for trailing_up in trailing_ups
            for trailing_down in trailing_downs
        ]
        print(f"\nBacktests a ejecutar: {len(run_combos)}")

        results: list[tuple[Decimal, Decimal, Decimal, int, int, str, str, BacktestResult]] = []
        for saldo, size, step, trailing_up, trailing_down in run_combos:
            levels_above, levels_below = size_line_config[(saldo, size)]
            output_label = (
                f"saldo-{_decimal_slug(saldo)}"
                f"-size-{_decimal_slug(size)}-step-{_decimal_slug(step)}"
                f"-up-{levels_above}-down-{levels_below}"
                f"-tu-{trailing_up}-td-{trailing_down}"
            )
            result = run_grid_backtest(
                saldo=saldo,
                size=size,
                step=step,
                initial_price=initial_price,
                levels_above=levels_above,
                levels_below=levels_below,
                start_date=start_date,
                end_date=end_date,
                trailing_up_mode=trailing_up,
                trailing_down_mode=trailing_down,
                market_trades=market_trades,
                output_label=output_label,
            )
            results.append((saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result))

        if not results:
            print("\n[!] No se ejecuto ningun backtest.")
            return

        print("\n=== RESULTADOS BACKTEST ===")
        print(f"Precio inicial      : {_price_key(initial_price)} USDC")
        print(f"Saldos             : {', '.join(_price_key(saldo) for saldo in saldos)} USDC")
        print(f"Backtests ejecutados: {len(results)}")
        if len(size_line_config) == 1:
            (saldo, size), (above, below) = next(iter(size_line_config.items()))
            print(
                f"Lineas             : saldo {_price_key(saldo)} | size {fmt_amount(size)} | "
                f"{above} arriba / {below} abajo"
            )
        else:
            print("Lineas             : configuradas por saldo/size")
            for (saldo, size), (above, below) in sorted(size_line_config.items(), key=lambda item: (item[0][0], item[0][1])):
                print(f"  - saldo {_price_key(saldo)} | size {fmt_amount(size)}: {above} arriba / {below} abajo")

        print(
            f"\n  {'Saldo':>10} {'Size':>12} {'Step':>10} {'Up':>5} {'Down':>5} "
            f"{'T.Up':>8} {'T.Down':>8} {'Fills':>8} {'Realized':>12} "
            f"{'GridProf':>12} {'PnL MTM':>12} {'PnL %':>10} "
            f"{'Final USDC':>12} {'Final BTC':>14} {'CSV'}"
        )
        print(f"  {'-' * 175}")

        best_abs: Optional[tuple[Decimal, Decimal, Decimal, int, int, str, str, BacktestResult, Decimal]] = None
        best_pct: Optional[tuple[Decimal, Decimal, Decimal, int, int, str, str, BacktestResult, Decimal]] = None
        for saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result in results:
            pnl = result.end_equity - result.start_equity
            pnl_pct = (
                pnl / result.start_equity * Decimal("100")
                if result.start_equity
                else Decimal("0")
            )
            if best_abs is None or pnl > best_abs[8]:
                best_abs = (saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result, pnl)
            if best_pct is None or pnl_pct > best_pct[8]:
                best_pct = (saldo, size, step, levels_above, levels_below, trailing_up, trailing_down, result, pnl_pct)
            print(
                f"  {_price_key(saldo):>10} {fmt_amount(size):>12} {_price_key(step):>10} "
                f"{levels_above:>5} {levels_below:>5} "
                f"{trailing_up:>8} {trailing_down:>8} "
                f"{result.fills:>8} {_price_key(result.realized_profit):>12} "
                f"{_price_key(result.grid_profit):>12} {_price_key(pnl):>12} "
                f"{fmt_amount(pnl_pct):>10} "
                f"{_price_key(result.end_usdc):>12} {fmt_amount(result.end_btc):>14} "
                f"{result.output_path}"
            )

        if best_abs is not None:
            best_saldo, best_size, best_step, best_above, best_below, best_trailing_up, best_trailing_down, best_result, best_pnl = best_abs
            best_abs_pct = (
                best_pnl / best_result.start_equity * Decimal("100")
                if best_result.start_equity
                else Decimal("0")
            )
            print(
                f"\nMejor PnL MTM      : saldo {_price_key(best_saldo)}, size {fmt_amount(best_size)}, "
                f"step {_price_key(best_step)}, lineas {best_above}/{best_below} "
                f"trailing {best_trailing_up}/{best_trailing_down} "
                f"=> {_price_key(best_pnl)} USDC ({fmt_amount(best_abs_pct)}%) | "
                f"balance final {_price_key(best_result.end_usdc)} USDC + {fmt_amount(best_result.end_btc)} BTC | "
                f"realized FIFO {_price_key(best_result.realized_profit)} | grid {_price_key(best_result.grid_profit)}"
            )

        if best_pct is not None:
            best_saldo, best_size, best_step, best_above, best_below, best_trailing_up, best_trailing_down, best_result, best_pnl_pct = best_pct
            best_pnl = best_result.end_equity - best_result.start_equity
            print(
                f"Mejor PnL %        : saldo {_price_key(best_saldo)}, size {fmt_amount(best_size)}, "
                f"step {_price_key(best_step)}, lineas {best_above}/{best_below} "
                f"trailing {best_trailing_up}/{best_trailing_down} "
                f"=> {fmt_amount(best_pnl_pct)}% ({_price_key(best_pnl)} USDC) | "
                f"balance final {_price_key(best_result.end_usdc)} USDC + {fmt_amount(best_result.end_btc)} BTC | "
                f"realized FIFO {_price_key(best_result.realized_profit)} | grid {_price_key(best_result.grid_profit)}"
            )

        _write_backtest_summary(
            saldos=saldos,
            initial_price=initial_price,
            size_line_config=size_line_config,
            start_date=start_date,
            end_date=end_date,
            results=results,
        )

    except BacktestCancelled:
        print("\n[!] Backtest cancelado con ESC.")
        return

    except Exception as exc:
        print(f"\n[!] Backtest cancelado: {exc}")
        return

def run_grid_backtest(
    *,
    saldo: Decimal,
    size: Decimal,
    step: Decimal,
    initial_price: Decimal,
    levels_above: int,
    levels_below: int,
    start_date: str,
    end_date: str,
    symbol: str = SYMBOL,
    trailing_up_mode: str,
    trailing_down_mode: str,
    market_trades: Optional[Sequence[dict[str, Any]]] = None,
    output_label: Optional[str] = None,
) -> BacktestResult:
    if saldo <= 0:
        raise ValueError("El saldo debe ser mayor que cero.")
    if size <= 0:
        raise ValueError("El size debe ser mayor que cero.")
    if step <= 0:
        raise ValueError("El step debe ser mayor que cero.")
    if initial_price <= 0:
        raise ValueError("El precio inicial debe ser mayor que cero.")

    since = _parse_date_to_ms(start_date)
    until = _parse_date_to_ms(end_date, end_of_day=True)
    if since > until:
        raise ValueError("La fecha de inicio no puede ser mayor que la fecha final.")

    trades = list(market_trades) if market_trades is not None else _load_market_trades(symbol, since, until)
    if not trades:
        raise RuntimeError("No hay trades de mercado en el rango seleccionado.")

    if levels_above < 0 or levels_below < 0:
        raise ValueError("Las lineas arriba y abajo no pueden ser negativas.")
    if levels_above + levels_below <= 0:
        raise ValueError("Debes colocar al menos una linea entre arriba y abajo.")

    center = _quantize_price(initial_price)
    grid_step = _quantize_price(step)
    required_balance = _required_balance_for_grid(
        levels_above=levels_above,
        levels_below=levels_below,
        size=size,
        step=grid_step,
        center=center,
    )
    if required_balance > saldo:
        raise ValueError(
            "La distribución de lineas no cabe en el saldo disponible. "
            f"Necesitas {_price_key(required_balance)} USDC y tienes {_price_key(saldo)} USDC."
        )

    lines = levels_above + levels_below

    started_at = time.perf_counter()
    log_event(
        f"[BACKTEST] Inicio | saldo={_price_key(saldo)} size={fmt_amount(size)} step={_price_key(step)} "
        f"up={levels_above} down={levels_below} "
        f"trailing_up={trailing_up_mode} trailing_down={trailing_down_mode} "
        f"rango={start_date}->{end_date}",
        "info",
    )

    engine = PaperGridEngine(
        saldo=saldo,
        levels_above=levels_above,
        levels_below=levels_below,
        step_usdc=grid_step,
        base_size=size,
        initial_price=center,
    )
    engine.initialize(recover_state=False)
    engine.set_trailing(trailing_up_mode, trailing_down_mode)

    fills: list[dict[str, str]] = []
    last_price = center

    previous_price = center

    for trade in trades:
        trade_price = _trade_price(trade)
        last_price = trade_price

        with engine._state_lock:
            engine.current_price = trade_price

        trade_time = _item_time(trade)

        # Recalcula los cruces después de cada fill para que el trailing
        # pueda seguir generando nuevas órdenes dentro del mismo movimiento.
        while True:
            fill_keys = _select_trade_fill_keys(engine, previous_price, trade_price)
            if not fill_keys:
                break

            any_fill = False
            for fill_key in fill_keys:
                fill = engine.execute_fill(fill_key)
                if fill is None:
                    continue
                fill["time"] = trade_time
                fill["market_price"] = _price_key(trade_price)
                fills.append(fill)
                any_fill = True

            if not any_fill:
                break

        previous_price = trade_price

    suffix = f"-{output_label}" if output_label else ""
    output_path = BACKTEST_OUTPUT_DIR / f"backtest-{symbol}-{start_date}_to_{end_date}{suffix}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["time", "side", "price", "size", "realized_profit", "grid_profit", "virtual", "market_price"],
        )
        writer.writeheader()
        writer.writerows(fills)

    # Las virtuales son sentinels de trailing, no transacciones reales.
    real_fills = [f for f in fills if f["virtual"] == "no"]

    # Balance final real por moneda: libre + reservado.
    # No vende ni valora el BTC aqui; solo suma BTC y USDC con los que acaba la simulacion.
    end_usdc, end_btc = engine.final_balances()

    # Conversion posterior solo para PnL MTM en USDC.
    end_equity = end_usdc + (end_btc * last_price)

    runtime_snapshot = engine.get_runtime_snapshot()
    active_orders = runtime_snapshot["active_orders"]
    open_buys = sum(
        1 for info in active_orders.values()
        if info["side"] == "buy" and info["order_id"] != "virtual"
    )
    open_sells = sum(
        1 for info in active_orders.values()
        if info["side"] == "sell" and info["order_id"] != "virtual"
    )
    open_virtual_buys = sum(
        1 for info in active_orders.values()
        if info["side"] == "buy" and info["order_id"] == "virtual"
    )
    open_virtual_sells = sum(
        1 for info in active_orders.values()
        if info["side"] == "sell" and info["order_id"] == "virtual"
    )
    first_trade_time = _item_time(trades[0]) if trades else ""
    last_trade_time = _item_time(trades[-1]) if trades else ""
    virtual_fills = len(fills) - len(real_fills)
    pnl_mtm = end_equity - saldo

    elapsed = time.perf_counter() - started_at
    log_event(
        "\n".join([
            "[BACKTEST] Fin",
            (
                f"  Config      : saldo={_price_key(saldo)} | size={fmt_amount(size)} | step={_price_key(step)} | "
                f"up={levels_above} | down={levels_below} | "
                f"trailing_up={trailing_up_mode} | trailing_down={trailing_down_mode}"
            ),
            (
                f"  Trades      : market_trades={len(trades)} | "
                f"first={first_trade_time} | last={last_trade_time} | "
                f"last_price={_price_key(last_price)}"
            ),
            (
                f"  Fills       : real={len(real_fills)} | virtual={virtual_fills} | "
                f"buy={sum(1 for f in real_fills if f['side'] == 'buy')} | "
                f"sell={sum(1 for f in real_fills if f['side'] == 'sell')}"
            ),
            (
                f"  Resultados  : realized_fifo={_price_key(engine.realized_profit)} | "
                f"grid_profit={_price_key(engine.grid_profit)} | "
                f"pnl_mtm={_price_key(pnl_mtm)}"
            ),
            (
                f"  Balance     : final_usdc={_price_key(end_usdc)} | "
                f"final_btc={fmt_amount(end_btc)}"
            ),
            (
                f"  Abiertas    : total={len(active_orders)} | buy={open_buys} | sell={open_sells} | "
                f"virtual_buy={open_virtual_buys} | virtual_sell={open_virtual_sells}"
            ),
            f"  Archivo     : csv={output_path.name} | elapsed={elapsed:.2f}s",
        ]),
        "info",
    )
    return BacktestResult(
        lines=lines,
        lines_above=levels_above,
        lines_below=levels_below,
        market_trades=len(trades),
        fills=len(real_fills),
        buys=sum(1 for f in real_fills if f["side"] == "buy"),
        sells=sum(1 for f in real_fills if f["side"] == "sell"),
        realized_profit=engine.realized_profit,
        grid_profit=engine.grid_profit,
        start_equity=saldo,
        end_equity=end_equity,
        end_usdc=end_usdc,
        end_btc=end_btc,
        open_orders=len(active_orders),
        open_buys=open_buys,
        open_sells=open_sells,
        open_virtual_buys=open_virtual_buys,
        open_virtual_sells=open_virtual_sells,
        virtual_fills=virtual_fills,
        first_trade_time=first_trade_time,
        last_trade_time=last_trade_time,
        last_price=last_price,
        output_path=output_path,
    )

