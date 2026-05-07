from __future__ import annotations

import csv
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Optional, cast

from api import _price_key, fmt_amount, get_market_trades_page
from config import SYMBOL, TICK_SIZE
from engine import GridEngine
from logger import log_event
from types_ import LogEntry, OrderInfo


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
    start_equity: Decimal
    end_equity: Decimal
    open_orders: int
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
        self.realized_profit: Decimal = Decimal("0")
        self._paper_order_index: dict[str, str] = {}
        self._paper_last_fill: Optional[OrderInfo] = None
        # Habilita trailing up y down extendidos durante el backtest
        self.trailing_up_mode   = 'extended'
        self.trailing_up_enabled = True
        self.trailing_down_mode = 'extended'
        self.trailing_down_enabled = True
        self._trailing_up_steps = 0
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
        self.place_initial_orders()

    def save_state(self) -> bool:
        return True

    def clear_state(self) -> None:
        return

    def _get_available_usdc(self) -> Decimal:
        return self.paper_usdc

    def _get_available_btc(self) -> Decimal:
        return self.paper_btc

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

        # FIX Bug 1: paired_buy_price se asigna siempre para SELLs, salvo que
        # el metadata ya lo traiga (órdenes extended que lo calculan explícitamente).
        # La condición original "metadata is None" excluía las SELLs de trailing-up
        # (metadata = {"trailing_up_step": N}), haciendo que se usase center_price
        # como coste base en lugar del BUY real, sobreestimando realized_profit.
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
        if key is None:
            return {"status_code": 204}, []

        info = self.active_orders.get(key)
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

        side = str(snapshot["side"])
        price = Decimal(str(snapshot["price"]))
        order_size = self._order_size(snapshot)

        if order_id != "virtual":
            if side == "buy":
                self.paper_btc += order_size
            elif side == "sell":
                self.paper_usdc += price * order_size
                paired_buy_price = snapshot.get("paired_buy_price")
                if paired_buy_price is not None:
                    self.realized_profit += (price - Decimal(str(paired_buy_price))) * order_size
                else:
                    self.realized_profit += (price - Decimal(str(self.center_price))) * order_size

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
            "virtual": "yes" if order_id == "virtual" else "no",
        }

    def equity(self, mark_price: Decimal) -> Decimal:
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
        return self.paper_usdc + reserved_usdc + ((self.paper_btc + reserved_btc) * mark_price)


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


def _quantize_price(price: Decimal) -> Decimal:
    return Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)


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
        # FIX Bug 4: el rango es exclusivo en el extremo inferior (previous_price)
        # para evitar doble-fill en niveles de frontera. Un SELL en previous_price
        # ya habría sido procesado en el ciclo anterior si existía.
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


MAX_WINDOW_MS = 30 * 24 * 60 * 60 * 1000


def _load_market_trades(symbol: str, since: int, until: int) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    window_start = since

    while window_start <= until:
        window_end = min(window_start + MAX_WINDOW_MS - 1, until)
        cursor: Optional[str] = None

        while True:
            response, logs = get_market_trades_page(
                symbol=symbol,
                start_date=window_start,
                end_date=window_end,
                cursor=cursor,
            )

            for entry in logs:
                log_event(f"[BACKTEST] {entry['msg']}", entry.get("level", "info"))

            if not isinstance(response, dict) or response.get("error"):
                raise RuntimeError("No se pudieron obtener trades de mercado para el backtest.")

            data = response.get("data", [])
            if isinstance(data, list):
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    trade_id = str(row.get("tid") or row.get("id") or "")
                    if trade_id and trade_id in seen_ids:
                        continue
                    if trade_id:
                        seen_ids.add(trade_id)
                    trades.append(row)

            metadata = response.get("metadata", {})
            cursor = metadata.get("next_cursor") if isinstance(metadata, dict) else None
            if not cursor:
                break

        window_start = window_end + 1

    return sorted(trades, key=_item_time_ms)


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

    market_trades = _load_market_trades(symbol, since, until)
    if not market_trades:
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

    # FIX Bug 3: inicializar previous_price con el precio del primer trade real,
    # no con center. Si se usa center y el mercado abre lejos (ej. center=75000,
    # primer trade=71000), todos los BUYs entre ambos se ejecutan de golpe en el
    # primer ciclo, produciendo un estado irreal. Usando el primer trade como punto
    # de partida, el grid solo reacciona a movimientos a partir de ese instante.
    previous_price = _trade_price(market_trades[0]) if market_trades else center
    for trade in market_trades:
        trade_price = _trade_price(trade)
        last_price = trade_price

        with engine._state_lock:
            engine.current_price = trade_price

        fill_keys = _select_trade_fill_keys(engine, previous_price, trade_price)
        previous_price = trade_price
        if not fill_keys:
            continue

        trade_time = _item_time(trade)
        for fill_key in fill_keys:
            fill = engine.execute_fill(fill_key)
            if fill is None:
                continue
            fill["time"] = trade_time
            fill["market_price"] = _price_key(trade_price)
            fills.append(fill)

    output_path = Path(f"backtest-{symbol}-{start_date}_to_{end_date}.csv")
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["time", "side", "price", "size", "realized_profit", "virtual", "market_price"],
        )
        writer.writeheader()
        writer.writerows(fills)

    # FIX Bug 2: excluir activaciones de órdenes virtuales del conteo de fills.
    # Las virtuales son sentinels de trailing, no transacciones reales.
    real_fills = [f for f in fills if f["virtual"] == "no"]
    return BacktestResult(
        lines=lines,
        lines_above=levels_above,
        lines_below=levels_below,
        market_trades=len(market_trades),
        fills=len(real_fills),
        buys=sum(1 for f in real_fills if f["side"] == "buy"),
        sells=sum(1 for f in real_fills if f["side"] == "sell"),
        realized_profit=engine.realized_profit,
        start_equity=saldo,
        end_equity=engine.equity(last_price),
        open_orders=len(engine.get_runtime_snapshot()["active_orders"]),
        last_price=last_price,
        output_path=output_path,
    )


def prompt_backtest() -> None:
    print("\n=== Backtesting Grid ===")
    print("Usa la logica de GridEngine con exchange simulado. Fechas en formato YYYYMMDD.")

    # Defaults
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
            raw = input(f"{label} [{default}]: ").strip()
            if not raw:
                raw = default
            try:
                return Decimal(raw)
            except Exception:
                print("Valor invalido. Introduce un numero.")

    def ask_date(label: str, default: str) -> str:
        raw = input(f"{label} [{default}]: ").strip()
        return raw or default

    def ask_trailing_mode(label: str, default: str) -> str:
        """Pregunta por el modo de trailing y valida la respuesta."""
        while True:
            raw = input(f"{label} (off/on/extended) [{default}]: ").strip().lower()
            if not raw:
                return default
            if raw in ("off", "on", "extended"):
                return raw
            print("Opción inválida. Debe ser off, on o extended.")

    def ask_non_negative_int(label: str, default: int) -> int:
        while True:
            raw = input(f"{label} [{default}]: ").strip()
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

    saldo = ask_decimal("Saldo USDC", default_saldo)
    size = ask_decimal("Size BTC por orden", default_size)
    step = ask_decimal("Step USDC entre lineas", default_step)
    initial_price = ask_decimal("Precio inicial", default_price)

    center = _quantize_price(initial_price)
    grid_step = _quantize_price(step)
    max_total_lines = _calculate_max_buy_only_lines(
        saldo=saldo,
        size=size,
        step=grid_step,
        center=center,
    )
    print(f"\nLineas maximas estimadas con este saldo: {max_total_lines} en total (si todas fuesen abajo).")
    print("La distribucion arriba/abajo cambia el saldo necesario.")

    if max_total_lines <= 0:
        print("[!] El saldo no permite abrir ninguna linea con ese size, step y precio inicial.")
        return

    while True:
        levels_above = ask_non_negative_int("Cuantas lineas quieres poner arriba", 0)
        levels_below = ask_non_negative_int("Cuantas lineas quieres poner abajo", max_total_lines)

        if levels_above + levels_below <= 0:
            print("[!] Debes colocar al menos una linea.")
            continue

        required_balance = _required_balance_for_grid(
            levels_above=levels_above,
            levels_below=levels_below,
            size=size,
            step=grid_step,
            center=center,
        )
        if required_balance > saldo:
            print(
                f"[!] Esa distribucion requiere {_price_key(required_balance)} USDC, "
                f"pero solo tienes {_price_key(saldo)} USDC."
            )
            continue
        break

    start_date = ask_date("Fecha inicio (YYYYMMDD)", start_default)
    end_date = ask_date("Fecha final (YYYYMMDD)", end_default)
    trailing_up = ask_trailing_mode("Trailing up", default_trailing_up)
    trailing_down = ask_trailing_mode("Trailing down", default_trailing_down)

    try:
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
        )
    except Exception as exc:
        print(f"\n[!] Backtest cancelado: {exc}")
        return

    pnl = result.end_equity - result.start_equity
    pnl_pct = (pnl / result.start_equity * Decimal("100")) if result.start_equity else Decimal("0")

    print("\n=== RESULTADO BACKTEST ===")
    print(f"Precio inicial     : {_price_key(initial_price)} USDC")
    print(f"Step               : {_price_key(step)} USDC")
    print(f"Trailings up/down  : {trailing_up} / {trailing_down}")
    print(f"Lineas totales     : {result.lines} ({result.lines_above} arriba / {result.lines_below} abajo)")
    print(f"Trades mercado     : {result.market_trades}")
    print(f"Fills simulados    : {result.fills} ({result.buys} BUY / {result.sells} SELL)")
    print(f"Profit realizado   : {_price_key(result.realized_profit)} USDC")
    print(f"Equity inicial     : {_price_key(result.start_equity)} USDC")
    print(f"Equity final       : {_price_key(result.end_equity)} USDC")
    print(f"PnL mark-to-market : {_price_key(pnl)} USDC ({fmt_amount(pnl_pct)}%)")
    print(f"Ordenes abiertas   : {result.open_orders}")
    print(f"Ultimo close       : {_price_key(result.last_price)} USDC")
    print(f"CSV fills          : {result.output_path}")
