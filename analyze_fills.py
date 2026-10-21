"""
analyze_fills.py — Empareja BUYs y SELLs del grid y genera un CSV de pares.

Añade métricas para detectar pares con step extendido/anómalo respecto al step base:
  - step_multiple : step_usdc / step_base
  - step_band     : 1x, 2x, 3x, >3x
  - gt_1x         : si step_usdc > 1 * step_base
  - gt_2x         : si step_usdc > 2 * step_base
  - gt_3x         : si step_usdc > 3 * step_base
"""

import csv
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Optional, Tuple


TICK_SIZE = Decimal("0.01")
MULTIPLE_DECIMALS = Decimal("0.0001")
QTY_EPSILON = Decimal("0.00000000000001")
STEP_DETECTION_WINDOWS_DAYS = (7, 14, 30)
GRID_STEP_TOLERANCE = Decimal("0.25")


def _price_key(price: Decimal) -> str:
    """Formatea un precio con el tick size usado en los CSV de salida."""
    return format(price.quantize(TICK_SIZE, rounding=ROUND_DOWN), "f")


def _multiple_key(value: Decimal) -> str:
    """Formatea multiplos de step con precision estable para analisis."""
    return format(value.quantize(MULTIPLE_DECIMALS, rounding=ROUND_DOWN), "f")


def _decimal_or_none(value: object) -> Optional[Decimal]:
    """Convierte valores externos a Decimal sin romper el analisis."""
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _quantity_key(quantity: Decimal) -> str:
    """Normaliza cantidades residuales para los CSV de salida."""
    normalized = quantity.normalize()
    return format(normalized, "f")


def load_fills(path: Path) -> List[Dict]:
    """Carga fills.csv y normaliza side, price y quantity para el analisis."""
    fills = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fills.append({
                "ts": row["timestamp"],
                "side": row["side"].strip().lower(),
                "price": Decimal(row["price"]),
                "quantity": Decimal(row["quantity"]),
            })
    return fills


def load_state_step(path: Path = Path("grid_state.json")) -> Optional[Decimal]:
    """Lee base_step/step del estado persistido si esta disponible."""
    if not path.exists():
        return None

    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return None

    return _decimal_or_none(raw.get("base_step")) or _decimal_or_none(raw.get("step"))


def _common_positive_diffs(prices: Iterable[Decimal]) -> List[Decimal]:
    """Devuelve saltos positivos entre precios unicos, redondeados a tick."""
    unique_prices = sorted(set(prices))
    if len(unique_prices) < 2:
        return []

    diffs: List[Decimal] = []
    for i in range(len(unique_prices) - 1):
        diff = (unique_prices[i + 1] - unique_prices[i]).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        if diff > 0:
            diffs.append(diff)
    return diffs


def _detect_step_from_fills(fills: List[Dict], require_repeated: bool = True) -> Decimal:
    """Detecta el salto dominante entre niveles de precio de un conjunto de fills."""
    diffs = _common_positive_diffs(f["price"] for f in fills)
    if not diffs:
        return Decimal("0")

    counts = Counter(diffs)
    most_common_diff, most_common_count = counts.most_common(1)[0]
    if most_common_count > 1:
        return most_common_diff

    if require_repeated:
        return Decimal("0")

    if len(diffs) <= 4:
        return Decimal(str(median(diffs))).quantize(TICK_SIZE, rounding=ROUND_DOWN)

    ordered = sorted(diffs)
    trim = max(1, len(ordered) // 10)
    trimmed = ordered[trim:-trim] or ordered
    return Decimal(str(median(trimmed))).quantize(TICK_SIZE, rounding=ROUND_DOWN)


def detect_step(fills: List[Dict]) -> Decimal:
    """
    Detecta el step base dando prioridad al regimen reciente del grid.

    El historico puede contener distintos steps usados en meses anteriores. Si se
    mezclan todos los precios, la moda historica puede dejar de representar el
    grid actual. Por eso se prueban primero ventanas recientes (7, 14 y 30 dias)
    respecto al ultimo fill. Solo si ninguna ofrece una moda repetida se usa todo
    el historico como fallback.
    """
    if not fills:
        return Decimal("0")

    parsed: List[Tuple[datetime, Dict]] = []
    for fill in fills:
        try:
            parsed.append((datetime.fromisoformat(str(fill["ts"])), fill))
        except (TypeError, ValueError):
            continue

    if parsed:
        latest_ts = max(ts for ts, _ in parsed)
        for window_days in STEP_DETECTION_WINDOWS_DAYS:
            cutoff = latest_ts - timedelta(days=window_days)
            recent_fills = [fill for ts, fill in parsed if ts >= cutoff]
            recent_step = _detect_step_from_fills(recent_fills, require_repeated=True)
            if recent_step > 0:
                return recent_step

    return _detect_step_from_fills(fills, require_repeated=False)


def detect_steps_by_day(fills: List[Dict], fallback_step: Decimal) -> Dict[str, Decimal]:
    """Detecta el step del grid por día y conserva el último si falta evidencia."""
    fills_by_day: Dict[str, List[Dict]] = {}
    for fill in fills:
        fills_by_day.setdefault(str(fill["ts"])[:10], []).append(fill)

    steps_by_day: Dict[str, Decimal] = {}
    last_step = fallback_step
    for day in sorted(fills_by_day):
        detected = _detect_step_from_fills(fills_by_day[day], require_repeated=True)
        if detected > 0:
            last_step = detected
        steps_by_day[day] = last_step
    return steps_by_day


def _find_best_buy_match(
    open_buys: List[Dict], sell_fill: Dict, expected_step: Decimal
) -> Optional[int]:
    """
    Busca el BUY abierto mas cercano por precio para emparejar un SELL de grid.

    La cantidad no interviene en la prioridad. Si las cantidades son distintas,
    pair_fills consume parcialmente el BUY o el SELL y continua con el siguiente
    nivel mas cercano. Esto evita saltarse BUYs proximos solo para encontrar una
    cantidad exactamente igual mucho mas abajo. Para evitar asociar inventario
    antiguo, el BUY debe estar dentro de la tolerancia del step de ese día.
    """
    sell_price = sell_fill["price"]
    candidates: List[Tuple[int, Dict]] = []

    for idx, buy in enumerate(open_buys):
        if buy["price"] >= sell_price:
            continue

        buy_qty = buy.get("remaining_quantity", buy["quantity"])
        if buy_qty <= QTY_EPSILON:
            continue

        realized_step = sell_price - buy["price"]
        tolerance = expected_step * GRID_STEP_TOLERANCE
        if expected_step <= 0 or abs(realized_step - expected_step) > tolerance:
            continue

        candidates.append((idx, buy))

    if not candidates:
        return None

    # Mayor precio BUY = menor distancia positiva al SELL. En empate, el mas
    # reciente primero para mantener un criterio determinista.
    candidates.sort(key=lambda item: (item[1]["price"], item[1]["ts"]), reverse=True)
    return candidates[0][0]


def _step_band(step_used: Decimal, step_base: Decimal) -> str:
    """Clasifica un step realizado respecto al step base detectado."""
    if step_base <= 0:
        return "n/a"
    if step_used <= step_base:
        return "1x"
    if step_used <= step_base * 2:
        return "2x"
    if step_used <= step_base * 3:
        return "3x"
    return ">3x"


def _step_flags(step_used: Decimal, step_base: Decimal) -> Dict[str, str]:
    """Calcula columnas derivadas para marcar steps extendidos o anomalos."""
    if step_base <= 0:
        return {
            "step_multiple": "0.0000",
            "step_band": "n/a",
            "gt_1x": "",
            "gt_2x": "",
            "gt_3x": "",
        }

    step_multiple = step_used / step_base
    return {
        "step_multiple": _multiple_key(step_multiple),
        "step_band": _step_band(step_used, step_base),
        "gt_1x": "yes" if step_used > step_base else "",
        "gt_2x": "yes" if step_used > step_base * 2 else "",
        "gt_3x": "yes" if step_used > step_base * 3 else "",
    }


def pair_fills(
    fills: List[Dict],
    step: Decimal,
    steps_by_day: Optional[Dict[str, Decimal]] = None,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Empareja fills BUY/SELL y devuelve pares de grid, BUYs abiertos y SELLs sin
    BUY compatible.

    Un SELL puede consumir varios BUYs cercanos cuando sus quantities no coinciden
    exactamente (por ejemplo, tras un resize). Esos consumos se agregan en un solo
    par usando un precio BUY medio ponderado. Asi el numero de pares sigue
    representando el numero de SELLs cerrados y no el numero de fragmentos usados
    internamente para reconstruir su coste. No fuerza asociaciones con BUYs de
    otro régimen de step: esas cantidades se informan como SELLs sin casar.
    """
    pairs: List[Dict] = []
    open_buys: List[Dict] = []
    open_sells: List[Dict] = []

    for fill in fills:
        fill = {**fill, "remaining_quantity": fill["quantity"]}
        side = fill["side"]

        if side == "buy":
            open_buys.append(fill)
            continue

        if side != "sell":
            continue

        sell_price = fill["price"]
        expected_step = (steps_by_day or {}).get(str(fill["ts"])[:10], step)
        matched_quantity = Decimal("0")
        total_buy_value = Decimal("0")
        total_sell_value = Decimal("0")
        first_buy_ts: Optional[str] = None

        while fill["remaining_quantity"] > QTY_EPSILON:
            matched_idx = _find_best_buy_match(open_buys, fill, expected_step)
            if matched_idx is None:
                break

            buy = open_buys[matched_idx]
            buy_qty = buy["remaining_quantity"]
            quantity = min(buy_qty, fill["remaining_quantity"])

            if first_buy_ts is None:
                first_buy_ts = str(buy["ts"])

            matched_quantity += quantity
            total_buy_value += buy["price"] * quantity
            total_sell_value += sell_price * quantity

            buy["remaining_quantity"] -= quantity
            fill["remaining_quantity"] -= quantity
            if buy["remaining_quantity"] <= QTY_EPSILON:
                open_buys.pop(matched_idx)

        if matched_quantity > QTY_EPSILON:
            weighted_buy_price = total_buy_value / matched_quantity
            profit = total_sell_value - total_buy_value
            step_used = sell_price - weighted_buy_price

            pairs.append({
                "buy_ts": first_buy_ts or "",
                "buy_price": _price_key(weighted_buy_price),
                "sell_ts": fill["ts"],
                "sell_price": _price_key(sell_price),
                "quantity": _quantity_key(matched_quantity),
                "step_usdc": _price_key(step_used),
                "profit_usdc": f"{profit:.4f}",
                "step_base": _price_key(expected_step),
                **_step_flags(step_used, expected_step),
            })

        if fill["remaining_quantity"] > QTY_EPSILON:
            open_sells.append({
                "ts": fill["ts"],
                "price": fill["price"],
                "quantity": fill["remaining_quantity"],
                "matched_quantity": matched_quantity,
                "expected_step": expected_step,
            })

    return pairs, open_buys, open_sells


def write_pairs(pairs: List[Dict], path: Path) -> None:
    """Escribe el CSV de pares cerrados con metricas de step y beneficio."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "day",
                "buy_ts",
                "buy_price",
                "sell_ts",
                "sell_price",
                "quantity",
                "step_usdc",
                "step_base",
                "step_multiple",
                "step_band",
                "gt_1x",
                "gt_2x",
                "gt_3x",
                "profit_usdc",
            ],
        )
        writer.writeheader()
        for p in pairs:
            writer.writerow({"day": p["sell_ts"][:10], **p})


def write_open_buys(open_buys: List[Dict], path: Path) -> None:
    """Escribe el CSV de BUYs que no pudieron emparejarse con un SELL."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ts", "price", "quantity"])
        writer.writeheader()
        for b in open_buys:
            quantity = b.get("remaining_quantity", b["quantity"])
            writer.writerow({
                "ts": b["ts"],
                "price": _price_key(b["price"]),
                "quantity": _quantity_key(quantity),
            })


def write_open_sells(open_sells: List[Dict], path: Path) -> None:
    """Escribe SELLs que no tienen un BUY compatible en el mismo régimen."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ts", "price", "quantity", "matched_quantity", "expected_step"],
        )
        writer.writeheader()
        for sell in open_sells:
            writer.writerow({
                "ts": sell["ts"],
                "price": _price_key(sell["price"]),
                "quantity": _quantity_key(sell["quantity"]),
                "matched_quantity": _quantity_key(sell["matched_quantity"]),
                "expected_step": _price_key(sell["expected_step"]),
            })


def print_summary(
    pairs: List[Dict], open_buys: List[Dict], open_sells: List[Dict], step: Decimal
) -> None:
    """Muestra un resumen agregado del emparejamiento de fills."""
    print(f"\n{'=' * 52}")
    print("  RESUMEN DE FILLS EMPAREJADOS")
    print(f"{'=' * 52}")
    print(f"  Step base actual   : {step} USDC")
    print("  Steps históricos   : detectados por día")

    if not pairs:
        print("  Pares de grid      : 0")
        print(f"  BUYs sin emparejar : {len(open_buys)}")
        print(f"  SELLs sin emparejar: {len(open_sells)}")
        print(f"{'=' * 52}\n")
        return

    total_profit = sum(Decimal(p["profit_usdc"]) for p in pairs)
    avg_profit = total_profit / len(pairs)
    realized_steps = [Decimal(p["step_usdc"]) for p in pairs]
    min_step = min(realized_steps)
    max_step = max(realized_steps)
    median_step = Decimal(str(median(realized_steps)))

    gt_1x_count = sum(1 for p in pairs if p["gt_1x"] == "yes")
    gt_2x_count = sum(1 for p in pairs if p["gt_2x"] == "yes")
    gt_3x_count = sum(1 for p in pairs if p["gt_3x"] == "yes")

    print(f"  Pares de grid      : {len(pairs)}")
    print(f"  BUYs sin emparejar : {len(open_buys)}")
    print(f"  SELLs sin emparejar: {len(open_sells)}")
    print(f"  Beneficio total    : {total_profit:.4f} USDC")
    print(f"  Beneficio medio    : {avg_profit:.4f} USDC / par")
    print(f"  Step realizado min : {min_step} USDC")
    print(f"  Step realizado max : {max_step} USDC")
    print(f"  Step realizado med : {median_step:.4f} USDC")
    print(f"  Pares > 1x step    : {gt_1x_count}")
    print(f"  Pares > 2x step    : {gt_2x_count}")
    print(f"  Pares > 3x step    : {gt_3x_count}")

    days: Dict[str, List[Decimal]] = {}
    for p in pairs:
        day = p["sell_ts"][:10]
        days.setdefault(day, []).append(Decimal(p["profit_usdc"]))

    current_month = datetime.now().strftime("%Y-%m")
    months: Dict[str, List[Decimal]] = {}
    month_day_profits: Dict[str, Dict[str, List[Decimal]]] = {}
    for day, profits in days.items():
        month = day[:7]
        months.setdefault(month, []).extend(profits)
        month_day_profits.setdefault(month, {})[day] = profits

    print(f"\n  {'Mes/Día':<12} {'Pares':>6} {'Beneficio':>12} {'Media':>10}")
    print(f"  {'-' * 42}")
    for month in sorted(months):
        if month == current_month:
            for day in sorted(month_day_profits[month]):
                profits = month_day_profits[month][day]
                day_total = sum(profits)
                day_avg = day_total / len(profits)
                print(f"  {day:<12} {len(profits):>6} {day_total:>11.4f}$ {day_avg:>9.4f}$")
        else:
            month_total = sum(months[month])
            month_avg = month_total / len(months[month])
            print(f"  {month:<12} {len(months[month]):>6} {month_total:>11.4f}$ {month_avg:>9.4f}$")

    print(f"{'=' * 52}\n")


def main() -> None:
    """Punto de entrada CLI para analizar fills.csv y generar reportes CSV."""
    args = sys.argv[1:]
    manual_step: Optional[Decimal] = None
    fills_arg: Optional[str] = None
    idx = 0
    while idx < len(args):
        arg = args[idx]
        if arg == "--step" and idx + 1 < len(args):
            manual_step = _decimal_or_none(args[idx + 1])
            idx += 2
            continue
        if arg.startswith("--step="):
            manual_step = _decimal_or_none(arg.split("=", 1)[1])
            idx += 1
            continue
        if fills_arg is None:
            fills_arg = arg
        idx += 1

    fills_path = Path(fills_arg) if fills_arg else Path("fills.csv")

    if not fills_path.exists():
        print(f"[!] No se encontró {fills_path}")
        sys.exit(1)

    fills = load_fills(fills_path)
    state_step = load_state_step(fills_path.parent / "grid_state.json")
    step = manual_step or state_step or detect_step(fills)

    if step == 0 and len(fills) < 2:
        print("[!] No hay suficientes fills para analizar.")
        sys.exit(1)

    steps_by_day = detect_steps_by_day(fills, step)
    pairs, open_buys, open_sells = pair_fills(fills, step, steps_by_day)

    pairs_path = fills_path.parent / "pairs_with_anomalies.csv"
    open_buys_path = fills_path.parent / "open_buys.csv"
    open_sells_path = fills_path.parent / "open_sells.csv"

    write_pairs(pairs, pairs_path)
    write_open_buys(open_buys, open_buys_path)
    write_open_sells(open_sells, open_sells_path)

    print_summary(pairs, open_buys, open_sells, step)
    print(f"  Pares guardados en    : {pairs_path}")
    print(f"  BUYs abiertos en      : {open_buys_path}")
    print(f"  SELLs sin emparejar en: {open_sells_path}")


if __name__ == "__main__":
    main()
