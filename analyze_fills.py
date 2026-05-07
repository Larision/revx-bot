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
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Optional, Tuple


TICK_SIZE = Decimal("0.01")
MULTIPLE_DECIMALS = Decimal("0.0001")
QTY_EPSILON = Decimal("0.00000000000001")


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


def detect_step(fills: List[Dict]) -> Decimal:
    """
    Detecta un step base robusto desde fills historicos.

    Antes se usaba el menor salto positivo entre cualquier precio. Con trailing,
    cambios de size o resets del grid, ese minimo suele ser ruido y distorsiona
    todos los multiplos. Aqui usamos el salto mas frecuente; si no hay una moda
    clara, usamos una mediana recortada.
    """
    diffs = _common_positive_diffs(f["price"] for f in fills)
    if not diffs:
        return Decimal("0")

    counts = Counter(diffs)
    most_common_diff, most_common_count = counts.most_common(1)[0]
    if most_common_count > 1:
        return most_common_diff

    if len(diffs) <= 4:
        return median(diffs)

    ordered = sorted(diffs)
    trim = max(1, len(ordered) // 10)
    trimmed = ordered[trim:-trim] or ordered
    return Decimal(str(median(trimmed))).quantize(TICK_SIZE, rounding=ROUND_DOWN)


def _find_best_buy_match(open_buys: List[Dict], sell_fill: Dict) -> Optional[int]:
    """Busca el BUY abierto mas cercano para emparejar un SELL ejecutado."""
    sell_price = sell_fill["price"]
    remaining_sell_qty = sell_fill.get("remaining_quantity", sell_fill["quantity"])

    exact_qty_candidates: List[Tuple[int, Dict]] = []
    fallback_candidates: List[Tuple[int, Dict]] = []

    for idx, buy in enumerate(open_buys):
        if buy["price"] >= sell_price:
            continue

        buy_qty = buy.get("remaining_quantity", buy["quantity"])
        if buy_qty <= 0:
            continue

        if buy_qty == remaining_sell_qty:
            exact_qty_candidates.append((idx, buy))
        else:
            fallback_candidates.append((idx, buy))

    candidates = exact_qty_candidates or fallback_candidates
    if not candidates:
        return None

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


def pair_fills(fills: List[Dict], step: Decimal) -> Tuple[List[Dict], List[Dict]]:
    """Empareja fills BUY/SELL y devuelve pares cerrados mas BUYs abiertos."""
    pairs: List[Dict] = []
    open_buys: List[Dict] = []

    for fill in fills:
        fill = {**fill, "remaining_quantity": fill["quantity"]}
        side = fill["side"]

        if side == "buy":
            open_buys.append(fill)
            continue

        if side != "sell":
            continue

        while fill["remaining_quantity"] > QTY_EPSILON:
            matched_idx = _find_best_buy_match(open_buys, fill)
            if matched_idx is None:
                break

            buy = open_buys[matched_idx]
            buy_qty = buy["remaining_quantity"]
            quantity = min(buy_qty, fill["remaining_quantity"])
            sell_price = fill["price"]
            buy_price = buy["price"]
            buy_value = buy_price * quantity
            sell_value = sell_price * quantity
            profit = sell_value - buy_value
            step_used = sell_price - buy_price

            pairs.append({
                "buy_ts": buy["ts"],
                "buy_price": _price_key(buy_price),
                "sell_ts": fill["ts"],
                "sell_price": _price_key(sell_price),
                "quantity": _quantity_key(quantity),
                "step_usdc": _price_key(step_used),
                "profit_usdc": f"{profit:.4f}",
                **_step_flags(step_used, step),
            })

            buy["remaining_quantity"] -= quantity
            fill["remaining_quantity"] -= quantity
            if buy["remaining_quantity"] <= QTY_EPSILON:
                open_buys.pop(matched_idx)

    return pairs, open_buys


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


def print_summary(pairs: List[Dict], open_buys: List[Dict], step: Decimal) -> None:
    """Muestra un resumen agregado del emparejamiento de fills."""
    print(f"\n{'=' * 52}")
    print("  RESUMEN DE FILLS EMPAREJADOS")
    print(f"{'=' * 52}")
    print(f"  Step base detectado: {step} USDC")

    if not pairs:
        print("  Pares completos    : 0")
        print(f"  BUYs sin emparejar : {len(open_buys)}")
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

    print(f"  Pares completos    : {len(pairs)}")
    print(f"  BUYs sin emparejar : {len(open_buys)}")
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

    print(f"\n  {'Día':<12} {'Pares':>6} {'Beneficio':>12} {'Media':>10}")
    print(f"  {'-' * 42}")
    for day in sorted(days):
        profits = days[day]
        day_total = sum(profits)
        day_avg = day_total / len(profits)
        print(f"  {day:<12} {len(profits):>6} {day_total:>11.4f}$ {day_avg:>9.4f}$")

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

    pairs, open_buys = pair_fills(fills, step)

    pairs_path = fills_path.parent / "pairs_with_anomalies.csv"
    open_buys_path = fills_path.parent / "open_buys.csv"

    write_pairs(pairs, pairs_path)
    write_open_buys(open_buys, open_buys_path)

    print_summary(pairs, open_buys, step)
    print(f"  Pares guardados en    : {pairs_path}")
    print(f"  BUYs abiertos en      : {open_buys_path}")


if __name__ == "__main__":
    main()
