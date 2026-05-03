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
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, List, Optional, Tuple


TICK_SIZE = Decimal("0.01")
MULTIPLE_DECIMALS = Decimal("0.0001")


def _price_key(price: Decimal) -> str:
    """Formatea un precio con el tick size usado en los CSV de salida."""
    return format(price.quantize(TICK_SIZE, rounding=ROUND_DOWN), "f")


def _multiple_key(value: Decimal) -> str:
    """Formatea multiplos de step con precision estable para analisis."""
    return format(value.quantize(MULTIPLE_DECIMALS, rounding=ROUND_DOWN), "f")


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


def detect_step(fills: List[Dict]) -> Decimal:
    """Detecta el menor salto positivo entre precios ejecutados."""
    prices = sorted(set(f["price"] for f in fills))
    if len(prices) < 2:
        return Decimal("0")

    diffs = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
    positive_diffs = [d for d in diffs if d > 0]
    if not positive_diffs:
        return Decimal("0")
    return min(positive_diffs)


def _find_best_buy_match(open_buys: List[Dict], sell_fill: Dict) -> Optional[int]:
    """Busca el BUY abierto mas cercano para emparejar un SELL ejecutado."""
    sell_price = sell_fill["price"]
    sell_qty = sell_fill["quantity"]

    exact_qty_candidates: List[Tuple[int, Dict]] = []
    fallback_candidates: List[Tuple[int, Dict]] = []

    for idx, buy in enumerate(open_buys):
        if buy["price"] >= sell_price:
            continue

        if buy["quantity"] == sell_qty:
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
        side = fill["side"]

        if side == "buy":
            open_buys.append(fill)
            continue

        if side != "sell":
            continue

        matched_idx = _find_best_buy_match(open_buys, fill)
        if matched_idx is None:
            continue

        buy = open_buys.pop(matched_idx)
        quantity = buy["quantity"]
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
            "quantity": str(quantity),
            "step_usdc": _price_key(step_used),
            "profit_usdc": f"{profit:.4f}",
            **_step_flags(step_used, step),
        })

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
            writer.writerow({
                "ts": b["ts"],
                "price": _price_key(b["price"]),
                "quantity": str(b["quantity"]),
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
    avg_step = sum(realized_steps) / len(realized_steps)

    gt_1x_count = sum(1 for p in pairs if p["gt_1x"] == "yes")
    gt_2x_count = sum(1 for p in pairs if p["gt_2x"] == "yes")
    gt_3x_count = sum(1 for p in pairs if p["gt_3x"] == "yes")

    print(f"  Pares completos    : {len(pairs)}")
    print(f"  BUYs sin emparejar : {len(open_buys)}")
    print(f"  Beneficio total    : {total_profit:.4f} USDC")
    print(f"  Beneficio medio    : {avg_profit:.4f} USDC / par")
    print(f"  Step realizado min : {min_step} USDC")
    print(f"  Step realizado max : {max_step} USDC")
    print(f"  Step realizado med : {avg_step:.4f} USDC")
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
    fills_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("fills.csv")

    if not fills_path.exists():
        print(f"[!] No se encontró {fills_path}")
        sys.exit(1)

    fills = load_fills(fills_path)
    step = detect_step(fills)

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
