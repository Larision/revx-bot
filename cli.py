from __future__ import annotations
import csv
import json
import signal
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    from engine import GridEngine

from config import (
    DEFAULT_BASE_SIZE,
    DEFAULT_GRID_LEVELS_ABOVE,
    DEFAULT_GRID_LEVELS_BELOW,
    DEFAULT_STEP_PERCENT,
    STATE_PATH,
    SYMBOL,
    TICK_SIZE,
)
from logger import log_event, log_file
from api import (
    _parse_balances,
    _price_key,
    cancel_all_orders,
    fmt_amount,
    get_active_orders,
    get_all_balances,
    get_current_price,
    get_order_by_id,
    get_ticker_price,
    place_order,
)


# =========================================================
# ====================== MANUAL ORDER =====================
# =========================================================

def manual_order() -> Tuple[Optional[str], Optional[Decimal], Optional[Decimal]]:

    while True:
        try:
            price_input = input("Precio: ")
            price_val = Decimal(price_input)
            break
        except Exception:
            print("Precio inválido.")

    while True:
        side = input("Lado (BUY/SELL): ").strip().lower()
        if side not in ("buy", "sell"):
            print("Lado inválido. Debe ser BUY o SELL.")
        else:
            break

    while True:
        bs_input = input(f"Tamaño base [{fmt_amount(DEFAULT_BASE_SIZE)}]: ").strip()
        try:
            bs_val = Decimal(bs_input) if bs_input else DEFAULT_BASE_SIZE
            break
        except Exception:
            print("Tamaño inválido.")

    confirmacion = input(
        f"Creando orden {side} a {_price_key(price_val)} "
        f"tamaño {fmt_amount(bs_val)}. Confirma (s/n): "
    )

    if confirmacion.strip().lower().startswith("s"):
        return side, price_val, bs_val

    return None, None, None



def _epoch_ms_to_iso(ms: object) -> str:
    """Convierte epoch ms a ISO UTC. Devuelve cadena vacía si no es válido."""
    if ms is None or isinstance(ms, bool):
        return ''
    if not isinstance(ms, (int, float, str)):
        return ''

    try:
        value = int(ms)
    except (TypeError, ValueError):
        return ''

    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def menu_exportar_datos():
    while True:
        print("\n=== Obtener y exportar datos ===")
        print("1. Histórico de mercado")
        print("2. Histórico de candles")
        print("3. Atrás")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            exportar_mercado_menu()
        elif opcion == "2":
            exportar_candles_menu()
        elif opcion == "3":
            break
        else:
            print("Opción inválida")


def exportar_mercado_menu():
    print("\n=== Obtener histórico de mercado (trades públicos) ===")
    print("=== Rango máximo por petición: 30 días (auto-splitting activado) ===")

    symbol = input(f"Symbol [{SYMBOL}]: ").strip() or SYMBOL

    from datetime import datetime, timezone

    def _parse_date_to_ms(date_str: str, end_of_day: bool = False) -> int:
        dt = datetime.strptime(date_str, "%Y%m%d")

        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    def _epoch_ms_to_iso(ms):
        try:
            return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            return ""

    # =========================
    # INPUT FECHAS
    # =========================

    while True:
        start_str = input("Fecha inicio (YYYYMMDD): ").strip()
        try:
            since = _parse_date_to_ms(start_str)
            break
        except ValueError:
            print("Formato inválido. Usa YYYYMMDD")

    while True:
        end_str = input("Fecha fin (YYYYMMDD) [hoy]: ").strip()

        if not end_str:
            until = int(time.time() * 1000)
            break

        try:
            until = _parse_date_to_ms(end_str, end_of_day=True)
            break
        except ValueError:
            print("Formato inválido. Usa YYYYMMDD")

    if since > until:
        print("La fecha de inicio no puede ser mayor que la fecha de fin.")
        return

    from api import get_market_trades_page

    WINDOW_MS = 30 * 24 * 60 * 60 * 1000

    all_rows = []
    seen_ids = set()

    window_start = since

    # =========================
    # LOOP POR VENTANAS
    # =========================

    while window_start <= until:
        window_end = min(window_start + WINDOW_MS - 1, until)

        print(f"\n--- Descargando ventana ---")
        print(f"Desde: {datetime.fromtimestamp(window_start/1000)}")
        print(f"Hasta: {datetime.fromtimestamp(window_end/1000)}")

        cursor = None

        while True:
            response, logs = get_market_trades_page(
                symbol=symbol,
                start_date=window_start,
                end_date=window_end,
                cursor=cursor
            )

            for l in logs:
                log_event(f"[LOG] {l['msg']}", l.get("level", "info"))

            if not isinstance(response, dict) or response.get("error"):
                print("Error obteniendo datos de mercado")
                return

            data = response.get("data", [])

            if isinstance(data, list):
                for row in data:
                    tid = row.get("tid")

                    # deduplicación REAL
                    if tid and tid in seen_ids:
                        continue

                    if tid:
                        seen_ids.add(tid)

                    row["tdt_iso"] = _epoch_ms_to_iso(row.get("tdt"))
                    all_rows.append(row)

            metadata = response.get("metadata", {})
            cursor = metadata.get("next_cursor")

            if not cursor:
                break

        window_start = window_end + 1

    # =========================
    # EXPORT CSV
    # =========================

    if not all_rows:
        print("No hay datos.")
        return

    end_label = end_str if end_str else "now"
    filename = Path(f"market-{symbol}-{start_str}_to_{end_label}.csv")

    fieldnames = sorted({k for row in all_rows for k in row.keys()})

    with filename.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in all_rows:
            writer.writerow(row)

    print("\n=== EXPORT COMPLETADO ===")
    print(f"CSV generado: {filename}")
    print(f"Trades únicos exportados: {len(all_rows)}")


def exportar_candles_menu():
    print("\n=== Obtener y exportar histórico de candles ===")
    print("\n=== Maximo intervalo permitido = 50000 candles (ej: 1 mes con 1 min) ===")

    symbol = input(f"Symbol [{SYMBOL}]: ").strip() or SYMBOL

    while True:
        interval = input(
            "Intervalo de candles? [1, 5, 15, 30, 60, 240, 1440, 2880, 5760, 10080, 20160, 40320]: "
        ).strip()
        try:
            interval = int(interval)
            break
        except ValueError:
            print("Intervalo inválido.")

    from datetime import datetime, timezone

    def _parse_date_to_ms(date_str: str, end_of_day: bool = False) -> int:
        """
        Convierte YYYYMMDD a timestamp ms en UTC.
        Si end_of_day=True → 23:59:59.999
        """
        dt = datetime.strptime(date_str, "%Y%m%d")

        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


    # Fecha inicio
    while True:
        start_str = input("Fecha inicio (YYYYMMDD): ").strip()
        try:
            since = _parse_date_to_ms(start_str)
            break
        except ValueError:
            print("Formato inválido. Usa YYYYMMDD (ej: 20260415)")

    # Fecha fin (default = ahora)
    while True:
        end_str = input("Fecha fin (YYYYMMDD) [hoy]: ").strip()

        if not end_str:
            now = int(time.time() * 1000)
            until = now
            break

        try:
            until = _parse_date_to_ms(end_str, end_of_day=True)
            break
        except ValueError:
            print("Formato inválido. Usa YYYYMMDD (ej: 20260415)")
    
    if since > until:
        print("La fecha de inicio no puede ser mayor que la fecha de fin.")
        return

    from api import get_candles

    response, logs = get_candles(
        symbol=symbol,
        interval=interval,
        since=since,
        until=now
    )

    for l in logs:
        log_event(f"[LOG] {l['msg']}", l.get("level", "info"))

    if not isinstance(response, dict) or response.get("error"):
        print("Error obteniendo candles")
        return

    data = response.get("data", [])

    if not data:
        print("No hay datos.")
        return

    end_label = end_str if end_str else "now"
    filename = Path(f"candles-{symbol}-{interval}m-{start_str}_to_{end_label}.csv")

    with filename.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["start", "open", "high", "low", "close", "volume"])

        for c in data:
            writer.writerow([
                c.get("start"),
                c.get("open"),
                c.get("high"),
                c.get("low"),
                c.get("close"),
                c.get("volume"),
            ])

    print(f"CSV generado: {filename}")
    print(f"Candles exportados: {len(data)}")



# =========================================================
# =================== GRID PREVIEW ========================
# =========================================================

def choose_initial_grid_price() -> Optional[Decimal]:
    """
    Consulta bid/ask/mid con get_ticker_price() y pide al usuario el precio
    inicial del grid. El valor por defecto es mid.
    """
    print("\n  Consultando bid/ask/mid...")

    try:
        ticker_resp, _ = get_ticker_price()
    except Exception as exc:
        log_event(f"[ERROR] No se pudo consultar get_ticker_price(): {exc}", "error")
        ticker_resp = None

    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
    mid: Optional[Decimal] = None

    if isinstance(ticker_resp, dict):
        data = ticker_resp.get("data", [])
        if isinstance(data, list) and data:
            item = data[0]
            try:
                bid = Decimal(str(item.get("bid"))) if item.get("bid") is not None else None
                ask = Decimal(str(item.get("ask"))) if item.get("ask") is not None else None
                mid = Decimal(str(item.get("mid"))) if item.get("mid") is not None else None
            except Exception:
                bid = ask = mid = None

    if bid is not None and ask is not None and mid is not None:
        print(f"  Bid: {_price_key(bid)} USDC")
        print(f"  Ask: {_price_key(ask)} USDC")
        print(f"  Mid: {_price_key(mid)} USDC (predeterminado)")
        default_price = mid
    else:
        print("  [WARN] No se pudo leer bid/ask/mid. Usando precio actual como fallback.")
        default_price, _ = get_current_price()
        if default_price is None:
            print("  [ERROR] No se pudo obtener ningún precio inicial.")
            return None
        print(f"  Precio actual: {_price_key(default_price)} USDC (predeterminado)")

    while True:
        try:
            raw = input(f"  Precio inicial del grid [{_price_key(default_price)}]: ").strip()
        except EOFError:
            raw = ""

        if not raw:
            return default_price.quantize(TICK_SIZE, rounding=ROUND_DOWN)

        try:
            return Decimal(raw).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        except Exception:
            print("  Precio inválido. Introduce un número o pulsa Enter para usar el predeterminado.")


def show_grid_preview(
    levels_below: int,
    levels_above: int,
    base_size: Decimal,
    step_percent: Decimal,
) -> Tuple[bool, Optional[Decimal]]:
    """
    Muestra la configuración del grid y los fondos necesarios antes de iniciar.
    Devuelve (confirmado, precio_inicial).
    """
    def _lp(msg: str = "", level: str = "info") -> None:
        print(msg)
        if msg.strip():
            log_file(msg.strip(), level)

    _lp("\n" + "=" * 50)
    _lp("  RESUMEN DE CONFIGURACIÓN — GRID ENGINE")
    _lp("=" * 50)

    _lp(f"\n  Símbolo          : {SYMBOL}")
    _lp(f"  Niveles abajo    : {levels_below}  (órdenes BUY)")
    _lp(f"  Niveles arriba   : {levels_above}  (órdenes SELL)")
    _lp(f"  Total órdenes    : {levels_below + levels_above}")
    _lp(f"  Tamaño base      : {fmt_amount(base_size)} BTC por orden")
    _lp(f"  Step percent     : {fmt_amount(step_percent * 100)}%  entre niveles")

    initial_price = choose_initial_grid_price()
    if initial_price is None:
        _lp("  [ERROR] No se pudo determinar el precio inicial.", "error")
        _lp("=" * 50)
        return False, None

    step_val = (initial_price * step_percent).quantize(TICK_SIZE, rounding=ROUND_DOWN)
    _lp(f"\n  Precio inicial   : {_price_key(initial_price)} USDC")
    _lp(f"  Step calculado   : {_price_key(step_val)} USDC entre niveles  ({fmt_amount(step_percent * 100)}%)")

    levels = []
    for i in range(-levels_below, levels_above + 1):
        lvl = (initial_price + (Decimal(i) * step_val)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        levels.append(lvl)
    levels = sorted(set(levels))

    buy_prices = [lvl for lvl in levels if lvl < initial_price]
    sell_prices = [lvl for lvl in levels if lvl > initial_price]

    required_usdc = sum((base_size * p for p in buy_prices), Decimal("0"))
    required_btc = base_size * Decimal(len(sell_prices))

    _lp(f"\n  {'Nivel':<8}  {'Precio':>12}  {'Lado'}")
    _lp(f"  {'-' * 8}  {'-' * 12}  {'-' * 14}")
    for lvl in reversed(levels):
        key = _price_key(lvl)
        if lvl > initial_price:
            lado = "SELL"
        elif lvl < initial_price:
            lado = "BUY"
        else:
            lado = "--- inicio"
        marker = " ◄ PRECIO INICIAL" if lvl == initial_price else ""
        _lp(f"  {'':8}  {key:>12}  {lado}{marker}")

    _lp(f"\n  Fondos necesarios:")
    _lp(f"    USDC requerido : {_price_key(required_usdc)} USDC  ({len(buy_prices)} órdenes BUY)")
    _lp(f"    BTC requerido  : {fmt_amount(required_btc)} BTC   ({len(sell_prices)} órdenes SELL)")

    _lp("\n  Consultando balances...")
    balances_resp, _ = get_all_balances()
    usdc_balance, btc_balance = _parse_balances(balances_resp)

    usdc_ok = usdc_balance >= required_usdc
    btc_ok = btc_balance >= required_btc

    _lp(f"    USDC disponible: {_price_key(usdc_balance)} USDC  {'✓' if usdc_ok else '✗ INSUFICIENTE'}")
    _lp(f"    BTC disponible : {fmt_amount(btc_balance)} BTC   {'✓' if btc_ok else '✗ INSUFICIENTE'}")
    _lp("\n" + "=" * 50)

    if not usdc_ok or not btc_ok:
        _lp("  [!] Fondos insuficientes para iniciar el grid.", "warning")
        _lp("      Deposita los fondos necesarios o ajusta la configuración.", "warning")
        _lp("=" * 50)
        return False, initial_price

    _lp("  [✓] Fondos suficientes para iniciar el grid.")
    _lp("=" * 50)

    try:
        respuesta = input("\n  ¿Deseas iniciar el Grid Engine? (s/n): ").strip().lower()
    except EOFError:
        respuesta = "n"

    return respuesta.startswith("s"), initial_price


# =========================================================
# ================= ENGINE MONITOR SUBMENU ================
# =========================================================

def _show_grid_levels(engine: "GridEngine") -> None:
    """Muestra los niveles del grid con el estado de cada orden."""
    snapshot = engine.get_runtime_snapshot()
    levels = sorted(snapshot["levels"], reverse=True)
    price = snapshot["current_price"]
    orders = snapshot["active_orders"]

    closest = (
        min(levels, key=lambda level: abs(level - price))
        if price is not None and levels else None
    )

    SEP = "  " + "-" * 62
    print(f"\n  {'Precio':>12}  {'Side':<5}  {'Order ID':<38}  {'':6}")
    print(SEP)

    for lvl in levels:
        key = _price_key(lvl)
        info = orders.get(key)

        if info:
            side = str(info["side"]).upper()
            oid = str(info["order_id"])
            if oid == "virtual":
                tag = " [V]"
                oid_str = "virtual"
            elif oid == "pending_post_only":
                tag = " [P]"
                oid_str = "latente (post_only)"
            elif oid == "pending_manual":
                tag = " [M]"
                oid_str = "reservada (manual)"
            else:
                tag = "    "
                oid_str = oid[:36]
        else:
            side = "---"
            oid_str = "vacío"
            tag = "    "

        marker = " ◄" if closest is not None and lvl == closest else "  "
        print(f"  {key:>12}  {side:<5}  {oid_str:<38}{tag}{marker}")

    print(SEP)
    price_str = _price_key(price) if price else "N/A"
    print(f"  Precio actual: {price_str} USDC  |  Órdenes: {len(orders)}  |  Niveles: {len(levels)}")


def _show_active_orders(engine: "GridEngine") -> None:
    """Lista las órdenes activas ordenadas por precio, separadas por side."""
    orders = engine.get_runtime_snapshot()["active_orders"]

    if not orders:
        print("  No hay órdenes activas registradas en el engine.")
        return

    buys = sorted([(k, v) for k, v in orders.items() if v["side"] == "buy"],
                  key=lambda item: Decimal(item[0]), reverse=True)
    sells = sorted([(k, v) for k, v in orders.items() if v["side"] == "sell"],
                   key=lambda item: Decimal(item[0]), reverse=True)

    SEP = "  " + "-" * 62
    print(f"\n  {'Side':<5}  {'Precio':>12}  {'Order ID'}")
    print(SEP)

    for key, info in sells:
        oid = str(info["order_id"])
        if oid == "virtual":
            tag = " [virtual]"
        elif oid == "pending_post_only":
            tag = " [latente]"
        elif oid == "pending_manual":
            tag = " [reservada]"
        else:
            tag = ""
        print(f"  {'SELL':<5}  {key:>12}  {oid}{tag}")

    print(f"  {'---':<5}  {'--- centro ---':>12}")

    for key, info in buys:
        oid = str(info["order_id"])
        if oid == "virtual":
            tag = " [virtual]"
        elif oid == "pending_post_only":
            tag = " [latente]"
        elif oid == "pending_manual":
            tag = " [reservada]"
        else:
            tag = ""
        print(f"  {'BUY':<5}  {key:>12}  {oid}{tag}")

    print(SEP)
    print(f"  Total: {len(sells)} SELL  |  {len(buys)} BUY")


def _trailing_menu(engine: "GridEngine") -> None:
    """
    Interfaz de línea de comandos para configurar trailing up y down en vivo.

    Muestra un menú con opciones para habilitar/deshabilitar trailing up y down,
    y permite aplicar cambios en caliente o descartarlos.

    No devuelve nada.
    """
    def _normalize_down_mode(value: object) -> str:
        if isinstance(value, bool):
            return "on" if value else "off"
        mode = str(value).strip().lower()
        if mode in {"off", "on", "extended", "extendido"}:
            return "extended" if mode == "extendido" else mode
        return "off"

    def _mode_label(mode: str) -> str:
        return {
            "off": "OFF",
            "on": "ON",
            "extended": "EXTENDIDO",
        }.get(mode, mode.upper())

    original_up = engine.trailing_up_enabled
    original_down = _normalize_down_mode(getattr(engine, "trailing_down_mode", engine.trailing_down_enabled))

    new_up = original_up
    new_down = original_down
    cycle = ["off", "on", "extended"]

    while True:
        print("\n=== CONFIGURAR TRAILINGS ===")
        print(f"1. Trailing up   > {'ON' if new_up else 'OFF'}")
        print(f"2. Trailing down > {_mode_label(new_down)}")
        print("3. Atrás")

        opcion = input("Opción: ").strip()

        if opcion == "1":
            new_up = not new_up

        elif opcion == "2":
            idx = cycle.index(new_down) if new_down in cycle else 0
            new_down = cycle[(idx + 1) % len(cycle)]

        elif opcion == "3":
            if new_up != original_up or new_down != original_down:
                confirm = input("¿Aplicar cambios? (s/n): ").strip().lower()
                if confirm.startswith("s"):
                    engine.set_trailing(new_up, new_down)
                    print("✓ Cambios aplicados en caliente")
                else:
                    print("Cambios descartados")
            return

        else:
            print("Opción inválida")


def format_balances_live(engine: Optional["GridEngine"] = None) -> str:
    """Devuelve un resumen legible de balances, incluyendo fondos comprometidos en la rejilla."""
    balances_resp, _ = get_all_balances()
    usdc, btc = _parse_balances(balances_resp)

    lines = [
        "──────────────────────────────────────",
        "SALDO DISPONIBLE",
        "──────────────────────────────────────",
        f"BTC  disponible : {fmt_amount(btc)}",
        f"USDC disponible : {_price_key(usdc)}",
    ]

    if engine is not None:
        snapshot = engine.get_runtime_snapshot()
        active_orders = snapshot["active_orders"]
        base_size = snapshot["base_size"]

        if active_orders:
            btc_en_grid = Decimal("0")
            usdc_en_grid = Decimal("0")

            for info in active_orders.values():
                order_id = str(info["order_id"])
                if order_id in {"virtual", "pending_post_only", "pending_manual"}:
                    continue
                order_size = Decimal(str(info.get("size", base_size)))
                if info["side"] == "sell":
                    btc_en_grid += order_size
                elif info["side"] == "buy":
                    usdc_en_grid += info["price"] * order_size

            lines.extend([
                "",
                "EN LA REJILLA",
                "──────────────────────────────────────",
                f"BTC  en órdenes : {fmt_amount(btc_en_grid)}",
                f"USDC en órdenes : {_price_key(usdc_en_grid)}",
                "",
                "TOTAL",
                "──────────────────────────────────────",
                f"BTC  total      : {fmt_amount(btc + btc_en_grid)}",
                f"USDC total      : {_price_key(usdc + usdc_en_grid)}",
            ])

    lines.append("──────────────────────────────────────")
    return "\n".join(lines)


def _show_balances_live(engine: Optional["GridEngine"] = None) -> None:
    """Consulta y muestra los balances en tiempo real, incluyendo lo comprometido en la rejilla."""
    print("  Consultando balances...")
    summary = format_balances_live(engine)
    print("\n" + "\n".join(f"  {line}" if line else "" for line in summary.splitlines()) + "\n")


def _add_manual_order(engine: "GridEngine") -> None:
    """
    Permite colocar una orden manual y registrarla en active_orders del engine.
    Reserva el nivel antes de enviar para evitar carreras con el hilo del engine.
    """
    while True:
        try:
            price_val = Decimal(input("  Precio: ").strip())
            break
        except Exception:
            print("  Precio inválido.")

    key = _price_key(price_val)

    while True:
        side = input("  Lado (buy/sell): ").strip().lower()
        if side in ("buy", "sell"):
            break
        print("  Lado inválido. Debe ser buy o sell.")

    default_size = engine.get_runtime_snapshot()["base_size"]
    while True:
        try:
            bs_input = input(f"  Tamaño [{fmt_amount(default_size)}]: ").strip()
            base_size = Decimal(bs_input) if bs_input else default_size
            break
        except Exception:
            print("  Tamaño inválido.")

    confirm = input(f"  Colocar {side.upper()} en {key} tamaño {fmt_amount(base_size)}? (s/n): ").strip().lower()
    if not confirm.startswith("s"):
        print("  Abortado.")
        return

    order_id, logs, error_msg = engine.place_manual_order(price_val, side, base_size)
    for entry in logs:
        log_event(f"[MANUAL] {entry['msg']}", entry["level"])

    if error_msg:
        print(f"  [!] {error_msg}")
        return

    if not order_id:
        print("  [!] No se pudo colocar la orden. Revisa el log.")
        return

    log_event(f"[MANUAL] Orden {side.upper()} registrada en {key} -> {order_id}", "info")
    print("  ✓ Orden registrada en el engine.")


def _fill_empty_levels(engine: "GridEngine") -> None:
    """Ejecuta manualmente fill_empty_levels usando un precio fresco."""
    print("  Consultando precio actual...")
    current_price, _ = get_current_price()

    if current_price is None:
        current_price = engine.get_runtime_snapshot()["current_price"]

    if current_price is None:
        print("  [!] No se pudo obtener el precio actual.")
        return

    print("\n" + "=" * 50)
    print("  FILL EMPTY LEVELS")
    print("=" * 50)
    print(f"  Precio actual: {_price_key(current_price)} USDC")

    try:
        confirm = input("\n  ¿Ejecutar fill empty levels? (s/n): ").strip().lower()
    except EOFError:
        confirm = "n"

    if not confirm.startswith("s"):
        print("  Abortado.")
        return

    engine.fill_empty_levels(current_price)
    engine.save_state()
    print("  ✓ Fill empty levels ejecutado.")


def run_engine_menu(engine: "GridEngine", engine_thread: threading.Thread) -> None:
    """
    Submenú interactivo del engine.
    Permite monitorizar sin bloquear el menú principal.
    """
    while engine_thread.is_alive():
        snapshot = engine.get_runtime_snapshot()
        current_price = snapshot["current_price"]
        price_str = _price_key(current_price) if current_price else "N/A"

        print("\n" + "=" * 40)
        print("  ENGINE EN MARCHA  ●")
        print("=" * 40)
        print(f"  Precio          : {price_str} USDC")
        print(f"  Órdenes activas : {len(snapshot['active_orders'])}")
        print(f"  Fills sesión    : {len(snapshot['fill_history'])}")
        print(f"  Último fill     : {snapshot['last_fill_side'] or 'ninguno'}")
        print("=" * 40)
        print("  1. Ver niveles del grid")
        print("  2. Ver órdenes activas")
        print("  3. Activar-Desactivar trailings")
        print("  4. Ver balances")
        print("  5. Añadir orden manual")
        print("  6. Fill empty levels")
        print("  v. Volver al menú principal")
        print("=" * 40)

        try:
            opcion = input("  Opción: ").strip()
        except (EOFError, KeyboardInterrupt):
            opcion = "0"

        if opcion == "1":
            _show_grid_levels(engine)

        elif opcion == "2":
            _show_active_orders(engine)

        elif opcion == "3":
            _trailing_menu(engine)

        elif opcion == "4":
            _show_balances_live(engine)

        elif opcion == "5":
            _add_manual_order(engine)

        elif opcion == "6":
            _fill_empty_levels(engine)

        elif opcion.lower() == "v":
            print("  Volviendo al menú principal...")
            break

    # El hilo murió solo (error inesperado)
    if not engine_thread.is_alive() and engine.is_running():
        print("\n  [!] El engine se detuvo inesperadamente. Revisa el log.")


# =========================================================
# ========================= MENU ==========================
# =========================================================

def show_menu(engine_running: bool = False) -> str:
    print("=" * 40)
    print("MENÚ PRINCIPAL")
    print("=" * 40)
    print(f"1. Precio actual {SYMBOL}")
    print("2. Ver balances")
    print("3. Obtener y exportar datos")
    print("4. Orden manual" + (" (bloqueada con engine)" if engine_running else ""))
    print("5. Cancelar todas órdenes" + (" (bloqueada con engine)" if engine_running else ""))
    print("6. Iniciar Grid Engine" if not engine_running else "8. Engine ya en marcha")
    print("7. Monitor del engine")
    print("8. Detener engine")
    print("c. Configuración manual")
    print("0. Salir")
    print("=" * 40)
    return input("Selecciona una opción: ").strip().lower()


# =========================================================
# ====================== MAIN LOOP ========================
# =========================================================

def run_cli() -> None:
    from engine import GridEngine
    from private_config import get_telegram_enabled

    telegram_enabled = get_telegram_enabled(default=True)
    start_telegram_bot = None
    tg_state = None

    if telegram_enabled:
        from telegram_bot import start_telegram_bot as _start_telegram_bot, _state as _tg_state
        start_telegram_bot = _start_telegram_bot
        tg_state = _tg_state

    grid_levels_below:    int     = DEFAULT_GRID_LEVELS_BELOW
    grid_levels_above:    int     = DEFAULT_GRID_LEVELS_ABOVE
    base_size_default:    Decimal = DEFAULT_BASE_SIZE
    step_percent_default: Decimal = DEFAULT_STEP_PERCENT

    print("GRID ENGINE v1.1 — CLI INTERACTIVO")

    # Arrancar bot de Telegram solo si está habilitado en private_config.ini
    if telegram_enabled and start_telegram_bot is not None:
        start_telegram_bot()
    else:
        log_event("[TELEGRAM] Bot deshabilitado por configuración ([telegram] enabled = false).", "info")
    
    engine: Optional[GridEngine] = None
    engine_thread: Optional[threading.Thread] = None

    while True:
        engine_running = engine_thread is not None and engine_thread.is_alive()
        opcion = show_menu(engine_running)

        # --------------------------------------------------
        # 1. Precio actual
        # --------------------------------------------------
        if opcion == "1":
            print(f"\n=== Precio actual {SYMBOL} ===")
            try:
                ticker_resp, logs = get_ticker_price()
                for l in logs:
                    log_event(f"[LOG] {l['msg']}")

                bid = ticker_resp.get("bid") if isinstance(ticker_resp, dict) else None
                ask = ticker_resp.get("ask") if isinstance(ticker_resp, dict) else None
                mid = ticker_resp.get("mid") if isinstance(ticker_resp, dict) else None

                if bid is not None and ask is not None and mid is not None:
                    log_event(
                        f"Bid: {_price_key(Decimal(str(bid)))} | Ask: {_price_key(Decimal(str(ask)))} | Mid: {_price_key(Decimal(str(mid)))}"
                    )
                else:
                    price, _ = get_current_price()
                    if price is not None:
                        log_event(f"Precio actual: {_price_key(price)}")
                    else:
                        log_event("No se pudo leer precio.", "error")
            except Exception as exc:
                log_event(f"No se pudo leer precio: {exc}", "error")

        # --------------------------------------------------
        # 2. Ver balances
        # --------------------------------------------------
        elif opcion == "2":
            print("\n=== Ver balances ===")
            balances, _ = get_all_balances()
            log_event(f"{json.dumps(balances, indent=2, ensure_ascii=False)}", "info")

        # --------------------------------------------------
        # 3. Obtener y exportar datos
        # --------------------------------------------------
        elif opcion == "3":
            menu_exportar_datos()

        # --------------------------------------------------
        # 4. Orden manual
        # --------------------------------------------------
        elif opcion == "4":
            if engine_running:
                print("\n[!] El engine está en marcha.")
                print("    Usa el monitor del engine (opción 9) para añadir órdenes manuales.")
                continue

            print("\n=== Orden manual ===")
            print(
                "ATENCION: RECUERDA cancelar todas ordenes manuales antes "
                "de activar el grid engine para evitar conflictos."
            )

            side, price_val, bs_val = manual_order()

            if side is None or price_val is None or bs_val is None:
                print("Orden manual abortada.")
                continue

            order_id, logs = place_order(side, price_val, bs_val)
            for l in logs:
                log_event(f"[LOG] {l['msg']}")

            if order_id:
                log_event(f"Orden {side} colocada en {_price_key(price_val)} -> {order_id}", "info")
            else:
                log_event("No se pudo colocar la orden manual.", "error")

        # --------------------------------------------------
        # 5. Cancelar todas ordenes
        # --------------------------------------------------
        elif opcion == "5":
            if engine_running:
                print("\n[!] El engine está en marcha.")
                print("    Detén el engine (opción 's') antes de cancelar todas las órdenes.")
                continue

            print("\n=== Cancelar todas las órdenes ===")
            confirmacion = input("¿Está seguro? Esto cancelará TODAS las órdenes (s/n): ")
            if confirmacion.strip().lower().startswith("s"):
                cancel_all_orders()
            else:
                log_event("Cancelación abortada.", "info")

        # --------------------------------------------------
        # 6. Iniciar Grid Engine
        # --------------------------------------------------
        elif opcion == "6":
            if engine_running:
                print("El engine ya está en marcha.")
                continue

            if base_size_default <= 0:
                print("Configura primero un base_size válido.")
                continue

            recover_state: Optional[bool] = None
            initial_price: Optional[Decimal] = None

            if STATE_PATH.exists():
                try:
                    raw = input(f"\nSe encontró estado previo en {STATE_PATH}. ¿Deseas recuperarlo? (s/n): ").strip().lower()
                except EOFError:
                    raw = "n"
                recover_state = raw.startswith("s")
            else:
                recover_state = False

            if not recover_state:
                confirmar, initial_price = show_grid_preview(
                    grid_levels_below,
                    grid_levels_above,
                    base_size_default,
                    step_percent_default,
                )
                if not confirmar:
                    print("\nGrid no iniciado.")
                    continue

            engine = GridEngine(
                levels_below=grid_levels_below,
                levels_above=grid_levels_above,
                step_percent=step_percent_default,
                base_size=base_size_default,
                initial_price=initial_price,
            )

            try:
                engine.initialize(recover_state=recover_state)
            except Exception as exc:
                log_event(f"[ERROR] No se pudo inicializar el motor: {exc}", "error")
                continue

            engine_thread = threading.Thread(
                target=engine.run,
                daemon=True,
                name="GridEngineThread"
            )
            engine_thread.start()

            if tg_state is not None:
                tg_state.engine = engine
                tg_state.engine_thread = engine_thread

            print("\n[ENGINE] Arrancado en segundo plano.")
            print("Puedes seguir usando el menú principal.")
            continue

        # --------------------------------------------------
        # 7. Monitor del engine
        # --------------------------------------------------
        elif opcion == "7":
            if not engine_running or engine is None or engine_thread is None:
                print("No hay engine en marcha.")
                continue

            run_engine_menu(engine, engine_thread)
        
        # --------------------------------------------------
        # 8. Detener engine
        # --------------------------------------------------
        elif opcion == "8":
            if not engine_running or engine is None or engine_thread is None:
                print("No hay engine en marcha.")
                continue

            print("\nDeteniendo engine...")
            engine.stop()
            engine_thread.join(timeout=10)

            if tg_state is not None:
                tg_state.engine = None
                tg_state.engine_thread = None

            if engine_thread.is_alive():
                print("El engine no respondió en 10s.")
            else:
                print("Engine detenido.")
            
            try:
                resp = input("  ¿Cancelar todas las órdenes? (s/n): ").strip().lower()
                if resp.startswith("s"):
                    log_event("[ENGINE] Cancelando todas las órdenes...", "info")
                    cancel_all_orders()
                    engine.clear_state()
                else:
                    log_event("[ENGINE] Órdenes conservadas — estado guardado para recuperación.", "info")
            except Exception as e:
                log_event(f"[ENGINE] Error al procesar respuesta: {e}", "error")

            engine = None
            engine_thread = None
        
        # --------------------------------------------------
        # Configuración
        # --------------------------------------------------
        elif opcion.lower() == "c":
            print("\n=== Configuración manual ===")

            new_levels_below = input(f"Niveles por debajo del precio inicial [{grid_levels_below}]: ")
            if new_levels_below.strip():
                try:
                    grid_levels_below = int(new_levels_below)
                except ValueError:
                    log_event("[ERROR] Valor de niveles abajo inválido, conservando el anterior.", "error")

            new_levels_above = input(f"Niveles por encima del precio inicial [{grid_levels_above}]: ")
            if new_levels_above.strip():
                try:
                    grid_levels_above = int(new_levels_above)
                except ValueError:
                    log_event("[ERROR] Valor de niveles arriba inválido, conservando el anterior.", "error")

            new_bs = input(f"Base size por defecto [{fmt_amount(base_size_default)}]: ")
            if new_bs.strip():
                try:
                    base_size_default = Decimal(new_bs)
                except Exception:
                    log_event("[ERROR] Valor de base size inválido, conservando el anterior.", "error")

            new_step_percent = input(f"Step percent por defecto [{fmt_amount(step_percent_default)}]: ")
            if new_step_percent.strip():
                try:
                    step_percent_default = Decimal(new_step_percent)
                except Exception:
                    log_event("[ERROR] Valor de step percent inválido, conservando el anterior.", "error")

        # --------------------------------------------------
        # Salir
        # --------------------------------------------------
        elif opcion == "0":
            print("¡Hasta luego!")
            break

        else:
            print("Opción no válida. Intenta de nuevo.")
