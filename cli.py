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
    get_all_trades_history_days_back,
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


def _export_trades_history_to_csv(days_back: int) -> Tuple[Optional[Path], int]:
    """Descarga el histórico de trades y lo exporta a CSV."""
    safe_days = max(1, int(days_back))
    response, logs = get_all_trades_history_days_back(safe_days)

    for l in logs:
        log_event(f"[LOG] {l['msg']}", l.get('level', 'info'))

    if not isinstance(response, dict):
        log_event('[ERROR] Respuesta inesperada al descargar histórico de trades.', 'error')
        return None, 0

    if response.get('error'):
        log_event(f"[ERROR] No se pudo descargar el histórico de trades: {json.dumps(response, ensure_ascii=False)}", 'error')
        return None, 0

    rows = response.get('data', [])
    if not isinstance(rows, list):
        log_event('[ERROR] El campo data del histórico de trades no es una lista.', 'error')
        return None, 0

    filename = Path(f"historical-orderdata-{safe_days}-{SYMBOL}.csv")

    preferred_fields = [
        'tdt', 'tdt_iso',
        'aid', 'anm',
        'p', 'pc', 'pn',
        'q', 'qc', 'qn',
        've', 'pdt', 'pdt_iso', 'vp', 'tid',
    ]
    extra_fields = sorted({
        key
        for row in rows if isinstance(row, dict)
        for key in row.keys()
        if key not in {'tdt_iso', 'pdt_iso'} and key not in preferred_fields
    })
    fieldnames = preferred_fields + extra_fields

    with filename.open('w', newline='', encoding='utf-8-sig') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            if not isinstance(row, dict):
                continue
            out = dict(row)
            out['tdt_iso'] = _epoch_ms_to_iso(row.get('tdt'))
            out['pdt_iso'] = _epoch_ms_to_iso(row.get('pdt'))
            writer.writerow(out)

    return filename, len([row for row in rows if isinstance(row, dict)])


def menu_exportar_datos():
    while True:
        print("\n=== Obtener y exportar datos ===")
        print("1. Histórico de trades globales")
        print("2. Histórico de candles")
        print("3. Atrás")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            exportar_trades_menu()
        elif opcion == "2":
            exportar_candles_menu()
        elif opcion == "3":
            break
        else:
            print("Opción inválida")


def exportar_trades_menu():
    print("\n=== Obtener y exportar histórico de trades globales a CSV ===")

    while True:
        raw_days = input(
            "¿De cuántos días atrás desde el momento actual quieres el histórico? [7]: "
        ).strip()

        if not raw_days:
            days_back = 7
            break

        try:
            days_back = int(raw_days)
            if days_back < 1:
                raise ValueError
            break
        except ValueError:
            print("Introduce un número entero mayor o igual que 1.")

    csv_path, row_count = _export_trades_history_to_csv(days_back)
    if csv_path is None:
        print("No se pudo generar el CSV. Revisa el log.")
        return

    print(f"CSV generado: {csv_path}")
    print(f"Trades exportados: {row_count}")
    log_event(f"Histórico de trades exportado a {csv_path} ({row_count} filas).", "info")


def exportar_candles_menu():
    print("\n=== Obtener y exportar histórico de candles ===")

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

    while True:
        dias = input("Días atrás desde hoy: ").strip()
        try:
            dias = int(dias)
            if dias < 1:
                raise ValueError
            break
        except ValueError:
            print("Introduce un número válido >= 1")

    now = int(time.time() * 1000)
    since = now - dias * 24 * 60 * 60 * 1000

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

    filename = Path(f"candles-{symbol}-{interval}-{dias}d.csv")

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
    levels = sorted(engine.levels, reverse=True)
    price  = engine.current_price
    orders = engine.active_orders

    closest = (
        min(engine.levels, key=lambda l: abs(l - price))
        if price and engine.levels else None
    )

    SEP = "  " + "-" * 62
    print(f"\n  {'Precio':>12}  {'Side':<5}  {'Order ID':<38}  {'':6}")
    print(SEP)

    for lvl in levels:
        key  = _price_key(lvl)
        info = orders.get(key)

        if info:
            side    = info["side"].upper()
            oid     = info["order_id"]
            if oid == "virtual":
                tag     = " [V]"
                oid_str = "virtual"
            elif oid == "pending_post_only":
                tag     = " [P]"
                oid_str = "latente (post_only)"
            else:
                tag     = "    "
                oid_str = oid[:36]
        else:
            side    = "---"
            oid_str = "vacío"
            tag     = "    "

        marker = " ◄" if closest is not None and lvl == closest else "  "
        print(f"  {key:>12}  {side:<5}  {oid_str:<38}{tag}{marker}")

    print(SEP)
    price_str = _price_key(price) if price else "N/A"
    print(f"  Precio actual: {price_str} USDC  |  Órdenes: {len(orders)}  |  Niveles: {len(levels)}")


def _show_active_orders(engine: "GridEngine") -> None:
    """Lista las órdenes activas ordenadas por precio, separadas por side."""
    orders = engine.active_orders

    if not orders:
        print("  No hay órdenes activas registradas en el engine.")
        return

    buys  = sorted([(k, v) for k, v in orders.items() if v["side"] == "buy"],
                   key=lambda x: Decimal(x[0]), reverse=True)
    sells = sorted([(k, v) for k, v in orders.items() if v["side"] == "sell"],
                   key=lambda x: Decimal(x[0]), reverse=True)

    SEP = "  " + "-" * 62
    print(f"\n  {'Side':<5}  {'Precio':>12}  {'Order ID'}")
    print(SEP)

    for key, info in sells:
        oid = info["order_id"]
        if oid == "virtual":
            tag = " [virtual]"
        elif oid == "pending_post_only":
            tag = " [latente]"
        else:
            tag = ""
        print(f"  {'SELL':<5}  {key:>12}  {oid}{tag}")

    print(f"  {'---':<5}  {'--- centro ---':>12}")

    for key, info in buys:
        oid = info["order_id"]
        if oid == "virtual":
            tag = " [virtual]"
        elif oid == "pending_post_only":
            tag = " [latente]"
        else:
            tag = ""
        print(f"  {'BUY':<5}  {key:>12}  {oid}{tag}")

    print(SEP)
    print(f"  Total: {len(sells)} SELL  |  {len(buys)} BUY")


def _show_fill_history(engine: "GridEngine", n: int = 20) -> None:
    """Muestra los últimos N fills registrados en esta sesión."""
    history = engine.fill_history

    if not history:
        print("  No hay fills registrados en esta sesión.")
        return

    recent = history[-n:][::-1]  # más recientes primero

    SEP = "  " + "-" * 62
    print(f"\n  {'Hora':<10}  {'Side':<5}  {'Precio':>12}  {'Order ID'}")
    print(SEP)

    for entry in recent:
        ts    = time.strftime("%H:%M:%S", time.localtime(entry["ts"]))
        side  = entry["side"].upper()
        price = entry["price"]
        oid   = entry["order_id"]
        print(f"  {ts:<10}  {side:<5}  {price:>12}  {oid}")

    print(SEP)
    print(f"  Total fills esta sesión: {len(history)}")


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

    if engine is not None and engine.active_orders:
        btc_en_grid = Decimal("0")
        usdc_en_grid = Decimal("0")

        for info in engine.active_orders.values():
            if info["order_id"] in ("virtual", "pending_post_only"):
                continue
            if info["side"] == "sell":
                btc_en_grid += engine.base_size
            elif info["side"] == "buy":
                usdc_en_grid += info["price"] * engine.base_size

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
    Verifica que el nivel no esté ya ocupado antes de enviar.
    """
    from api import place_order

    # Pedir precio
    while True:
        try:
            price_val = Decimal(input("  Precio: ").strip())
            break
        except Exception:
            print("  Precio inválido.")

    key = _price_key(price_val)

    # Comprobar que el nivel está vacío
    if key in engine.active_orders:
        existing = engine.active_orders[key]
        print(f"  [!] El nivel {key} ya tiene una orden {existing['side'].upper()} ({existing['order_id'][:8]}...). Abortando.")
        return

    # Pedir lado
    while True:
        side = input("  Lado (buy/sell): ").strip().lower()
        if side in ("buy", "sell"):
            break
        print("  Lado inválido. Debe ser buy o sell.")

    # Pedir tamaño
    while True:
        try:
            bs_input = input(f"  Tamaño [{fmt_amount(engine.base_size)}]: ").strip()
            base_size = Decimal(bs_input) if bs_input else engine.base_size
            break
        except Exception:
            print("  Tamaño inválido.")

    # Confirmar
    confirm = input(f"  Colocar {side.upper()} en {key} tamaño {fmt_amount(base_size)}? (s/n): ").strip().lower()
    if not confirm.startswith("s"):
        print("  Abortado.")
        return

    # Colocar orden
    order_id, logs = place_order(side, price_val, base_size)
    for l in logs:
        log_event(f"[MANUAL] {l['msg']}", l["level"])

    if not order_id:
        print("  [!] No se pudo colocar la orden. Revisa el log.")
        return

    # Registrar en active_orders del engine
    import time
    engine.active_orders[key] = {
        "side":      side,
        "order_id":  order_id,
        "price":     price_val,
        "placed_at": time.time(),
    }
    engine.save_state()
    log_event(f"[MANUAL] Orden {side.upper()} registrada en {key} -> {order_id}", "info")
    print(f"  ✓ Orden registrada en el engine.")




def _fill_empty_levels(engine: "GridEngine") -> None:
    """Ejecuta manualmente fill_empty_levels usando un precio fresco."""
    print("  Consultando precio actual...")
    current_price, _ = get_current_price()

    if current_price is None:
        current_price = engine.current_price

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
    Submenú interactivo mientras el engine corre en segundo plano.
    El engine escribe en su estado, este menú solo lee — seguro con el GIL.
    """
    while engine_thread.is_alive():
        price_str = _price_key(engine.current_price) if engine.current_price else "N/A"

        print("\n" + "=" * 40)
        print("  ENGINE EN MARCHA  ●")
        print("=" * 40)
        print(f"  Precio          : {price_str} USDC")
        print(f"  Órdenes activas : {len(engine.active_orders)}")
        print(f"  Fills sesión    : {len(engine.fill_history)}")
        print(f"  Último fill     : {engine.last_fill_side or 'ninguno'}")
        print("=" * 40)
        print("  1. Ver niveles del grid")
        print("  2. Ver órdenes activas")
        print("  3. Ver fills de esta sesión")
        print("  4. Ver balances")
        print("  5. Añadir orden manual")
        print("  6. Fill empty levels")
        print("  0. Detener engine")
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
            _show_fill_history(engine)

        elif opcion == "4":
            _show_balances_live(engine)

        elif opcion == "5":
            _add_manual_order(engine)

        elif opcion == "6":
            _fill_empty_levels(engine)

        elif opcion == "0":
            print("\n  Deteniendo engine...")
            engine.stop()
            engine_thread.join(timeout=10)

            if engine_thread.is_alive():
                print("  [!] El engine no respondió en 10s.")
            else:
                print("  Engine detenido.")

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
            break

        else:
            print("  Opción no válida.")

    # El hilo murió solo (error inesperado)
    if not engine_thread.is_alive() and engine.is_running():
        print("\n  [!] El engine se detuvo inesperadamente. Revisa el log.")


# =========================================================
# ========================= MENU ==========================
# =========================================================

def show_menu() -> str:
    print("=" * 40)
    print("MENÚ PRINCIPAL")
    print("=" * 40)
    print(f"1. Precio actual {SYMBOL}")
    print("2. Ver balances")
    print("3. Obtener y exportar datos")
    print("4. Orden manual")
    print("5. Ver órdenes activas")
    print("6. Ver orden por ID")
    print("7. Cancelar todas órdenes")
    print("8. Iniciar Grid Engine")
    print("c. Configuración manual")
    print("0. Salir")
    print("=" * 40)
    return input("Selecciona una opción (0-9, c): ")


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

    print("GRID ENGINE v1.0 — CLI INTERACTIVO")

    # Arrancar bot de Telegram solo si está habilitado en private_config.ini
    if telegram_enabled and start_telegram_bot is not None:
        start_telegram_bot()
    else:
        log_event("[TELEGRAM] Bot deshabilitado por configuración ([telegram] enabled = false).", "info")

    while True:
        opcion = show_menu()

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
        # 5. Órdenes activas
        # --------------------------------------------------
        elif opcion == "5":
            print("\n=== Órdenes activas ===")
            resp, logs = get_active_orders()
            for l in logs:
                log_event(f"[LOG] {l['msg']}", "info")
            log_event(f"{json.dumps(resp, indent=2, ensure_ascii=False)}", "info")

        # --------------------------------------------------
        # 6. Ver orden por ID
        # --------------------------------------------------
        elif opcion == "6":
            print("\n=== Ver orden por ID ===")
            order_id = input("Introduce el ID de la orden: ").strip()

            if not order_id:
                print("ID vacío. Operación abortada.")
                continue

            try:
                resp, logs = get_order_by_id(order_id)

                for l in logs:
                    log_event(f"[LOG] {l['msg']}", "info")

                if not resp:
                    print("No se recibió respuesta del servidor.")
                    log_event(f"[ERROR] Respuesta vacía al consultar la orden {order_id}.", "error")
                    continue

                if isinstance(resp, dict) and resp.get("error"):
                    print(f"No se pudo encontrar la orden {order_id}: {resp.get('error')}")
                    log_event(f"[ERROR] Consulta de orden {order_id}: {json.dumps(resp, indent=2, ensure_ascii=False)}", "error")
                    continue

                data = resp.get("data") if isinstance(resp, dict) else None
                if not data:
                    print(f"No existe una orden con ID {order_id}.")
                    log_event(f"[WARNING] La orden {order_id} no existe o no devolvió datos.", "warning")
                    if isinstance(resp, dict):
                        log_event(f"{json.dumps(resp, indent=2, ensure_ascii=False)}", "warning")
                    continue

                log_event(f"{json.dumps(resp, indent=2, ensure_ascii=False)}", "info")

            except Exception as exc:
                log_event(f"[ERROR] No se pudo consultar la orden {order_id}: {exc}", "error")
                print("No se pudo consultar la orden. Revisa el log.")

        # --------------------------------------------------
        # 7. Cancelar todas ordenes
        # --------------------------------------------------
        elif opcion == "7":
            print("\n=== Cancelar todas las órdenes ===")
            confirmacion = input("¿Está seguro? Esto cancelará TODAS las órdenes (s/n): ")
            if confirmacion.strip().lower().startswith("s"):
                cancel_all_orders()
            else:
                log_event("Cancelación abortada.", "info")

        # --------------------------------------------------
        # 8. Iniciar Grid Engine
        # --------------------------------------------------
        elif opcion == "8":
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

            def _sigterm_handler(signum, frame):
                log_event("[ENGINE] SIGTERM recibido — deteniendo engine...", "info")
                engine.stop()

            signal.signal(signal.SIGTERM, _sigterm_handler)
            
            # Lanzar el engine en un hilo secundario
            engine_thread = threading.Thread(
                target=engine.run,
                daemon=True,
                name="GridEngineThread"
            )
            engine_thread.start()
            log_event("[ENGINE] Corriendo en segundo plano.", "info")

            # Registrar en estado de Telegram para que /status y /grid funcionen
            if tg_state is not None:
                tg_state.engine = engine
                tg_state.engine_thread = engine_thread

            # Entrar al submenú de monitorización
            run_engine_menu(engine, engine_thread)

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
