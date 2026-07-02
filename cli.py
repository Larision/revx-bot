from __future__ import annotations
import csv
import json
import signal
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING, Any, Optional, Tuple, cast

if TYPE_CHECKING:
    from engine import GridEngine

from config import (
    DEFAULT_BASE_SIZE,
    DEFAULT_GRID_LEVELS_ABOVE,
    DEFAULT_GRID_LEVELS_BELOW,
    DEFAULT_STEP_PERCENT,
    DEFAULT_TRAILING_DOWN,
    DEFAULT_TRAILING_UP,
    STATE_PATH,
    SYMBOL,
    TICK_SIZE,
    VERSION,
    WINDOW_MS,
    MIN_USDC_RESERVE,
)
from logger import log_event, log_file
from trailing import (
    normalize_trailing_down_mode,
    normalize_trailing_up_mode,
    trailing_down_mode_label,
    trailing_up_mode_label,
)
from api import (
    _parse_balances,
    _price_key,
    cancel_all_orders,
    fmt_amount,
    get_all_balances,
    get_current_price,
    get_ticker,
    place_order,
)

class InputCancelled(Exception):
    """Cancelación controlada de inputs desde la CLI."""
    pass

def input_with_esc(prompt: str) -> str:
    """
    Función de input personalizada que permite cancelar con ESC.

    - En Windows usa msvcrt.
    - En Linux/macOS usa termios + tty + select.
    - Si no hay terminal interactiva, cae a input() normal.
    """
    import platform
    import select
    import sys

    if platform.system().lower() == "windows":
        import msvcrt

        print(prompt, end='', flush=True)
        buffer = ''
        while True:
            if msvcrt.kbhit():
                ch = msvcrt.getch()
                if ch == b'\x1b':  # ESC key
                    print()
                    raise InputCancelled("Entrada cancelada con ESC")
                if ch in (b'\r', b'\n'):  # Enter
                    print()
                    return buffer
                if ch == b'\x08':  # Backspace
                    if buffer:
                        buffer = buffer[:-1]
                        print('\b \b', end='', flush=True)
                    continue
                try:
                    char = ch.decode('utf-8')
                except UnicodeDecodeError:
                    continue  # ignore invalid chars
                buffer += char
                print(char, end='', flush=True)
            time.sleep(0.01)

        return buffer

    if not sys.stdin.isatty():
        try:
            return input(prompt)
        except EOFError as exc:
            raise InputCancelled("Entrada cancelada") from exc

    try:
        import termios
        import tty
    except ImportError:
        try:
            return input(prompt)
        except EOFError as exc:
            raise InputCancelled("Entrada cancelada") from exc

    # Tipado explícito para que Pylance no marque los miembros POSIX como inexistentes.
    termios_mod: Any = termios
    tty_mod: Any = tty
    select_mod: Any = select

    print(prompt, end='', flush=True)
    buffer = ''
    fd = sys.stdin.fileno()
    old_settings = termios_mod.tcgetattr(fd)

    try:
        tty_mod.setcbreak(fd)
        while True:
            readable, _, _ = select_mod.select([sys.stdin], [], [], 0.05)
            if not readable:
                continue

            ch = sys.stdin.read(1)
            if ch == '\x1b':
                print()
                raise InputCancelled("Entrada cancelada con ESC")
            if ch in ('\r', '\n'):
                print()
                return buffer
            if ch in ('\x08', '\x7f'):
                if buffer:
                    buffer = buffer[:-1]
                    print('\b \b', end='', flush=True)
                continue

            buffer += ch
            print(ch, end='', flush=True)
    finally:
        termios_mod.tcsetattr(fd, termios_mod.TCSADRAIN, old_settings)

# =========================================================
# ====================== MANUAL ORDER =====================
# =========================================================

def manual_order() -> Tuple[Optional[str], Optional[Decimal], Optional[Decimal]]:
    """
    Pregunta al usuario el precio, lado y tamaño base de forma interactiva.
    Confirma antes de devolver los valores.

    Retorna:
        Tupla (side, price, base_size) si se confirma, de lo contrario (None, None, None).
    """
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

# =========================================================
# ========================= HELPERS =======================
# =========================================================

def _epoch_ms_to_iso(ms: object) -> str:
    """
    Convierte una marca de tiempo epoch (milisegundos) a una cadena ISO 8601 UTC.

    Args:
        ms: int, float o str que representa epoch en milisegundos.

    Returns:
        Cadena con formato ISO, o cadena vacía si falla la conversión.
    """
    if ms is None or isinstance(ms, bool):
        return ''
    if not isinstance(ms, (int, float, str)):
        return ''

    try:
        value = int(ms)
    except (TypeError, ValueError):
        return ''

    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def _parse_date_to_ms(date_str: str, end_of_day: bool = False) -> int:
    """
    Convierte una cadena YYYYMMDD a milisegundos epoch en UTC.

    Args:
        date_str: Fecha en formato YYYYMMDD.
        end_of_day: Si es True, establece la hora a 23:59:59.999 UTC; de lo contrario, medianoche.

    Returns:
        Milisegundos enteros desde epoch.

    Raises:
        ValueError: Si el formato de date_str es inválido.
    """
    dt = datetime.strptime(date_str, "%Y%m%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def check_esc():
    if _esc_pressed():
        raise InputCancelled("Operación cancelada con ESC")

# =========================================================
# ====================== MENU EXPORTAR ====================
# =========================================================

def menu_exportar_datos():
    """
    Submenú para obtener y exportar datos históricos de mercado (trades o velas).
    """
    while True:
        print("\n=== Obtener y exportar datos ===")
        print("1. Histórico de mercado")
        print("2. Histórico de candles")
        print("3. Histórico de transacciones")
        print("4. Atrás")

        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            exportar_datos_mercado()
        elif opcion == "2":
            exportar_datos_candles()
        elif opcion == "3":
            exportar_datos_transacciones()
        elif opcion == "4":
            break
        else:
            print("Opción inválida")


def exportar_datos_mercado():
    """
    Submenú interactivo para descargar y exportar trades públicos de mercado (hasta 7 días por solicitud, con auto‑división).
    Los resultados se guardan como un archivo CSV.
    """
    print("\n=== Obtener histórico de mercado (trades públicos) ===")

    try:
        symbol = input_with_esc(f"Symbol [{SYMBOL}]: ").strip() or SYMBOL
        default_start_date = _parse_date_to_ms("20251014")

        # Fechas de inicio y fin
        while True:
            start_str = input_with_esc("Fecha inicio (YYYYMMDD) [20251014]: ").strip()
            if not start_str:
                since = default_start_date
                start_str = "20251014"
                break
            try:
                since = _parse_date_to_ms(start_str)
                break
            except ValueError:
                print("Formato inválido. Usa YYYYMMDD")

        while True:
            end_str = input_with_esc("Fecha fin (YYYYMMDD) [hoy]: ").strip()
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

        from api import get_historic_market_trades

        all_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        window_start = since

        # Descargar por ventanas de 7 días
        while window_start <= until:
            check_esc()   # Permite cancelar la operación con ESC

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
                        check_esc()   # Permite cancelar la operación con ESC

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

        if not all_rows:
            print("No hay datos.")
            return

        end_label = end_str if end_str else "now"
        filename = Path(f"market-{symbol}-{start_str}_to_{end_label}.csv")

        # Orden cronológico ascendente
        all_rows.sort(key=lambda r: int(r.get("tdt") or 0))

        fieldnames = sorted({k for row in all_rows for k in row.keys()})

        with filename.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for row in all_rows:
                writer.writerow(row)
                
        print("\n=== EXPORT COMPLETADO ===")
        print(f"CSV generado: {filename}")
        print(f"Trades únicos exportados: {len(all_rows)}")

    except InputCancelled:
        print("\nOperación cancelada.")
        return

def exportar_datos_candles():
    """
    Submenú interactivo para descargar y exportar datos históricos de velas.
    Los resultados se guardan como un archivo CSV.
    """
    print("\n=== Obtener y exportar histórico de candles ===")
    print("\n=== Maximo intervalo permitido = 50000 candles (ej: 1 mes con 1 min) ===")

    try:
        symbol = input_with_esc(f"Symbol [{SYMBOL}]: ").strip() or SYMBOL

        while True:
            interval = input_with_esc(
                "Intervalo de candles? [1, 5, 15, 30, 60, 240, 1440, 2880, 5760, 10080, 20160, 40320]: "
            ).strip()
            try:
                interval = int(interval)
                break
            except ValueError:
                print("Intervalo inválido.")

        while True:
            start_str = input_with_esc("Fecha inicio (YYYYMMDD): ").strip()
            try:
                since = _parse_date_to_ms(start_str)
                break
            except ValueError:
                print("Formato inválido. Usa YYYYMMDD (ej: 20260415)")

        while True:
            end_str = input_with_esc("Fecha fin (YYYYMMDD) [hoy]: ").strip()
            if not end_str:
                until = int(time.time() * 1000)
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
            until=until
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
            writer.writerow(["start", "datetime","open", "high", "low", "close", "volume"])
            for c in data:
                start = c.get("start")
                try:
                    dt = datetime.fromtimestamp(int(start)/1000).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    dt = ""

                writer.writerow([
                    start,
                    dt,
                    c.get("open"),
                    c.get("high"),
                    c.get("low"),
                    c.get("close"),
                    c.get("volume"),
                ])

        print(f"CSV generado: {filename}")
        print(f"Candles exportados: {len(data)}")

    except InputCancelled:
        print("\nOperación cancelada.")
        return

def exportar_datos_transacciones():
    """
    Submenú interactivo para descargar y exportar datos privados históricos de transacciones (fills).
    Los resultados se guardan como un archivo CSV.
    """
    print("\n=== Obtener histórico de transacciones personal(fills) ===")

    try:
        symbol = input_with_esc(f"Symbol [{SYMBOL}]: ").strip() or SYMBOL
        default_start_date = _parse_date_to_ms("20251014")

        # Fechas de inicio y fin
        while True:
            start_str = input_with_esc("Fecha inicio (YYYYMMDD) [20251014]: ").strip()
            if not start_str:
                since = default_start_date
                start_str = "20251014"
                break
            try:
                since = _parse_date_to_ms(start_str)
                break
            except ValueError:
                print("Formato inválido. Usa YYYYMMDD")

        while True:
            end_str = input_with_esc("Fecha fin (YYYYMMDD) [hoy]: ").strip()
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
        
        from api import get_private_trades

        all_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        window_start = since

        # Descargar por ventanas de 7 días
        while window_start <= until:
            check_esc()  # Permite cancelar la operación con ESC

            window_end = min(window_start + WINDOW_MS - 1, until)
            print(f"\n--- Descargando ventana ---")
            print(f"Desde: {datetime.fromtimestamp(window_start/1000)}")
            print(f"Hasta: {datetime.fromtimestamp(window_end/1000)}")

            cursor = None
            while True:
                response, logs = get_private_trades(
                    symbol=symbol,
                    start_date=window_start,
                    end_date=window_end,
                    cursor=cursor
                )
                for l in logs:
                    log_event(f"[LOG] {l['msg']}", l.get("level", "info"))

                if not isinstance(response, dict) or response.get("error"):
                    print("Error obteniendo datos privados de transacciones")
                    raise RuntimeError("No se pudieron obtener los datos privados de transacciones.")

                data = response.get("data", [])
                if isinstance(data, list):
                    for row in data:
                        check_esc()  # Permite cancelar la operación con ESC
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

        if not all_rows:
            print("No hay datos.")
            return

        end_label = end_str if end_str else "now"
        filename = Path(f"historial_de_transacciones_propias-{symbol}-{start_str}_to_{end_label}.csv")

        # Orden cronológico ascendente
        all_rows.sort(key=lambda r: int(r.get("tdt") or 0))

        fieldnames = sorted({k for row in all_rows for k in row.keys()})

        with filename.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for row in all_rows:
                writer.writerow(row)
                
        print("\n=== EXPORT COMPLETADO ===")
        print(f"CSV generado: {filename}")
        print(f"Transacciones únicas exportadas: {len(all_rows)}")

    except InputCancelled:
        print("\nOperación cancelada.")
        return

# =========================================================
# ===================== TAX FIFO MENU =====================
# =========================================================

def _normalize_tax_timestamp(raw: str) -> str:
    """Normaliza fechas para lotes manuales del ledger FIFO."""
    text = raw.strip()
    if not text:
        raise ValueError("fecha vacía")

    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d 00:00:00")

    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text} 00:00:00"

    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _ask_tax_limit(default: int = 20) -> int:
    """Pide un límite de filas para listados fiscales."""
    raw = input(f"  Límite de filas [{default}]: ").strip()
    if not raw:
        return default
    try:
        return max(1, min(500, int(raw)))
    except Exception:
        print("  Valor inválido. Usando límite predeterminado.")
        return default


def _show_tax_files() -> None:
    """Muestra los archivos fiscales conocidos y su estado."""
    from tax_fifo import TAX_LOTS_PATH, TAX_SALES_PATH, TAX_UNMATCHED_SALES_PATH

    print("\n=== ARCHIVOS FISCALES FIFO ===")
    for path in (TAX_LOTS_PATH, TAX_SALES_PATH, TAX_UNMATCHED_SALES_PATH):
        if path.exists():
            print(f"  ✓ {path}  ({path.stat().st_size} bytes)")
        else:
            print(f"  - {path}  (no existe todavía)")


def _simulate_tax_fifo_sale(engine: Optional["GridEngine"] = None) -> None:
    """Simula una venta FIFO sin modificar el ledger fiscal."""
    from tax_fifo import simulate_fifo_sell

    try:
        price = Decimal(input("  Precio de venta: ").strip())
    except Exception:
        print("  Precio inválido.")
        return

    default_qty: Optional[Decimal] = None
    if engine is not None:
        try:
            default_qty = Decimal(str(engine.get_runtime_snapshot()["base_size"]))
        except Exception:
            default_qty = None

    if default_qty is not None:
        qty_raw = input(f"  Cantidad BTC [{fmt_amount(default_qty)}]: ").strip()
        try:
            quantity = Decimal(qty_raw) if qty_raw else default_qty
        except Exception:
            print("  Cantidad inválida.")
            return
    else:
        try:
            quantity = Decimal(input("  Cantidad BTC: ").strip())
        except Exception:
            print("  Cantidad inválida.")
            return

    try:
        text = simulate_fifo_sell(price=price, quantity=quantity)
    except Exception as exc:
        print(f"  [!] Error simulando FIFO: {exc}")
        return

    print("\n=== SIMULACIÓN FIFO ===")
    print(text)


def _import_tax_manual_lot() -> None:
    """Importa un lote FIFO inicial/manual desde CLI."""
    from tax_fifo import import_manual_lot

    print("\n=== IMPORTAR LOTE FIFO MANUAL ===")
    print("Fecha admitida: YYYYMMDD, YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS")

    try:
        buy_ts = _normalize_tax_timestamp(input("  Fecha compra: "))
        buy_price = Decimal(input("  Precio compra: ").strip())
        quantity = Decimal(input("  Cantidad BTC: ").strip())
        note = input("  Nota [cli]: ").strip() or "cli"
        if buy_price <= 0 or quantity <= 0:
            raise ValueError("precio y cantidad deben ser mayores que cero")
    except Exception as exc:
        print(f"  [!] Datos inválidos: {exc}")
        return

    try:
        lot = import_manual_lot(
            buy_ts=buy_ts,
            buy_price=buy_price,
            quantity=quantity,
            note=note,
        )
    except Exception as exc:
        print(f"  [!] No se pudo importar el lote: {exc}")
        return

    print("\n  ✓ Lote importado")
    print(f"    Fecha : {lot.buy_ts}")
    print(f"    Qty   : {lot.qty_total} BTC")
    print(f"    Precio: {lot.buy_price} USDC")
    print(f"    ID    : {lot.source_order_id}")


def menu_tax_fifo(engine: Optional["GridEngine"] = None) -> None:
    """Submenú de consulta y mantenimiento del ledger fiscal FIFO."""
    while True:
        print("\n=== FISCAL FIFO ===")
        print("1. Ver resumen FIFO")
        print("2. Ver lotes abiertos")
        print("3. Ver incidencias de ventas no cubiertas")
        print("4. Simular venta FIFO")
        print("5. Importar lote inicial/manual")
        print("6. Ver archivos fiscales")
        print("v. Volver")

        opcion = input("Opción: ").strip().lower()

        if opcion == "1":
            try:
                from tax_fifo import build_tax_status
                print("\n=== RESUMEN FIFO ===")
                print(build_tax_status())
            except Exception as exc:
                print(f"  [!] Error leyendo resumen FIFO: {exc}")

        elif opcion == "2":
            try:
                from tax_fifo import build_tax_lots_text
                limit = _ask_tax_limit()
                print("\n=== LOTES FIFO ABIERTOS ===")
                print(build_tax_lots_text(limit=limit))
            except Exception as exc:
                print(f"  [!] Error leyendo lotes FIFO: {exc}")

        elif opcion == "3":
            try:
                from tax_fifo import build_tax_unmatched_text
                limit = _ask_tax_limit()
                print("\n=== INCIDENCIAS FIFO ===")
                print(build_tax_unmatched_text(limit=limit))
            except Exception as exc:
                print(f"  [!] Error leyendo incidencias FIFO: {exc}")

        elif opcion == "4":
            _simulate_tax_fifo_sale(engine)

        elif opcion == "5":
            _import_tax_manual_lot()

        elif opcion == "6":
            _show_tax_files()

        elif opcion == "v":
            break

        else:
            print("Opción inválida")

# =========================================================
# =================== GRID PREVIEW ========================
# =========================================================

def choose_initial_grid_price() -> Optional[Decimal]:
    """
    Obtiene el bid/ask/mid actual del ticker y pregunta al usuario el precio inicial del grid.
    El valor predeterminado es el precio medio.

    Retorna:
        Precio Decimal cuantizado a TICK_SIZE, o None en caso de fallo.
    """
    print("\n  Consultando bid/ask/mid...")

    try:
        ticker_resp, _ = get_ticker()
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


def _fmt_usdc_value(value: Decimal) -> str:
    """Formatea importes USDC usando la misma precisión visual que los precios."""
    return _price_key(Decimal(str(value)))


def _fmt_grid_size(value: object) -> str:
    """Formatea sizes del grid a 8 decimales maximo para no descuadrar tablas."""
    try:
        size = Decimal(str(value)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    except Exception:
        return "?"
    return fmt_amount(size)


def _read_available_balances() -> Tuple[Decimal, Decimal, bool]:
    """Lee balances disponibles de USDC y BTC; indica si la consulta fue válida."""
    try:
        balances_resp, _ = get_all_balances()
    except Exception as exc:
        log_event(f"[BALANCE] No se pudieron consultar balances: {exc}", "warning")
        return Decimal("0"), Decimal("0"), False

    if isinstance(balances_resp, dict) and balances_resp.get("error"):
        log_event(f"[BALANCE] Respuesta de error consultando balances: {balances_resp}", "warning")
        return Decimal("0"), Decimal("0"), False

    usdc_balance, btc_balance = _parse_balances(balances_resp)
    return usdc_balance, btc_balance, True


def _esc_pressed() -> bool:
    """
    Detecta ESC sin bloquear.

    - En Windows usa msvcrt.
    - En Debian/Linux/macOS usa termios + tty + select.
    - Evita avisos de Pylance en Windows.
    """
    import importlib
    import platform
    import select
    import sys

    if platform.system().lower() == "windows":
        try:
            msvcrt = cast(Any, importlib.import_module("msvcrt"))
        except ImportError:
            return False

        while msvcrt.kbhit():
            ch = msvcrt.getch()
            if ch == b"\x1b":
                return True

        return False

    if not sys.stdin.isatty():
        return False

    try:
        termios = cast(Any, importlib.import_module("termios"))
        tty = cast(Any, importlib.import_module("tty"))
    except ImportError:
        return False

    termios_mod = cast(Any, termios)
    tty_mod = cast(Any, tty)

    fd = sys.stdin.fileno()
    old_settings = termios_mod.tcgetattr(fd)

    try:
        tty_mod.setcbreak(fd)
        readable, _, _ = select.select([sys.stdin], [], [], 0)

        if not readable:
            return False

        return sys.stdin.read(1) == "\x1b"

    finally:
        termios_mod.tcsetattr(fd, termios_mod.TCSADRAIN, old_settings)


def _decimal_from_order_data(
    data: dict[str, Any],
    field_names: tuple[str, ...],
    default: Decimal = Decimal("0"),
) -> Decimal:
    """Extrae un Decimal de una respuesta de orden usando varios nombres posibles."""
    for name in field_names:
        raw = data.get(name)
        if raw is None or raw == "":
            continue
        try:
            value = Decimal(str(raw))
            return value
        except Exception:
            continue
    return default


def _record_initial_btc_buy_fifo(
    *,
    order_id: str,
    order_data: dict[str, Any],
    fallback_qty: Decimal,
    fallback_price: Decimal,
) -> bool:
    """Registra en tax_fifo el BTC comprado por la orden inicial."""
    from tax_fifo import record_tax_fill

    filled_qty = _decimal_from_order_data(
        order_data,
        ("filled_quantity", "filled_size", "executed_quantity", "filled_qty"),
        Decimal("0"),
    )

    status = str(order_data.get("status") or order_data.get("state") or "").lower()

    # Si la orden está filled pero el exchange no devuelve filled_quantity,
    # usamos la cantidad pedida como fallback.
    if filled_qty <= 0 and status == "filled":
        filled_qty = fallback_qty

    if filled_qty <= 0:
        return False

    fill_price = _decimal_from_order_data(
        order_data,
        ("average_fill_price", "avg_price", "avg_fill_price", "price"),
        fallback_price,
    )

    if fill_price <= 0:
        fill_price = fallback_price

    record_tax_fill(
        side="buy",
        price=fill_price,
        quantity=filled_qty,
        order_id=order_id,
    )

    print(
        f"  ✓ Lote FIFO registrado: {fmt_amount(filled_qty)} BTC "
        f"@ {_price_key(fill_price)} USDC"
    )
    log_event(
        f"[BTC_INIT] Lote FIFO registrado: BUY {fmt_amount(filled_qty)} BTC "
        f"@ {_price_key(fill_price)} USDC ({order_id})",
        "info",
    )
    return True


def _place_initial_btc_buy_order(quantity: Decimal, limit_price: Decimal) -> bool:
    """
    Crea una orden limit BUY para comprar el BTC inicial faltante.

    Devuelve True solo si:
      - la orden se crea,
      - se ejecuta completamente,
      - y queda registrada en tax_fifo.

    Si se pulsa ESC:
      - cancela la orden,
      - registra en tax_fifo cualquier fill parcial si lo hubo,
      - y devuelve False.
    """
    import uuid
    import time

    from api import cancel_order, get_order_by_id
    from http_client import send_request

    qty = Decimal(str(quantity))
    price = Decimal(str(limit_price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)

    if qty <= 0:
        return False

    print(
        f"\n  Creando orden LIMIT BUY de {fmt_amount(qty)} BTC "
        f"a {_price_key(price)} USDC..."
    )

    body = {
        "client_order_id": str(uuid.uuid4()),
        "symbol": SYMBOL,
        "side": "buy",
        "order_configuration": {
            "limit": {
                "base_size": fmt_amount(qty),
                "price": _price_key(price),
                "execution_instructions": ["post_only"],
            }
        },
    }

    resp, logs = send_request("POST", "/api/1.0/orders", body=body)
    for entry in logs:
        log_event(f"[BTC_INIT] {entry['msg']}", entry.get("level", "info"))

    if not isinstance(resp, dict) or resp.get("error"):
        print(f"  [!] No se pudo crear la orden de compra de BTC. Respuesta: {resp}")
        return False

    data = resp.get("data")
    if not isinstance(data, dict):
        print(f"  [!] Respuesta inesperada al crear compra BTC: {resp}")
        return False

    state = str(data.get("state") or data.get("status") or "").lower()
    order_id = data.get("venue_order_id")

    if state == "rejected":
        print("  [!] La orden de compra BTC fue rechazada por post_only o por el exchange.")
        print("      Prueba con otro precio límite o compra manualmente antes de arrancar el grid.")
        return False

    if not isinstance(order_id, str) or not order_id:
        print(f"  [!] No se obtuvo venue_order_id en la compra BTC. Respuesta: {resp}")
        return False

    print(f"  ✓ Orden de compra BTC creada: {order_id}")
    print("  Esperando a la ejecución de la compra... Pulsa ESC para cancelar la compra.")

    final_states = {"filled", "cancelled", "canceled", "rejected", "expired", "failed"}
    last_status_log = 0.0
    poll_interval = 2.0

    while True:
        if _esc_pressed():
            print("\n  [!] ESC detectado. Cancelando compra BTC...")

            cancel_resp, cancel_logs = cancel_order(order_id)
            for entry in cancel_logs:
                log_event(f"[BTC_INIT] {entry['msg']}", entry.get("level", "info"))

            if isinstance(cancel_resp, dict) and cancel_resp.get("error"):
                print(f"  [!] No se pudo cancelar la orden. Respuesta: {cancel_resp}")
            else:
                print("  ✓ Orden de compra BTC cancelada o solicitud de cancelación enviada.")

            # Tras cancelar, consultamos la orden por si hubo fill parcial antes de la cancelación.
            order_resp, order_logs = get_order_by_id(order_id)
            for entry in order_logs:
                log_event(f"[BTC_INIT] {entry['msg']}", entry.get("level", "info"))

            order_data = order_resp.get("data") if isinstance(order_resp, dict) else None
            if isinstance(order_data, dict):
                try:
                    if _record_initial_btc_buy_fifo(
                        order_id=order_id,
                        order_data=order_data,
                        fallback_qty=qty,
                        fallback_price=price,
                    ):
                        print("  [!] Hubo fill parcial antes de cancelar. El grid no se inicia automáticamente.")
                except Exception as exc:
                    print(f"  [!] Error registrando fill parcial en FIFO: {exc}")
                    log_event(f"[BTC_INIT] Error registrando fill parcial en FIFO: {exc}", "warning")

            return False

        order_resp, order_logs = get_order_by_id(order_id)
        for entry in order_logs:
            log_event(f"[BTC_INIT] {entry['msg']}", entry.get("level", "info"))

        if not isinstance(order_resp, dict) or order_resp.get("error"):
            now = time.time()
            if now - last_status_log >= 15:
                print("  ... esperando confirmación de la orden BTC")
                last_status_log = now
            time.sleep(poll_interval)
            continue

        order_data = order_resp.get("data")
        if not isinstance(order_data, dict):
            print(f"  [!] Respuesta inesperada consultando compra BTC: {order_resp}")
            time.sleep(poll_interval)
            continue

        status = str(order_data.get("status") or order_data.get("state") or "").lower()

        if status == "filled":
            try:
                fifo_ok = _record_initial_btc_buy_fifo(
                    order_id=order_id,
                    order_data=order_data,
                    fallback_qty=qty,
                    fallback_price=price,
                )
            except Exception as exc:
                print(f"  [!] La compra se ejecutó, pero no se pudo registrar en FIFO: {exc}")
                log_event(f"[BTC_INIT] Error registrando FIFO: {exc}", "warning")
                return False

            if not fifo_ok:
                print("  [!] La compra figura como ejecutada, pero no se pudo leer cantidad ejecutada.")
                print("      Revisa la orden manualmente antes de arrancar el grid.")
                return False

            print("  ✓ Compra BTC ejecutada y registrada en FIFO.")
            print("  ✓ Ya puedes continuar con el arranque del grid.")
            return True

        if status in final_states:
            print(f"  [!] La orden BTC terminó con estado '{status}'. El grid no se inicia.")
            return False

        now = time.time()
        if now - last_status_log >= 15:
            filled_qty = _decimal_from_order_data(
                order_data,
                ("filled_quantity", "filled_size", "executed_quantity", "filled_qty"),
                Decimal("0"),
            )
            if filled_qty > 0:
                print(
                    f"  ... esperando ejecución completa "
                    f"({fmt_amount(filled_qty)} / {fmt_amount(qty)} BTC ejecutados). ESC para cancelar."
                )
            else:
                print("  ... esperando a la ejecución de la compra. ESC para cancelar.")
            last_status_log = now

        time.sleep(poll_interval)


def show_grid_preview(
    levels_below: int,
    levels_above: int,
    base_size: Decimal,
    step_percent: Decimal,
    trailing_up: str,
    trailing_down: str,
    reserve_usdc: Decimal,
    bot_usdc_budget: Decimal,
) -> Tuple[bool, Optional[Decimal]]:
    """
    Muestra un resumen de configuración, fondos requeridos y presupuesto asignado.

    El presupuesto `bot_usdc_budget` es el capital máximo asignado al bot en
    equivalente USDC. Si es <= 0, se conserva el comportamiento anterior y se
    considera disponible todo el balance actual estimado.
    """
    def _lp(msg: str = "", level: str = "info") -> None:
        """Imprime y registra un mensaje."""
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
    _lp(f"  Trailing up      : {trailing_up.upper()}")
    _lp(f"  Trailing down    : {trailing_down.upper()}")

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
    required_btc_value = required_btc * initial_price
    total_required_value = required_usdc + required_btc_value

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

    _lp("\n  Fondos necesarios para arrancar:")
    _lp(f"    USDC para BUYs       : {_fmt_usdc_value(required_usdc)} USDC  ({len(buy_prices)} órdenes BUY)")
    _lp(f"    BTC para SELLs       : {fmt_amount(required_btc)} BTC   ({len(sell_prices)} órdenes SELL)")
    _lp(f"    Valor BTC estimado   : {_fmt_usdc_value(required_btc_value)} USDC @ {_price_key(initial_price)}")
    _lp(f"    Capital total estim. : {_fmt_usdc_value(total_required_value)} USDC")

    _lp("\n  Consultando balances...")
    usdc_balance, btc_balance, balances_ok = _read_available_balances()
    if not balances_ok:
        _lp("    [!] No se pudieron leer balances fiables.", "warning")

    current_total_value = usdc_balance + (btc_balance * initial_price)
    assigned_budget = Decimal(str(bot_usdc_budget))
    if assigned_budget <= 0:
        effective_budget = current_total_value
        budget_text = f"{_fmt_usdc_value(effective_budget)} USDC (sin límite configurado; usando balance actual estimado)"
    else:
        effective_budget = assigned_budget
        budget_text = f"{_fmt_usdc_value(effective_budget)} USDC"

    missing_btc = required_btc - btc_balance
    if missing_btc < 0:
        missing_btc = Decimal("0")
    missing_btc_cost = missing_btc * initial_price
    usdc_needed_now = required_usdc + missing_btc_cost

    budget_ok = effective_budget >= total_required_value
    cash_ok = usdc_balance >= usdc_needed_now
    btc_ok = btc_balance >= required_btc

    _lp(f"    Saldo asignado bot   : {budget_text}")
    _lp(f"    USDC disponible      : {_fmt_usdc_value(usdc_balance)} USDC")
    _lp(f"    USDC reservado       : {_fmt_usdc_value(reserve_usdc)} USDC")
    _lp(f"    BTC disponible       : {fmt_amount(btc_balance)} BTC")
    _lp(f"    Valor cuenta estim.  : {_fmt_usdc_value(current_total_value)} USDC")

    if missing_btc > 0:
        _lp("\n  BTC inicial faltante:")
        _lp(f"    BTC a comprar        : {fmt_amount(missing_btc)} BTC")
        _lp(f"    Coste estimado       : {_fmt_usdc_value(missing_btc_cost)} USDC")
        _lp(f"    USDC necesario ahora : {_fmt_usdc_value(usdc_needed_now)} USDC  (BUYs + compra BTC)")
    else:
        _lp("\n  BTC inicial faltante   : 0 BTC")
        _lp(f"  USDC necesario ahora   : {_fmt_usdc_value(required_usdc)} USDC")

    _lp("\n" + "=" * 50)

    if not budget_ok:
        _lp("  [!] Presupuesto asignado insuficiente para esta configuración.", "warning")
        _lp(f"      Asignado : {_fmt_usdc_value(effective_budget)} USDC", "warning")
        _lp(f"      Necesario: {_fmt_usdc_value(total_required_value)} USDC", "warning")
        _lp("      Ajusta saldo asignado, líneas, size o step.", "warning")
        _lp("=" * 50)
        return False, initial_price

    if not cash_ok:
        _lp("  [!] USDC disponible insuficiente para arrancar con esta configuración.", "warning")
        _lp(f"      Disponible ahora : {_fmt_usdc_value(usdc_balance)} USDC", "warning")
        _lp(f"      Necesario ahora  : {_fmt_usdc_value(usdc_needed_now)} USDC", "warning")
        _lp("      Reduce configuración, libera USDC o deposita fondos.", "warning")
        _lp("=" * 50)
        return False, initial_price

    if not btc_ok:
        _lp("  [!] Hay presupuesto suficiente, pero falta BTC inicial para las órdenes SELL.", "warning")
        try:
            respuesta_btc = input(
                f"\n  ¿Crear orden LIMIT BUY para comprar {fmt_amount(missing_btc)} BTC "
                f"a {_price_key(initial_price)} USDC? (s/n): "
            ).strip().lower()
        except EOFError:
            respuesta_btc = "n"

        if respuesta_btc.startswith("s"):
            _place_initial_btc_buy_order(missing_btc, initial_price)
        else:
            print("\n  Compra BTC cancelada. Grid no iniciado.")
        return False, initial_price

    _lp("  [✓] Fondos suficientes dentro del saldo asignado para iniciar el grid.")
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
    snapshot = engine.get_runtime_snapshot()
    levels = sorted(snapshot["levels"], reverse=True)
    price = snapshot["current_price"]
    orders = snapshot["active_orders"]
    base_size = snapshot["base_size"]

    closest = (
        min(levels, key=lambda level: abs(level - price))
        if price is not None and levels else None
    )

    SEP = "  " + "-" * 80
    print(f"\n  {'Precio':>12}  {'Side':<5}  {'Size':>10}  {'Order ID':<32}")
    print(SEP)

    for lvl in levels:
        key = _price_key(lvl)
        info = orders.get(key)

        if info:
            side = str(info["side"]).upper()
            oid = str(info["order_id"])

            size = info.get("size", base_size)
            size_str = _fmt_grid_size(size)

            if oid == "virtual":
                tag = " [V]"
                oid_str = "virtual"
            elif oid == "pending_post_only":
                tag = " [P]"
                oid_str = "latente"
            elif oid == "pending_manual":
                tag = " [M]"
                oid_str = "manual"
            elif oid == "pending_replace":
                tag = " [R]"
                oid_str = "reemplazando"
            else:
                tag = ""
                oid_str = oid[:32]
        else:
            side = "---"
            size_str = "-"
            oid_str = "vacío"
            tag = ""

        marker = " ◄" if closest is not None and lvl == closest else ""
        print(f"  {key:>12}  {side:<5}  {size_str:>10}  {oid_str:<32}{tag}{marker}")

    print(SEP)
    price_str = _price_key(price) if price else "N/A"
    print(f"  Precio actual: {price_str} USDC  |  Órdenes: {len(orders)}  |  Niveles: {len(levels)}")


def _show_active_orders(engine: "GridEngine") -> None:
    """
    Lista todas las órdenes activas registradas por el motor, agrupadas por lado y ordenadas por precio.
    """
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
        elif oid == "pending_replace":
            tag = " [reemplazando]"
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
        elif oid == "pending_replace":
            tag = " [reemplazando]"
        else:
            tag = ""
        print(f"  {'BUY':<5}  {key:>12}  {oid}{tag}")

    print(SEP)
    print(f"  Total: {len(sells)} SELL  |  {len(buys)} BUY")


def _trailing_menu(engine: "GridEngine") -> None:
    """
    Menú interactivo para la configuración en vivo de trailing up/down.
    Los cambios se pueden aplicar inmediatamente o descartar.
    """
    def _normalize_up_mode(value: object) -> str:
        """Normaliza el modo de trailing up a 'off', 'on' o 'extended'."""
        return normalize_trailing_up_mode(value)

    def _normalize_down_mode(value: object) -> str:
        """Normaliza el modo de trailing down a 'off', 'on' o 'extended'."""
        return normalize_trailing_down_mode(value)

    def _up_mode_label(mode: str) -> str:
        """Devuelve una etiqueta de visualización para el modo de trailing up."""
        return trailing_up_mode_label(mode)

    def _down_mode_label(mode: str) -> str:
        """Devuelve una etiqueta de visualización para el modo de trailing down."""
        return trailing_down_mode_label(mode)

    original_up = _normalize_up_mode(
        getattr(engine, "trailing_up_mode", engine.trailing_up_enabled)
    )
    original_down = _normalize_down_mode(
        getattr(engine, "trailing_down_mode", engine.trailing_down_enabled)
    )

    new_up = original_up
    new_down = original_down
    up_cycle = ["off", "on", "extended", "fixed_quote"]
    down_cycle = ["off", "on", "extended"]

    while True:
        print("\n=== CONFIGURAR TRAILINGS ===")
        print(f"1. Trailing up   > {_up_mode_label(new_up)}")
        print(f"2. Trailing down > {_down_mode_label(new_down)}")
        print("3. Atrás")

        opcion = input("Opción: ").strip()

        if opcion == "1":
            idx = up_cycle.index(new_up) if new_up in up_cycle else 0
            new_up = up_cycle[(idx + 1) % len(up_cycle)]
        elif opcion == "2":
            idx = down_cycle.index(new_down) if new_down in down_cycle else 0
            new_down = down_cycle[(idx + 1) % len(down_cycle)]
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
    """
    Construye una cadena legible para humanos que muestra los saldos disponibles y los fondos asignados en el grid.

    Args:
        engine: Instancia opcional de GridEngine; si se proporciona, se contabilizan las órdenes del grid.

    Retorna:
        Cadena de varias líneas con los saldos.
    """
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
                if order_id in {"virtual", "pending_post_only", "pending_manual", "pending_replace"}:
                    continue
                order_size = Decimal(str(info.get("size", base_size)))
                if info["side"] == "sell":
                    btc_en_grid += order_size
                elif info["side"] == "buy":
                    usdc_en_grid += info["price"] * order_size

            current_price = snapshot.get("current_price")
            btc_total = btc + btc_en_grid
            usdc_total = usdc + usdc_en_grid
            lines.extend([
                "",
                "EN LA REJILLA",
                "──────────────────────────────────────",
                f"BTC  en órdenes : {fmt_amount(btc_en_grid)}",
                f"USDC en órdenes : {_price_key(usdc_en_grid)}",
                "",
                "TOTAL",
                "──────────────────────────────────────",
                f"BTC  total      : {fmt_amount(btc_total)}",
                f"USDC total      : {_price_key(usdc_total)}",
                f"USD aprox ($)   : {_price_key(btc_total * current_price + usdc_total)}" if current_price is not None else "USD aprox ($)   : N/A",
            ])

    lines.append("──────────────────────────────────────")
    return "\n".join(lines)


def _format_balance_value(currency: str, value: Decimal) -> str:
    """Formatea un valor de balance según la moneda para una salida legible."""
    code = currency.strip().upper()
    if code in {"USDC", "USD", "EUR", "GBP"}:
        return _price_key(value)
    return fmt_amount(value)


def _format_balances_nonzero() -> str:
    """Devuelve un resumen legible mostrando solo monedas con saldo positivo."""
    try:
        balances_resp, _ = get_all_balances()
    except Exception as exc:
        return f"No se pudieron consultar los balances: {exc}"

    entries: list[dict[str, Any]] = []

    if isinstance(balances_resp, list):
        entries = [row for row in balances_resp if isinstance(row, dict)]
    elif isinstance(balances_resp, dict):
        for key in ("balances", "data", "items", "results"):
            value = balances_resp.get(key)
            if isinstance(value, list):
                entries = [row for row in value if isinstance(row, dict)]
                break

        if not entries:
            entries = [value for value in balances_resp.values() if isinstance(value, dict)]

    def _to_decimal(raw: object) -> Decimal:
        try:
            return Decimal(str(raw))
        except Exception:
            return Decimal("0")

    lines = [
        "──────────────────────────────────────",
        "BALANCES CON SALDO",
        "──────────────────────────────────────",
    ]

    shown = 0
    for entry in entries:
        currency = str(entry.get("currency") or entry.get("symbol") or entry.get("asset") or "").strip().upper()
        if not currency:
            continue

        available = _to_decimal(
            entry.get("available")
            or entry.get("free")
            or entry.get("balance")
            or entry.get("available_balance")
            or entry.get("quantity")
            or 0
        )
        reserved = _to_decimal(
            entry.get("reserved")
            or entry.get("locked")
            or entry.get("hold")
            or entry.get("reserved_balance")
            or 0
        )
        total = _to_decimal(
            entry.get("total")
            or entry.get("amount")
            or entry.get("available_balance")
            or (available + reserved)
        )

        if total <= 0 and available <= 0 and reserved <= 0:
            continue

        shown += 1
        lines.append(
            f"{currency:<6} disponible: {_format_balance_value(currency, available)}"
            f" | reservado: {_format_balance_value(currency, reserved)}"
            f" | total: {_format_balance_value(currency, total)}"
        )

    if shown == 0:
        lines.append("Sin balances con saldo positivo.")

    lines.append("──────────────────────────────────────")
    return "\n".join(lines)


def _show_balances_live(engine: Optional["GridEngine"] = None) -> None:
    """
    Imprime los balances actuales (incluyendo asignaciones del grid) en la consola.
    """
    print("  Consultando balances...")
    summary = format_balances_live(engine)
    print("\n" + "\n".join(f"  {line}" if line else "" for line in summary.splitlines()) + "\n")


def _add_manual_order(engine: "GridEngine") -> None:
    """
    Función interactiva para colocar una orden manual y registrarla en las órdenes activas del motor.
    Reserva el nivel antes de enviar para evitar condiciones de carrera.
    """
    try:
        while True:
            try:
                price_val = Decimal(input_with_esc("  Precio: ").strip())
                break
            except Exception:
                print("  Precio inválido.")

        key = _price_key(price_val)

        while True:
            side = input_with_esc("  Lado (buy/sell): ").strip().lower()
            if side in ("buy", "sell"):
                break
            print("  Lado inválido. Debe ser buy o sell.")

        default_size = engine.get_runtime_snapshot()["base_size"]
        while True:
            try:
                bs_input = input_with_esc(f"  Tamaño [{fmt_amount(default_size)}]: ").strip()
                base_size = Decimal(bs_input) if bs_input else default_size
                break
            except Exception:
                print("  Tamaño inválido.")

    except InputCancelled:
        print("  Entrada cancelada.")
        return

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


def _cancel_order_by_price(engine: "GridEngine") -> None:
    """
    Función interactiva para cancelar una orden activa del motor especificando su precio.
    """
    snapshot = engine.get_runtime_snapshot()
    active_orders = snapshot["active_orders"]

    if not active_orders:
        print("  No hay órdenes activas registradas en el engine.")
        return

    print("\n  Órdenes activas registradas:")
    print(f"  {'Side':<5}  {'Precio':>12}  {'Order ID'}")
    print("  " + "-" * 62)

    for key, info in sorted(active_orders.items(), key=lambda item: Decimal(item[0]), reverse=True):
        oid = str(info["order_id"])
        if oid == "virtual":
            tag = " [virtual]"
        elif oid == "pending_post_only":
            tag = " [latente]"
        elif oid == "pending_manual":
            tag = " [reservada]"
        elif oid == "pending_cancel":
            tag = " [cancelando]"
        elif oid == "pending_replace":
            tag = " [reemplazando]"
        else:
            tag = ""
        print(f"  {str(info['side']).upper():<5}  {key:>12}  {oid}{tag}")

    while True:
        raw_price = input("\n  Precio de la orden a cancelar: ").strip()
        if not raw_price:
            print("  Precio vacío.")
            continue
        try:
            target_key = _price_key(Decimal(raw_price))
            break
        except Exception:
            print("  Precio inválido.")

    info = engine.get_order_info(target_key)
    if info is None:
        print(f"  [!] No hay orden en {target_key}.")
        return

    order_id = str(info["order_id"])
    side = str(info["side"]).upper()

    if order_id in {"virtual", "pending_post_only", "pending_manual", "pending_cancel", "pending_replace"}:
        print(f"  [!] La orden en {target_key} no se puede cancelar desde aquí ({order_id}).")
        return

    confirm = input(f"  ¿Cancelar {side} en {target_key} ({order_id})? (s/n): ").strip().lower()
    if not confirm.startswith("s"):
        print("  Abortado.")
        return

    ok, logs, error_msg = engine.cancel_order_by_key(target_key, expected_order_id=order_id)
    for entry in logs:
        log_event(f"[CANCEL] {entry['msg']}", entry["level"])

    if not ok:
        print(f"  [!] {error_msg or 'No se pudo cancelar la orden.'}")
        return

    print(f"  ✓ Orden cancelada en {target_key}.")


def _fill_empty_levels(engine: "GridEngine") -> None:
    """
    Activa un llenado manual de los niveles vacíos del grid utilizando un precio fresco.
    """
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


def _resize_to_default(engine: "GridEngine") -> None:
    """Redimensiona ordenes de trailing_up fixed_quote al base_size predeterminado."""
    preview = engine.preview_resize_trailing_up_fixed_quote_to_default()

    if not preview.get("enabled"):
        print(f"  [!] {preview.get('reason') or 'Opción no disponible.'}")
        return

    real_orders = cast(list[dict[str, Any]], preview.get("real_orders", []) or [])
    state_only_orders = cast(list[dict[str, Any]], preview.get("state_only_orders", []) or [])
    required_btc = Decimal(str(preview.get("required_btc", "0") or "0"))
    required_usdc = Decimal(str(preview.get("required_usdc", "0") or "0"))
    default_size = Decimal(str(preview.get("default_size", "0") or "0"))
    anchor = preview.get("anchor")

    # Compatibilidad con engines intermedios: si required_usdc no viene en
    # preview, lo inferimos desde las BUY reales incluidas en la lista.
    if required_usdc <= 0:
        for row in real_orders:
            if str(row.get("side", "")).lower() != "buy":
                continue
            try:
                price = Decimal(str(row.get("price", row.get("price_key", "0"))))
                delta = Decimal(str(row.get("delta", "0")))
                if delta <= 0:
                    current_size = Decimal(str(row.get("current_size", "0")))
                    target_size = Decimal(str(row.get("target_size", default_size)))
                    delta = target_size - current_size
                if price > 0 and delta > 0:
                    required_usdc += price * delta
            except Exception:
                continue

    if not real_orders and not state_only_orders:
        print("  No hay ordenes fixed_quote por debajo del tamaño predeterminado.")
        return

    print("\n" + "=" * 50)
    print("  RESIZE TO DEFAULT — TRAILING UP FIXED_QUOTE")
    print("=" * 50)
    print(f"  Tamaño objetivo : {fmt_amount(default_size)} BTC")
    if anchor is not None:
        print(f"  Anchor fixed_q. : {_price_key(Decimal(str(anchor)))} USDC")
    print(f"  Ordenes reales  : {len(real_orders)}")
    print(f"  Virtual/latente : {len(state_only_orders)}")
    print(f"  BTC extra req.  : {fmt_amount(required_btc)}")
    print(f"  USDC extra req. : {_fmt_usdc_value(required_usdc)}")

    if real_orders:
        available_usdc, available_btc, balances_ok = _read_available_balances()
        if balances_ok:
            print(f"  BTC disponible  : {fmt_amount(available_btc)}")
            print(f"  USDC disponible : {_fmt_usdc_value(available_usdc)}")

            if available_btc < required_btc:
                print("  [!] BTC insuficiente para redimensionar todas las SELL reales.")
                return

            if available_usdc < required_usdc:
                print("  [!] USDC insuficiente para redimensionar todas las BUY reales.")
                return
        else:
            print("  [!] No se pudieron comprobar balances de forma fiable.")
            return

    def _row_side(row: dict[str, Any]) -> str:
        side = str(row.get("side", "?")).upper()
        return side if side in {"BUY", "SELL"} else "?"

    def _row_price(row: dict[str, Any]) -> Decimal:
        return Decimal(str(row.get("price", row.get("price_key", "0"))))

    def _row_delta(row: dict[str, Any]) -> Decimal:
        try:
            delta = Decimal(str(row.get("delta", "0")))
            if delta > 0:
                return delta
        except Exception:
            pass

        current_size = Decimal(str(row.get("current_size", "0")))
        target_size = Decimal(str(row.get("target_size", default_size)))
        return target_size - current_size

    def _row_extra(row: dict[str, Any]) -> str:
        side = _row_side(row)
        delta = _row_delta(row)
        if side == "BUY":
            return f"{_fmt_usdc_value(_row_price(row) * delta)} USDC"
        if side == "SELL":
            return f"{fmt_amount(delta)} BTC"
        return "-"

    def _print_resize_row(label: str, row: dict[str, Any]) -> None:
        print(
            f"  {label:<10} {_row_side(row):<5} {str(row['price_key']):>12} "
            f"{fmt_amount(Decimal(str(row['current_size']))):>14} "
            f"{fmt_amount(Decimal(str(row['target_size']))):>14} "
            f"{_row_extra(row):>18} "
            f"{str(row['order_id'])[:32]}"
        )

    print("\n  Ordenes a redimensionar:")
    print(
        f"  {'Tipo':<10} {'Side':<5} {'Precio':>12} "
        f"{'Size actual':>14} {'Nuevo size':>14} {'Extra':>18} {'Order ID'}"
    )
    print("  " + "-" * 102)

    shown = 0
    for row in real_orders[:25]:
        _print_resize_row("REAL", row)
        shown += 1

    for row in state_only_orders[:25]:
        order_id = str(row["order_id"])
        label = "VIRTUAL" if order_id == "virtual" else "LATENTE"
        _print_resize_row(label, row)
        shown += 1

    total_rows = len(real_orders) + len(state_only_orders)
    if total_rows > shown:
        print(f"  ... y {total_rows - shown} mas")

    try:
        confirm = input("\n  ¿Ejecutar resize to default? (s/n): " ).strip().lower()
    except EOFError:
        confirm = "n"

    if not confirm.startswith("s"):
        print("  Abortado.")
        return

    ok, logs, error_msg, summary = engine.resize_trailing_up_fixed_quote_to_default()
    for entry in logs:
        log_event(f"[RESIZE] {entry['msg']}", entry.get("level", "info"))

    resized_real = int(summary.get("resized_real", 0) or 0)
    updated_state_only = int(summary.get("updated_state_only", 0) or 0)
    skipped = int(summary.get("skipped", 0) or 0)
    failed = summary.get("failed", []) or []

    if not ok:
        print(f"  [!] {error_msg or 'No se pudo completar el resize.'}")
        if failed:
            print("  Fallos:")
            for item in failed[:10]:
                print(f"    - {item}")
        return

    print(
        f"  ✓ Resize completado. Reales: {resized_real} | "
        f"Virtual/latente: {updated_state_only} | Saltadas: {skipped}"
    )

def run_engine_menu(engine: "GridEngine", engine_thread: threading.Thread) -> None:
    """
    Submenú de monitor interactivo para una instancia de GridEngine en ejecución.

    Args:
        engine: El objeto GridEngine en ejecución.
        engine_thread: El hilo en el que se está ejecutando el motor.
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
        print("  6. Cancelar orden por precio")
        print("  7. Fill empty levels")
        resize_to_default_available = snapshot.get("trailing_up_mode") == "fixed_quote"
        if resize_to_default_available:
            print("  8. Resize to default")
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
            _cancel_order_by_price(engine)
        elif opcion == "7":
            _fill_empty_levels(engine)
        elif opcion == "8" and resize_to_default_available:
            _resize_to_default(engine)
        elif opcion == "8":
            print("  Opción no disponible: trailing up no está en fixed_quote.")
        elif opcion.lower() == "v":
            print("  Volviendo al menú principal...")
            break

    # If the engine stopped unexpectedly, warn the user
    if not engine_thread.is_alive() and engine.is_running():
        print("\n  [!] El engine se detuvo inesperadamente. Revisa el log.")


# =========================================================
# ========================= MENU ==========================
# =========================================================

def show_menu(engine_running: bool = False) -> str:
    """
    Muestra el menú principal de la CLI y devuelve la opción del usuario.

    Args:
        engine_running: Si es True, algunas opciones se muestran como bloqueadas.

    Retorna:
        Opción en minúsculas y sin espacios.
    """
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
    print("9. Backtesting")
    print("10. Fiscal FIFO")
    print("c. Configuración manual")
    print("0. Salir")
    print("=" * 40)
    return input("Selecciona una opción: ").strip().lower()


# =========================================================
# ====================== MAIN LOOP ========================
# =========================================================

def run_cli() -> None:
    """
    Punto de entrada de la CLI interactiva.
    Gestiona el bucle principal del menú, el ciclo de vida del motor y la integración con el bot de Telegram.
    """
    from engine import GridEngine
    from private_config import (
        get_base_size_default,
        get_bot_usdc_budget_default,
        get_reserve_usdc_default,
        get_grid_levels_above,
        get_grid_levels_below,
        get_step_percent_default,
        get_telegram_enabled,
        get_trailing_down_default,
        get_trailing_up_default,
        save_grid_config,
    )

    telegram_enabled = get_telegram_enabled(default=True)
    start_telegram_bot = None
    tg_state = None

    if telegram_enabled:
        from telegram_bot import start_telegram_bot as _start_telegram_bot, _state as _tg_state
        start_telegram_bot = _start_telegram_bot
        tg_state = _tg_state

    grid_levels_below:    int     = get_grid_levels_below(DEFAULT_GRID_LEVELS_BELOW)
    grid_levels_above:    int     = get_grid_levels_above(DEFAULT_GRID_LEVELS_ABOVE)
    base_size_default:    Decimal = Decimal(get_base_size_default(str(DEFAULT_BASE_SIZE)))
    step_percent_default: Decimal = Decimal(get_step_percent_default(str(DEFAULT_STEP_PERCENT)))
    trailing_up_default:  str     = get_trailing_up_default("off")
    trailing_down_default: str    = get_trailing_down_default("off")
    reserve_usdc_default:  Decimal = Decimal(get_reserve_usdc_default(str(MIN_USDC_RESERVE)))
    bot_usdc_budget_default: Decimal = Decimal(get_bot_usdc_budget_default("0"))

    print(f"GRID ENGINE {VERSION} — CLI INTERACTIVO")

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
                ticker_resp, logs = get_ticker()
                for l in logs:
                    log_event(f"[LOG] {l['msg']}")

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
                    indent = " " * 31
                    log_event(
                        f"Bid: {_price_key(Decimal(str(bid)))}\n"
                        f"{indent}Ask: {_price_key(Decimal(str(ask)))}\n"
                        f"{indent}Mid: {_price_key(Decimal(str(mid)))}"
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
            print(_format_balances_nonzero())

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
                print("    Usa el monitor del engine (opción 7) para añadir órdenes manuales.")
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
                    trailing_up_default,
                    trailing_down_default,
                    reserve_usdc_default,
                    bot_usdc_budget_default,
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
                reserve_usdc=reserve_usdc_default,
            )

            if not recover_state:
                engine.set_trailing(trailing_up_default, trailing_down_default)

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
        # 9. Backtesting
        # --------------------------------------------------
        elif opcion == "9":
            from backtesting import prompt_backtest

            prompt_backtest()

        # --------------------------------------------------
        # 10. Fiscal FIFO
        # --------------------------------------------------
        elif opcion == "10":
            menu_tax_fifo(engine if engine_running else None)

        # --------------------------------------------------
        # c.Configuración manual
        # --------------------------------------------------
        elif opcion.lower() == "c":
            print("\n=== Configuración manual ===")

            try:
                new_levels_below = input_with_esc(f"Niveles por debajo del precio inicial [{grid_levels_below}]: ")
                if new_levels_below.strip():
                    try:
                        grid_levels_below = int(new_levels_below)
                    except ValueError:
                        log_event("[ERROR] Valor de niveles abajo inválido, conservando el anterior.", "error")

                new_levels_above = input_with_esc(f"Niveles por encima del precio inicial [{grid_levels_above}]: ")
                if new_levels_above.strip():
                    try:
                        grid_levels_above = int(new_levels_above)
                    except ValueError:
                        log_event("[ERROR] Valor de niveles arriba inválido, conservando el anterior.", "error")

                new_bs = input_with_esc(f"Base size por defecto [{fmt_amount(base_size_default)}]: ")
                if new_bs.strip():
                    try:
                        base_size_default = Decimal(new_bs)
                    except Exception:
                        log_event("[ERROR] Valor de base size inválido, conservando el anterior.", "error")

                new_step_percent = input_with_esc(f"Step percent por defecto [{fmt_amount(step_percent_default)}]: ")
                if new_step_percent.strip():
                    try:
                        step_percent_default = Decimal(new_step_percent)
                    except Exception:
                        log_event("[ERROR] Valor de step percent inválido, conservando el anterior.", "error")

                new_trailing_up = input_with_esc(f"Trailing up (off/on/extended/fixed_quote) [{trailing_up_default}]: ").strip().lower()
                if new_trailing_up and new_trailing_up in {"quote", "quote_fijo", "fixed-quote", "fixedquote"}:
                    new_trailing_up = "fixed_quote"
                if new_trailing_up and new_trailing_up in ("off", "on", "extended", "fixed_quote"):
                    trailing_up_default = new_trailing_up
                elif new_trailing_up:
                    log_event("[ERROR] Valor de trailing up inválido (debe ser off, on, extended o fixed_quote), conservando el anterior.", "error")

                new_trailing_down = input_with_esc(f"Trailing down (off/on/extended) [{trailing_down_default}]: ").strip().lower()
                if new_trailing_down and new_trailing_down in ("off", "on", "extended"):
                    trailing_down_default = new_trailing_down
                elif new_trailing_down:
                    log_event("[ERROR] Valor de trailing down inválido (debe ser off, on o extended), conservando el anterior.", "error")

                available_usdc, _, balances_ok = _read_available_balances()
                if balances_ok:
                    print(f"USDC disponible actual: {_fmt_usdc_value(available_usdc)}")
                else:
                    print("USDC disponible actual: no disponible")

                new_usdc_reserve = input_with_esc(
                    f"Cuanto USDC quieres reservar como colchón de seguridad: [{_fmt_usdc_value(reserve_usdc_default)}]: "
                ).strip().lower()

                if new_usdc_reserve:
                    try:
                        reserve_usdc_default = Decimal(new_usdc_reserve)
                        if reserve_usdc_default < 0:
                            raise ValueError("el colchón de seguridad no puede ser negativo")
                        if balances_ok and reserve_usdc_default > available_usdc:
                            raise ValueError(
                                f"el colchón de seguridad asignado ({_fmt_usdc_value(reserve_usdc_default)}) "
                                f"supera el USDC disponible ({_fmt_usdc_value(available_usdc)})"
                            )
                        log_event(f"Colchón de seguridad actualizado a {_fmt_usdc_value(reserve_usdc_default)} USDC", "info")
                    except Exception as exc:
                        log_event(f"[ERROR] Valor de colchón de seguridad inválido: {exc}. Conservando el anterior ({_fmt_usdc_value(reserve_usdc_default)}).", "error")
                        reserve_usdc_default = reserve_usdc_default

                budget_default_text = (
                    _fmt_usdc_value(bot_usdc_budget_default)
                    if bot_usdc_budget_default > 0
                    else "0"
                )
                new_budget = input_with_esc(
                    f"Saldo USDC total a emplear por el bot [{budget_default_text}] "
                    "(0 = sin límite explícito): "
                ).strip().lower()

                if new_budget:
                    try:
                        if new_budget in {"max", "todo", "all"}:
                            if not balances_ok:
                                raise ValueError("no se pudo leer USDC disponible")
                            parsed_budget = max(Decimal("0"), available_usdc - reserve_usdc_default)
                        else:
                            parsed_budget = Decimal(new_budget)

                        if parsed_budget < 0:
                            raise ValueError("el saldo no puede ser negativo")
                        
                        if balances_ok:
                            max_budget = max(Decimal("0"), available_usdc - reserve_usdc_default)
                            if parsed_budget > max_budget:
                                raise ValueError(
                                    f"el saldo asignado ({_fmt_usdc_value(parsed_budget)}) "
                                    f"supera el USDC disponible ({_fmt_usdc_value(max_budget)})"
                                )
                        bot_usdc_budget_default = parsed_budget

                    except Exception as exc:
                        log_event(f"[ERROR] Valor de saldo asignado inválido: {exc}. Conservando el anterior.", "error")
        
                elif balances_ok:
                    bot_usdc_budget_default = max(Decimal("0"), available_usdc - reserve_usdc_default)
                    log_event(f"Saldo asignado actualizado a {_fmt_usdc_value(bot_usdc_budget_default)} USDC (ajustado automáticamente según el colchón de seguridad)", "info")

                # Guardar la configuración
                save_grid_config(
                    grid_levels_below,
                    grid_levels_above,
                    str(base_size_default),
                    str(step_percent_default),
                    trailing_up_default,
                    trailing_down_default,
                    str(reserve_usdc_default),
                    str(bot_usdc_budget_default),
                )
                print("✓ Configuración guardada como predeterminada.")

            except InputCancelled:
                print("Entrada cancelada. Configuración no guardada.")
                return

        # --------------------------------------------------
        # 0.Salir
        # --------------------------------------------------
        elif opcion == "0":
            print("¡Hasta luego!")
            break

        else:
            print("Opción no válida. Intenta de nuevo.")
