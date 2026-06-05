"""
telegram_bot.py — Interfaz de control y monitorización via Telegram.

Comandos disponibles:
  /help      — muestra ayuda con comandos disponibles
  /status    — precio actual, órdenes activas, fills de sesión y trailings
  /grid      — niveles del grid con órdenes
  /balance   — balances disponibles y fondos comprometidos en el grid
  /config    — muestra la configuración guardada del grid
  /set_config — cambia un valor de configuración guardada
  /trailings — muestra o configura trailing up/down
  /analyze   — resumen de fills emparejados y beneficio estimado
  /taxstatus — resumen FIFO fiscal
  /taxlots   — lotes FIFO abiertos
  /taxunmatched — incidencias FIFO
  /taxsim    — simulación FIFO de una venta
  /taxaddlot — importa lote fiscal inicial/manual
  /taxreport — exporta CSV/JSON fiscales
  /start_engine  — previsualiza y arranca el engine con confirmación
  /stop      — detiene el engine (requiere confirmación)
  /add_order — añade una orden manual (guiado por pasos)
  /cancel    — cancela una orden por precio (requiere confirmación)
  /confirm   — confirma una acción pendiente
  /abort     — cancela una acción pendiente

Notificaciones automáticas:
  - Fill confirmado
  - Orden rechazada (post_only)
  - Trailing down detenido por límite
  - Engine caído inesperadamente

Seguridad:
  - Solo el CHAT_ID configurado puede interactuar con el bot
  - Comandos destructivos requieren confirmación con /confirm o /abort
"""

import asyncio
import logging
import threading
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast

from telegram import Message, Update
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from api import _parse_balances, _price_key, fmt_amount, get_all_balances, get_current_price
from cli import format_balances_live
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
)
from logger import log_event
from trailing import (
    normalize_trailing_down_mode,
    normalize_trailing_up_mode,
    parse_trailing_down_mode,
    parse_trailing_up_mode,
    trailing_mode_label,
)
from tax_fifo import (
    TAX_LOTS_PATH,
    TAX_SALES_PATH,
    TAX_UNMATCHED_SALES_PATH,
    build_tax_lots_text,
    build_tax_status,
    build_tax_unmatched_text,
    import_manual_lot,
    simulate_fifo_sell,
)

if TYPE_CHECKING:
    from engine import GridEngine

# =========================================================
# Configuración
# =========================================================

TELEGRAM_TOKEN_PATH = Path("telegramapi.token")
TELEGRAM_CHATID_PATH = Path("telegram_chatid.txt")


def _read_token() -> Optional[str]:
    """
    Lee el token del bot en este orden:
    1. private_config.ini [telegram] token
    2. Archivo telegramapi.token (legacy)
    """
    try:
        from private_config import get_telegram_token

        token = get_telegram_token()
        if token:
            return token
    except Exception:
        pass

    if TELEGRAM_TOKEN_PATH.exists():
        return TELEGRAM_TOKEN_PATH.read_text(encoding="utf-8").strip()

    log_event("[TELEGRAM] No se encontró token (private_config.ini o telegramapi.token)", "error")
    return None


def _read_chat_id() -> Optional[int]:
    """
    Lee el chat_id autorizado en este orden:
    1. private_config.ini [telegram] chat_id
    2. Archivo telegram_chatid.txt (legacy)
    """
    try:
        from private_config import get_telegram_chat_id

        chat_id = get_telegram_chat_id()
        if chat_id is not None:
            return chat_id
    except Exception:
        pass

    if TELEGRAM_CHATID_PATH.exists():
        try:
            return int(TELEGRAM_CHATID_PATH.read_text(encoding="utf-8").strip())
        except ValueError:
            log_event("[TELEGRAM] telegram_chatid.txt contiene un valor inválido", "error")

    log_event("[TELEGRAM] No se encontró chat_id (private_config.ini o telegram_chatid.txt)", "error")
    return None


CHAT_ID: Optional[int] = _read_chat_id()


# =========================================================
# Estado compartido del bot
# =========================================================

class BotState:
    """Estado compartido entre el hilo del engine y el hilo de Telegram."""

    def __init__(self) -> None:
        self.engine: Optional["GridEngine"] = None
        self.engine_thread: Optional[threading.Thread] = None
        self.pending_confirm: Optional[tuple[str, dict[str, Any]]] = None
        self.add_order_step: Optional[str] = None  # "price" | "side" | "size" | "confirm"
        self.add_order_data: dict[str, Any] = {}


_state = BotState()


# =========================================================
# Utilidades
# =========================================================

def _get_message(update: Update) -> Optional[Message]:
    """Devuelve el mensaje del update cuando el evento lo incluye."""
    return update.message


def _get_engine() -> Optional["GridEngine"]:
    """Devuelve la instancia actual del engine gestionada por Telegram."""
    return _state.engine


def _get_engine_thread() -> Optional[threading.Thread]:
    """Devuelve el hilo donde corre el engine, si existe."""
    return _state.engine_thread


def _authorized(update: Update) -> bool:
    """Rechaza mensajes de usuarios no autorizados."""
    if CHAT_ID is None:
        return False
    chat = update.effective_chat
    return chat is not None and chat.id == CHAT_ID


def _engine_running() -> bool:
    """Indica si el engine existe y su hilo sigue activo."""
    engine = _get_engine()
    thread = _get_engine_thread()
    return engine is not None and thread is not None and thread.is_alive()


def _normalize_trailing_up_mode(value: object) -> str:
    """Normaliza el modo de trailing up usado por el engine."""
    return normalize_trailing_up_mode(value)


def _normalize_trailing_down_mode(value: object) -> str:
    """Normaliza el modo de trailing down usado por el engine."""
    return normalize_trailing_down_mode(value)


def _get_trailing_up_mode(engine: "GridEngine") -> str:
    """Lee el modo de trailing up, soportando estados antiguos booleanos."""
    raw_mode = getattr(engine, "trailing_up_mode", None)
    if raw_mode is None:
        raw_mode = getattr(engine, "trailing_up_enabled", False)
    return _normalize_trailing_up_mode(raw_mode)


def _get_trailing_down_mode(engine: "GridEngine") -> str:
    """Lee el modo de trailing down, soportando estados antiguos booleanos."""
    raw_mode = getattr(engine, "trailing_down_mode", None)
    if raw_mode is None:
        raw_mode = getattr(engine, "trailing_down_enabled", False)
    return _normalize_trailing_down_mode(raw_mode)


def _format_trailing_status(engine: "GridEngine") -> str:
    """Construye el bloque de estado de trailings para el comando /trailings."""
    up_mode = _get_trailing_up_mode(engine)
    down_mode = _get_trailing_down_mode(engine)

    return (
        "⚙️ *TRAILINGS*\n"
        f"Trailing up   : `{trailing_mode_label(up_mode)}`\n"
        f"Trailing down : `{trailing_mode_label(down_mode)}`\n\n"
        "Cambiar configuración:\n"
        "`/trailings up off|on|extended|fixed_quote`\n"
        "`/trailings down off|on|extended`\n\n"
        "Al poner un trailing en `off`, se eliminan sus órdenes virtuales pendientes."
    )


def _parse_on_off(value: str) -> Optional[bool]:
    """Parsea variantes comunes de on/off usadas en comandos de Telegram."""
    normalized = value.strip().lower()
    if normalized in {"on", "true", "1", "si", "sí", "s", "enable", "enabled"}:
        return True
    if normalized in {"off", "false", "0", "no", "n", "disable", "disabled"}:
        return False
    return None


def _parse_trailing_up_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing up recibido por comando."""
    return parse_trailing_up_mode(value)


def _parse_trailing_down_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing down recibido por comando."""
    return parse_trailing_down_mode(value)


def _apply_trailing_config(
    engine: "GridEngine",
    *,
    trailing_up: Optional[str] = None,
    trailing_down: Optional[str] = None,
) -> tuple[bool, str]:
    """Aplica cambios de trailing en caliente usando GridEngine.set_trailing()."""
    if not hasattr(engine, "set_trailing"):
        return False, "El engine no expone set_trailing()."

    current_up = _get_trailing_up_mode(engine)
    current_down = _get_trailing_down_mode(engine)

    new_up = current_up if trailing_up is None else _normalize_trailing_up_mode(trailing_up)
    new_down = current_down if trailing_down is None else _normalize_trailing_down_mode(trailing_down)

    if new_up not in {"off", "on", "extended", "fixed_quote"}:
        return False, "Modo de trailing up inválido."
    if new_down not in {"off", "on", "extended"}:
        return False, "Modo de trailing down inválido."

    engine.set_trailing(new_up, new_down)
    return True, _format_trailing_status(engine)


def _decimal_or_default(value: object, default: Decimal) -> Decimal:
    """Convierte un valor de configuración a Decimal con fallback seguro."""
    try:
        return Decimal(str(value).strip())
    except Exception:
        return Decimal(str(default))


def _parse_percent_value(value: str) -> Decimal:
    """Parsea step_percent; admite valores decimales o texto con % al final."""
    text = value.strip().replace(",", ".")
    if text.endswith("%"):
        return Decimal(text[:-1].strip()) / Decimal("100")
    return Decimal(text)


def _load_grid_config() -> dict[str, Any]:
    """Carga la configuración persistida del grid con defaults seguros."""
    from private_config import (
        get_base_size_default,
        get_bot_usdc_budget_default,
        get_grid_levels_above,
        get_grid_levels_below,
        get_step_percent_default,
        get_trailing_down_default,
        get_trailing_up_default,
    )

    base_size = _decimal_or_default(
        get_base_size_default(str(DEFAULT_BASE_SIZE)),
        DEFAULT_BASE_SIZE,
    )
    step_percent = _decimal_or_default(
        get_step_percent_default(str(DEFAULT_STEP_PERCENT)),
        DEFAULT_STEP_PERCENT,
    )
    bot_usdc_budget = _decimal_or_default(
        get_bot_usdc_budget_default("0"),
        Decimal("0"),
    )
    trailing_up = normalize_trailing_up_mode(
        get_trailing_up_default(str(DEFAULT_TRAILING_UP))
    )
    trailing_down = normalize_trailing_down_mode(
        get_trailing_down_default(str(DEFAULT_TRAILING_DOWN))
    )

    return {
        "levels_below": max(0, int(get_grid_levels_below(DEFAULT_GRID_LEVELS_BELOW))),
        "levels_above": max(0, int(get_grid_levels_above(DEFAULT_GRID_LEVELS_ABOVE))),
        "base_size": base_size,
        "step_percent": step_percent,
        "trailing_up": trailing_up,
        "trailing_down": trailing_down,
        "bot_usdc_budget": bot_usdc_budget if bot_usdc_budget > 0 else Decimal("0"),
    }


def _save_grid_config_from_dict(cfg: dict[str, Any]) -> None:
    """Persiste una configuración de grid ya validada."""
    from private_config import save_grid_config

    save_grid_config(
        int(cfg["levels_below"]),
        int(cfg["levels_above"]),
        str(cfg["base_size"]),
        str(cfg["step_percent"]),
        str(cfg["trailing_up"]),
        str(cfg["trailing_down"]),
        str(cfg["bot_usdc_budget"]),
    )


def _format_budget(value: Decimal) -> str:
    """Formatea el presupuesto asignado al bot."""
    if value <= 0:
        return "0 (sin límite explícito)"
    return f"{_price_key(value)} USDC"


def _fmt_grid_size(value: object) -> str:
    """Formatea sizes del grid a 8 decimales maximo para salidas Telegram."""
    try:
        size = Decimal(str(value)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    except Exception:
        return "?"
    return fmt_amount(size)


def _format_grid_config(cfg: dict[str, Any]) -> str:
    """Construye el texto visible para /config."""
    lines = [
        "⚙️ *CONFIG GRID*",
        "```",
        f"Symbol          : {SYMBOL}",
        f"Levels below    : {cfg['levels_below']}",
        f"Levels above    : {cfg['levels_above']}",
        f"Base size       : {fmt_amount(cfg['base_size'])} BTC",
        f"Step percent    : {fmt_amount(cfg['step_percent'] * Decimal('100'))}%",
        f"Trailing up     : {trailing_mode_label(str(cfg['trailing_up']))}",
        f"Trailing down   : {trailing_mode_label(str(cfg['trailing_down']))}",
        f"Bot USDC budget : {_format_budget(Decimal(str(cfg['bot_usdc_budget'])))}",
        "```",
        "Cambiar un valor:",
        "`/set_config levels_below 3`",
        "`/set_config levels_above 3`",
        "`/set_config base_size 0.00008`",
        "`/set_config step_percent 0.2%`",
        "`/set_config trailing_up extended`",
        "`/set_config trailing_down on`",
        "`/set_config bot_usdc_budget 1000`",
    ]
    if _engine_running():
        lines.append("\nEl cambio de configuración afecta a próximos arranques. Para el engine activo usa /trailings.")
    return "\n".join(lines)


def _set_config_usage() -> str:
    """Texto de ayuda para /set_config."""
    return (
        "Uso:\n"
        "`/set_config levels_below 3`\n"
        "`/set_config levels_above 3`\n"
        "`/set_config base_size 0.00008`\n"
        "`/set_config step_percent 0.2%`\n"
        "`/set_config trailing_up off|on|extended|fixed_quote`\n"
        "`/set_config trailing_down off|on|extended`\n"
        "`/set_config bot_usdc_budget 1000`\n"
        "También puedes usar `max` en bot_usdc_budget para tomar el USDC disponible."
    )


def _read_available_balances_for_telegram() -> tuple[Optional[Decimal], Optional[Decimal], list[dict[str, str]]]:
    """Lee saldos disponibles USDC/BTC para preflight de Telegram."""
    logs: list[dict[str, str]] = []
    try:
        balances_resp, balance_logs = get_all_balances()
        for log_item in balance_logs:
            logs.append({"level": log_item.get("level", "info"), "msg": log_item["msg"]})
    except Exception as exc:
        logs.append({"level": "error", "msg": f"No se pudieron consultar balances: {exc}"})
        return None, None, logs

    if not isinstance(balances_resp, dict) or balances_resp.get("error"):
        logs.append({"level": "error", "msg": f"Respuesta inválida consultando balances: {balances_resp}"})
        return None, None, logs

    usdc_balance, btc_balance = _parse_balances(balances_resp)
    return usdc_balance, btc_balance, logs


def _build_grid_levels_for_start(cfg: dict[str, Any], initial_price: Decimal) -> tuple[Decimal, list[Decimal]]:
    """Calcula step y niveles iniciales para el preflight de arranque."""
    center = Decimal(str(initial_price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
    step = (center * Decimal(str(cfg["step_percent"]))).quantize(TICK_SIZE, rounding=ROUND_DOWN)
    if step <= 0:
        raise ValueError("step calculado inválido")

    levels: list[Decimal] = []
    for i in range(-int(cfg["levels_below"]), int(cfg["levels_above"]) + 1):
        lvl = (center + (Decimal(i) * step)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
        levels.append(lvl)
    return step, sorted(set(levels))


def _build_start_preflight_text(cfg: dict[str, Any], initial_price: Decimal) -> tuple[bool, str]:
    """Genera el resumen previo de arranque y valida fondos/presupuesto."""
    try:
        step, levels = _build_grid_levels_for_start(cfg, initial_price)
    except Exception as exc:
        return False, f"❌ Configuración inválida: {exc}"

    base_size = Decimal(str(cfg["base_size"]))
    if base_size <= 0:
        return False, "❌ Configuración inválida: base_size debe ser mayor que cero."

    center = Decimal(str(initial_price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
    buy_prices = [lvl for lvl in levels if lvl < center]
    sell_prices = [lvl for lvl in levels if lvl > center]

    required_usdc = sum((base_size * price for price in buy_prices), Decimal("0"))
    required_btc = base_size * Decimal(len(sell_prices))
    required_btc_value = required_btc * center
    total_required_value = required_usdc + required_btc_value

    usdc_balance, btc_balance, logs = _read_available_balances_for_telegram()
    for log_item in logs:
        log_event(f"[TELEGRAM] {log_item['msg']}", log_item.get("level", "info"))

    if usdc_balance is None or btc_balance is None:
        return False, "❌ No se pudieron leer balances fiables para validar el arranque."

    current_total_value = usdc_balance + (btc_balance * center)
    configured_budget = Decimal(str(cfg["bot_usdc_budget"]))
    effective_budget = configured_budget if configured_budget > 0 else current_total_value
    budget_label = _format_budget(configured_budget)

    missing_btc = required_btc - btc_balance
    if missing_btc < 0:
        missing_btc = Decimal("0")
    missing_btc_cost = missing_btc * center

    budget_ok = effective_budget >= total_required_value
    usdc_ok = usdc_balance >= required_usdc
    btc_ok = btc_balance >= required_btc
    ok = budget_ok and usdc_ok and btc_ok

    lines = [
        "🚀 *PREVIEW START ENGINE*",
        "```",
        f"Symbol             : {SYMBOL}",
        f"Precio inicial     : {_price_key(center)} USDC",
        f"Step calculado     : {_price_key(step)} USDC ({fmt_amount(Decimal(str(cfg['step_percent'])) * Decimal('100'))}%)",
        f"Levels below/above : {cfg['levels_below']} / {cfg['levels_above']}",
        f"Base size          : {fmt_amount(base_size)} BTC",
        f"Trailing up/down   : {str(cfg['trailing_up']).upper()} / {str(cfg['trailing_down']).upper()}",
        "",
        "Fondos necesarios",
        f"USDC para BUYs     : {_price_key(required_usdc)} USDC",
        f"BTC para SELLs     : {fmt_amount(required_btc)} BTC",
        f"Valor BTC estim.   : {_price_key(required_btc_value)} USDC",
        f"Capital estimado   : {_price_key(total_required_value)} USDC",
        "",
        "Balance disponible",
        f"USDC disponible    : {_price_key(usdc_balance)} USDC",
        f"BTC disponible     : {fmt_amount(btc_balance)} BTC",
        f"Valor cuenta estim.: {_price_key(current_total_value)} USDC",
        f"Saldo asignado bot : {budget_label}",
    ]

    if missing_btc > 0:
        lines.extend([
            "",
            "BTC inicial faltante",
            f"BTC a comprar      : {fmt_amount(missing_btc)} BTC",
            f"Coste estimado     : {_price_key(missing_btc_cost)} USDC",
        ])

    lines.append("```")

    if ok:
        lines.append("✅ Fondos suficientes. Responde /confirm para iniciar o /abort para cancelar.")
    else:
        reasons: list[str] = []
        if not budget_ok:
            reasons.append(
                f"presupuesto asignado insuficiente ({_price_key(effective_budget)} < {_price_key(total_required_value)} USDC)"
            )
        if not usdc_ok:
            reasons.append(
                f"USDC insuficiente para BUYs ({_price_key(usdc_balance)} < {_price_key(required_usdc)})"
            )
        if not btc_ok:
            reasons.append(
                f"BTC insuficiente para SELLs ({fmt_amount(btc_balance)} < {fmt_amount(required_btc)})"
            )
        lines.append("❌ No se puede iniciar: " + "; ".join(reasons) + ".")

    return ok, "\n".join(lines)


async def _execute_start_engine(
    message: Message,
    *,
    recover_state: bool,
    initial_price: Optional[Decimal],
    cfg: Optional[dict[str, Any]] = None,
) -> None:
    """Inicializa y arranca el engine desde Telegram con la configuración indicada."""
    if recover_state and not STATE_PATH.exists():
        await message.reply_text("❌ Ya no existe estado previo que recuperar.")
        return

    cfg = cfg or _load_grid_config()

    from engine import GridEngine

    engine = GridEngine(
        levels_below=int(cfg["levels_below"]),
        levels_above=int(cfg["levels_above"]),
        step_percent=Decimal(str(cfg["step_percent"])),
        base_size=Decimal(str(cfg["base_size"])),
        initial_price=initial_price,
    )

    if not recover_state:
        engine.set_trailing(str(cfg["trailing_up"]), str(cfg["trailing_down"]))

    try:
        engine.initialize(recover_state=recover_state)
    except Exception as exc:
        log_event(f"[TELEGRAM] No se pudo inicializar el engine: {exc}", "error")
        await message.reply_text(f"❌ No se pudo inicializar el engine: {exc}")
        return

    thread = threading.Thread(
        target=engine.run,
        daemon=True,
        name="GridEngineThread",
    )
    thread.start()

    _state.engine = engine
    _state.engine_thread = thread

    if recover_state:
        log_event("[TELEGRAM] Engine iniciado via Telegram recuperando estado.", "info")
        await message.reply_text("✅ Engine en marcha recuperando estado previo.")
    else:
        log_event("[TELEGRAM] Engine iniciado via Telegram desde configuración guardada.", "info")
        await message.reply_text("✅ Engine en marcha con la configuración guardada.")


# =========================================================
# Notificaciones (llamadas desde el engine)
# =========================================================

_app: Optional[Application] = None
_bot_loop: Optional[asyncio.AbstractEventLoop] = None


def notify(message: str) -> None:
    """
    Envía una notificación al chat autorizado.
    Se llama desde el engine u otros módulos.
    Usa siempre el event loop del hilo del bot.
    """
    if _app is None or CHAT_ID is None:
        return

    try:
        loop = _bot_loop
        if loop is None or not loop.is_running():
            log_event("[TELEGRAM] Bot no listo todavía — notificación omitida.", "warning")
            return

        asyncio.run_coroutine_threadsafe(
            _app.bot.send_message(chat_id=CHAT_ID, text=message),
            loop,
        )
    except Exception as exc:
        log_event(f"[TELEGRAM] Error enviando notificación: {exc}", "warning")


# =========================================================
# Handler global de errores
# =========================================================

async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Registra errores del bot sin volcar tracebacks completos en consola."""
    del update

    err = context.error

    if isinstance(err, (NetworkError, TimedOut)):
        log_event(f"[TELEGRAM] Error de red: {err}", "warning")
        return

    log_event(f"[TELEGRAM] Error no controlado: {type(err).__name__}: {err}", "error")


# =========================================================
# Handlers de comandos
# =========================================================

def _build_help_text() -> str:
    """Construye la ayuda visible del bot de Telegram."""
    return "\n".join([
        "🤖 *GRID BOT — AYUDA*",
        "",
        "*Estado y monitorización*",
        "`/status` — estado del engine, precio, órdenes, fills y trailings",
        "`/grid` — niveles del grid y órdenes registradas",
        "`/balance` — saldos disponibles y fondos comprometidos en el grid",
        "`/analyze` — resumen de fills emparejados y beneficio estimado",
        "",
        "*Configuración*",
        "`/config` — muestra la configuración guardada del grid",
        "`/set_config clave valor` — cambia un valor de configuración",
        "`/trailings` — muestra trailing up/down del engine activo",
        "`/trailings up off|on|extended|fixed_quote` — cambia trailing up en caliente",
        "`/trailings down off|on|extended` — cambia trailing down en caliente",
        "",
        "*Arranque, parada y órdenes*",
        "`/start_engine` — previsualiza y arranca el engine con confirmación",
        "`/start_engine recover` — recupera estado previo si existe",
        "`/start_engine fresh` — ignora estado previo y arranca desde cero",
        "`/stop` — detiene el engine; conserva órdenes del exchange",
        "`/add_order` — añade una orden manual guiada por pasos",
        "`/cancel` — cancela una orden real por precio con confirmación",
        "`/confirm` — confirma una acción pendiente",
        "`/abort` — cancela una acción pendiente",
        "",
        "*Fiscal FIFO*",
        "`/taxstatus` — resumen del ledger FIFO fiscal",
        "`/taxlots [limite]` — lista lotes FIFO abiertos",
        "`/taxunmatched [limite]` — lista ventas sin lotes suficientes",
        "`/taxsim precio [cantidad_btc]` — simula una venta FIFO sin modificar archivos",
        "`/taxaddlot fecha precio cantidad_btc [nota]` — importa un lote manual",
        "`/taxreport` — envía los archivos fiscales disponibles",
        "",
        "Las acciones sensibles quedan pendientes hasta responder `/confirm` o `/abort`.",
    ])


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra la lista de comandos disponibles y su uso básico."""
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    await message.reply_text(_build_help_text(), parse_mode="Markdown")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if not _engine_running():
        await message.reply_text("⚪ Engine no está corriendo.")
        return

    eng = _get_engine()
    if eng is None:
        await message.reply_text("⚪ Engine no disponible.")
        return

    snapshot = eng.get_runtime_snapshot()
    current_price = snapshot["current_price"]
    price = f"{current_price:.2f}" if current_price else "N/A"
    lines = [
        "📊 *STATUS*",
        f"Precio actual : `{price} USDC`",
        f"Órdenes activas: `{len(snapshot['active_orders'])}`",
        f"Fills sesión  : `{len(snapshot['fill_history'])}`",
        f"Último fill   : `{snapshot['last_fill_side'] or 'ninguno'}`",
        f"Trailing up   : `{trailing_mode_label(_get_trailing_up_mode(eng))}`",
        f"Trailing down : `{trailing_mode_label(_get_trailing_down_mode(eng))}`",
    ]
    await message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_grid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if not _engine_running():
        await message.reply_text("⚪ Engine no está corriendo.")
        return

    eng = _get_engine()
    if eng is None:
        await message.reply_text("⚪ Engine no disponible.")
        return

    snapshot = eng.get_runtime_snapshot()
    levels = sorted(snapshot["levels"], reverse=True)
    active_orders = snapshot["active_orders"]

    lines = ["📋 *GRID*", "```"]
    for lvl in levels:
        key = _price_key(lvl)
        info = active_orders.get(key)
        if info is not None:
            side = str(info["side"]).upper()
            size = info.get("size")
            oid = str(info["order_id"])
            if oid == "virtual":
                tag = "[V]"
            elif oid == "pending_post_only":
                tag = "[P]"
            elif oid == "pending_manual":
                tag = "[M]"
            else:
                tag = ""
            size_text = _fmt_grid_size(size) if size is not None else "?"
            lines.append(f"{key:>12}  {side:<4} {size_text:<8} {tag}")
        else:
            lines.append(f"{key:>12}  ---  vacío")
    lines.append("```")

    current_price = snapshot["current_price"]
    if current_price:
        lines.append(f"Precio actual: `{current_price:.2f} USDC`")

    await message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    eng = _get_engine() if _engine_running() else None

    try:
        balance_text = format_balances_live(eng)
    except Exception as exc:
        log_event(f"[TELEGRAM] Error consultando balances: {exc}", "error")
        await message.reply_text(f"❌ No se pudieron consultar los balances: {exc}")
        return

    await message.reply_text(f"💰 *BALANCE*\n```\n{balance_text}\n```", parse_mode="Markdown")


async def cmd_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    try:
        cfg = _load_grid_config()
        await message.reply_text(_format_grid_config(cfg), parse_mode="Markdown")
    except Exception as exc:
        log_event(f"[TELEGRAM] Error leyendo configuración: {exc}", "error")
        await message.reply_text(f"❌ No se pudo leer la configuración: {exc}")


async def cmd_set_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    args = context.args or []
    if len(args) < 2:
        await message.reply_text(_set_config_usage(), parse_mode="Markdown")
        return

    key_raw = args[0].strip().lower().replace("-", "_")
    value_raw = " ".join(args[1:]).strip()
    aliases = {
        "below": "levels_below",
        "abajo": "levels_below",
        "levels_below": "levels_below",
        "above": "levels_above",
        "arriba": "levels_above",
        "levels_above": "levels_above",
        "size": "base_size",
        "base_size": "base_size",
        "step": "step_percent",
        "step_percent": "step_percent",
        "tu": "trailing_up",
        "up": "trailing_up",
        "trailing_up": "trailing_up",
        "td": "trailing_down",
        "down": "trailing_down",
        "trailing_down": "trailing_down",
        "budget": "bot_usdc_budget",
        "saldo": "bot_usdc_budget",
        "bot_budget": "bot_usdc_budget",
        "bot_usdc_budget": "bot_usdc_budget",
    }
    key = aliases.get(key_raw)
    if key is None:
        await message.reply_text(_set_config_usage(), parse_mode="Markdown")
        return

    try:
        cfg = _load_grid_config()

        if key in {"levels_below", "levels_above"}:
            parsed_int = int(value_raw)
            if parsed_int < 0:
                raise ValueError("los niveles no pueden ser negativos")
            cfg[key] = parsed_int

        elif key == "base_size":
            parsed_decimal = Decimal(value_raw.replace(",", "."))
            if parsed_decimal <= 0:
                raise ValueError("base_size debe ser mayor que cero")
            cfg[key] = parsed_decimal

        elif key == "step_percent":
            parsed_percent = _parse_percent_value(value_raw)
            if parsed_percent <= 0:
                raise ValueError("step_percent debe ser mayor que cero")
            cfg[key] = parsed_percent

        elif key == "trailing_up":
            parsed_mode = parse_trailing_up_mode(value_raw)
            if parsed_mode is None:
                raise ValueError("trailing_up debe ser off, on, extended o fixed_quote")
            cfg[key] = parsed_mode

        elif key == "trailing_down":
            parsed_mode = parse_trailing_down_mode(value_raw)
            if parsed_mode is None:
                raise ValueError("trailing_down debe ser off, on o extended")
            cfg[key] = parsed_mode

        elif key == "bot_usdc_budget":
            normalized = value_raw.strip().lower()
            if normalized in {"max", "todo", "all"}:
                usdc_balance, _, logs = _read_available_balances_for_telegram()
                for log_item in logs:
                    log_event(f"[TELEGRAM] {log_item['msg']}", log_item.get("level", "info"))
                if usdc_balance is None:
                    raise ValueError("no se pudo leer el USDC disponible")
                cfg[key] = usdc_balance
            else:
                parsed_budget = Decimal(value_raw.replace(",", "."))
                if parsed_budget < 0:
                    raise ValueError("bot_usdc_budget no puede ser negativo")
                cfg[key] = parsed_budget

        _save_grid_config_from_dict(cfg)

    except Exception as exc:
        await message.reply_text(f"❌ Valor inválido para `{key}`: {exc}", parse_mode="Markdown")
        return

    await message.reply_text(
        f"✅ Configuración actualizada: `{key}` = `{cfg[key]}`\n\n" + _format_grid_config(cfg),
        parse_mode="Markdown",
    )


async def cmd_trailings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if not _engine_running():
        await message.reply_text("⚪ El engine no está corriendo.")
        return

    eng = _get_engine()
    if eng is None:
        await message.reply_text("⚪ Engine no disponible.")
        return

    raw_args = context.args or []
    args = [arg.strip().lower() for arg in raw_args]
    if not args:
        await message.reply_text(_format_trailing_status(eng), parse_mode="Markdown")
        return

    if len(args) != 2 or args[0] not in {"up", "down"}:
        await message.reply_text(
            "Uso:\n"
            "`/trailings`\n"
            "`/trailings up off|on|extended|fixed_quote`\n"
            "`/trailings down off|on|extended`",
            parse_mode="Markdown",
        )
        return

    target, value = args

    if target == "up":
        mode = _parse_trailing_up_mode(value)
        if mode is None:
            await message.reply_text("Valor inválido. Usa `off`, `on`, `extended` o `fixed_quote`.", parse_mode="Markdown")
            return
        ok, response = _apply_trailing_config(eng, trailing_up=mode)

    else:
        mode = _parse_trailing_down_mode(value)
        if mode is None:
            await message.reply_text("Valor inválido. Usa `off`, `on` o `extended`.", parse_mode="Markdown")
            return
        ok, response = _apply_trailing_config(eng, trailing_down=mode)

    if not ok:
        await message.reply_text(f"❌ {response}")
        return

    log_event("[TELEGRAM] Configuración de trailings modificada via Telegram.", "info")
    await message.reply_text(response, parse_mode="Markdown")


async def cmd_start_engine(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if _engine_running():
        await message.reply_text("⚠️ El engine ya está corriendo.")
        return

    args = [arg.strip().lower() for arg in (context.args or [])]
    cfg = _load_grid_config()

    valid_args = {"recover", "recuperar", "fresh", "new", "nuevo", "desde_cero", "reset"}
    if args and args[0] not in valid_args:
        await message.reply_text(
            "Uso:\n"
            "`/start_engine`\n"
            "`/start_engine recover`\n"
            "`/start_engine fresh`",
            parse_mode="Markdown",
        )
        return

    force_fresh = bool(args and args[0] in {"fresh", "new", "nuevo", "desde_cero", "reset"})
    force_recover = bool(args and args[0] in {"recover", "recuperar"})
    has_state = STATE_PATH.exists()

    if has_state and not force_fresh:
        _state.pending_confirm = (
            "start_engine",
            {
                "recover_state": True,
                "initial_price": None,
                "cfg": cfg,
            },
        )
        extra = "" if force_recover else "\n\nPara ignorar el estado y arrancar desde cero usa `/start_engine fresh`."
        await message.reply_text(
            "📂 Estado previo detectado.\n"
            "Responde /confirm para recuperar el grid o /abort para cancelar."
            f"{extra}",
            parse_mode="Markdown",
        )
        return

    if force_recover and not has_state:
        await message.reply_text("⚪ No hay estado previo que recuperar.")
        return

    current_price, logs = get_current_price()
    for log_item in logs:
        log_event(f"[TELEGRAM] {log_item['msg']}", log_item.get("level", "info"))

    if current_price is None:
        await message.reply_text("❌ No se pudo obtener precio actual para iniciar el grid.")
        return

    ok, preflight_text = _build_start_preflight_text(cfg, current_price)
    await message.reply_text(preflight_text, parse_mode="Markdown")

    if not ok:
        return

    _state.pending_confirm = (
        "start_engine",
        {
            "recover_state": False,
            "initial_price": current_price,
            "cfg": cfg,
        },
    )

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if not _engine_running():
        await message.reply_text("⚪ El engine no está corriendo.")
        return

    _state.pending_confirm = ("stop", {})
    await message.reply_text(
        "⚠️ ¿Detener el engine?\n"
        "Responde /confirm para confirmar o /abort para cancelar.\n\n"
        "Las órdenes se conservarán en el exchange."
    )


async def cmd_add_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if not _engine_running():
        await message.reply_text("⚪ El engine no está corriendo.")
        return

    _state.add_order_step = "price"
    _state.add_order_data = {}
    await message.reply_text("💰 Introduce el precio de la orden:")


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if not _engine_running():
        await message.reply_text("⚪ El engine no está corriendo.")
        return

    eng = _get_engine()
    if eng is None:
        await message.reply_text("⚪ Engine no disponible.")
        return

    active_orders = eng.get_runtime_snapshot()["active_orders"]
    real_orders = {
        key: info
        for key, info in active_orders.items()
        if str(info["order_id"]) not in {"virtual", "pending_post_only", "pending_manual"}
    }

    if not real_orders:
        await message.reply_text("No hay órdenes activas para cancelar.")
        return

    lines = ["📋 *Órdenes activas* — responde con el precio a cancelar:\n```"]
    for key, info in sorted(real_orders.items(), key=lambda item: Decimal(item[0]), reverse=True):
        lines.append(f"{key:>12}  {str(info['side']).upper()}")
    lines.append("```")
    await message.reply_text("\n".join(lines), parse_mode="Markdown")

    _state.pending_confirm = ("cancel_select", {})


async def cmd_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if _state.add_order_step == "confirm":
        data = _state.add_order_data
        _state.add_order_step = None
        _state.add_order_data = {}

        eng = _get_engine()
        if eng is None:
            await message.reply_text("⚪ Engine no disponible.")
            return

        price = cast(Decimal, data["price"])
        side = cast(str, data["side"])
        size = cast(Decimal, data["size"])
        key = _price_key(price)

        order_id, logs, error_msg = eng.place_manual_order(price, side, size)
        for log_item in logs:
            log_event(f"[TELEGRAM] {log_item['msg']}", log_item["level"])

        if error_msg:
            await message.reply_text(f"⚠️ {error_msg}")
            return

        if not order_id:
            await message.reply_text("❌ No se pudo colocar la orden. Revisa el log.")
            return

        log_event(f"[TELEGRAM] Orden manual {side.upper()} registrada en {key} -> {order_id}", "info")
        await message.reply_text(f"✅ Orden {side.upper()} registrada en `{key}`.", parse_mode="Markdown")
        return

    if _state.pending_confirm is None:
        await message.reply_text("No hay ninguna acción pendiente de confirmar.")
        return

    action, kwargs = _state.pending_confirm
    _state.pending_confirm = None

    if action == "start_engine":
        if _engine_running():
            await message.reply_text("⚠️ El engine ya está corriendo.")
            return

        await _execute_start_engine(
            message,
            recover_state=bool(kwargs.get("recover_state", False)),
            initial_price=cast(Optional[Decimal], kwargs.get("initial_price")),
            cfg=cast(Optional[dict[str, Any]], kwargs.get("cfg")),
        )
        return

    if action == "stop":
        engine = _get_engine()
        thread = _get_engine_thread()
        if engine is None or thread is None:
            await message.reply_text("⚪ Engine no disponible.")
            return

        await message.reply_text("🛑 Deteniendo engine...")
        engine.stop()
        thread.join(timeout=10)
        _state.engine = None
        _state.engine_thread = None
        log_event("[TELEGRAM] Engine detenido via Telegram.", "info")
        await message.reply_text("✅ Engine detenido. Órdenes conservadas.")
        return

    if action == "cancel_order":
        key = str(kwargs["key"])
        order_id = str(kwargs["order_id"])
        eng = _get_engine()
        if eng is None:
            await message.reply_text("⚪ Engine no disponible.")
            return

        ok, logs, error_msg = eng.cancel_order_by_key(key, expected_order_id=order_id)
        for log_item in logs:
            log_event(f"[TELEGRAM] {log_item['msg']}", log_item["level"])

        if not ok:
            await message.reply_text(error_msg or "❌ No se pudo cancelar la orden.")
            return

        log_event(f"[TELEGRAM] Orden cancelada en {key} via Telegram.", "info")
        await message.reply_text(f"✅ Orden en {key} cancelada.")
        return

    await message.reply_text("No hay ninguna acción pendiente de confirmar.")


async def cmd_abort(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if _state.add_order_step is not None:
        _state.add_order_step = None
        _state.add_order_data = {}
        await message.reply_text("❌ Añadir orden cancelado.")
        return

    if _state.pending_confirm is not None:
        _state.pending_confirm = None
        await message.reply_text("❌ Acción cancelada.")
        return

    await message.reply_text("No hay ninguna acción pendiente.")


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    fills_path = Path("fills.csv")
    if not fills_path.exists():
        await message.reply_text("⚠️ No se encontró fills.csv")
        return

    try:
        from analyze_fills import detect_step, load_fills, load_state_step, pair_fills

        fills = load_fills(fills_path)
        step = load_state_step(Path("grid_state.json")) or detect_step(fills)
        if step == 0:
            await message.reply_text("⚠️ No se pudo detectar el step del grid.")
            return

        pairs, open_buys = pair_fills(fills, step)

        if not pairs:
            await message.reply_text(
                f"📊 *ANALYZE FILLS*\n"
                f"Step detectado: `{step} USDC`\n"
                f"Sin pares completos todavía.\n"
                f"BUYs sin emparejar: `{len(open_buys)}`",
                parse_mode="Markdown",
            )
            return

        total_profit = sum(Decimal(p["profit_usdc"]) for p in pairs)
        avg_profit = total_profit / len(pairs)

        days: dict[str, list[Decimal]] = {}
        for pair in pairs:
            day = str(pair["sell_ts"])[:10]
            days.setdefault(day, []).append(Decimal(pair["profit_usdc"]))

        current_month = datetime.now().strftime("%Y-%m")
        months: dict[str, list[Decimal]] = {}
        month_day_profits: dict[str, dict[str, list[Decimal]]] = {}
        for day, profits in days.items():
            month = day[:7]
            months.setdefault(month, []).extend(profits)
            month_day_profits.setdefault(month, {})[day] = profits

        lines = [
            "📊 *ANALYZE FILLS*",
            f"Step detectado  : `{step} USDC`",
            f"Pares completos : `{len(pairs)}`",
            f"BUYs abiertos   : `{len(open_buys)}`",
            f"Beneficio total : `{total_profit:.4f} USDC`",
            f"Media por par   : `{avg_profit:.4f} USDC`",
            "",
            "*Desglose por mes/día:*",
            "```",
        ]
        for month in sorted(months):
            if month == current_month:
                for day in sorted(month_day_profits[month]):
                    profits = month_day_profits[month][day]
                    day_total = sum(profits)
                    day_avg = day_total / len(profits)
                    lines.append(f"{day}  {len(profits):>4} pares  {day_total:>8.4f}$  {day_avg:.4f}$/par")
            else:
                month_total = sum(months[month])
                month_avg = month_total / len(months[month])
                lines.append(f"{month}  {len(months[month]):>4} pares  {month_total:>8.4f}$  {month_avg:.4f}$/par")
        lines.append("```")

        await message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as exc:
        await message.reply_text(f"❌ Error en analyze: {exc}")


def _default_taxsim_quantity() -> Optional[Decimal]:
    """Cantidad por defecto para /taxsim: base_size del engine si esta activo."""
    eng = _get_engine() if _engine_running() else None
    if eng is None:
        return None
    try:
        return Decimal(str(eng.get_runtime_snapshot()["base_size"]))
    except Exception:
        return None


def _normalize_tax_timestamp(raw: str) -> str:
    """Normaliza fechas de /taxaddlot a texto estable para el ledger FIFO."""
    text = raw.strip()
    if not text:
        raise ValueError("Fecha vacia.")

    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d 00:00:00")

    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text} 00:00:00"

    # Valida ISO o formato con espacio, pero conserva segundos legibles.
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


async def cmd_taxstatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    try:
        text = build_tax_status()
    except Exception as exc:
        await message.reply_text(f"❌ Error fiscal FIFO: {exc}")
        return

    await message.reply_text(f"📒 *FIFO FISCAL*\n```\n{text}\n```", parse_mode="Markdown")


async def cmd_taxlots(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    args = context.args or []
    limit = 20
    if args:
        try:
            limit = max(1, min(100, int(args[0])))
        except Exception:
            await message.reply_text("Uso: `/taxlots [limite]`", parse_mode="Markdown")
            return

    try:
        text = build_tax_lots_text(limit=limit)
    except Exception as exc:
        await message.reply_text(f"❌ Error leyendo lotes FIFO: {exc}")
        return

    await message.reply_text(f"📦 *LOTES FIFO*\n```\n{text}\n```", parse_mode="Markdown")


async def cmd_taxunmatched(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    args = context.args or []
    limit = 20
    if args:
        try:
            limit = max(1, min(100, int(args[0])))
        except Exception:
            await message.reply_text("Uso: `/taxunmatched [limite]`", parse_mode="Markdown")
            return

    try:
        text = build_tax_unmatched_text(limit=limit)
    except Exception as exc:
        await message.reply_text(f"❌ Error leyendo incidencias FIFO: {exc}")
        return

    await message.reply_text(f"⚠️ *INCIDENCIAS FIFO*\n```\n{text}\n```", parse_mode="Markdown")


async def cmd_taxsim(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    args = context.args or []
    if not args:
        await message.reply_text(
            "Uso: `/taxsim precio [cantidad_btc]`\n"
            "Ejemplo: `/taxsim 75000 0.0005`\n"
            "Si omites cantidad y el engine esta activo, se usa el base_size.",
            parse_mode="Markdown",
        )
        return

    try:
        price = Decimal(args[0])
        if len(args) >= 2:
            quantity = Decimal(args[1])
        else:
            quantity = _default_taxsim_quantity()
            if quantity is None:
                await message.reply_text(
                    "Indica cantidad BTC o arranca el engine para usar base_size.\n"
                    "Uso: `/taxsim precio cantidad_btc`",
                    parse_mode="Markdown",
                )
                return
        text = simulate_fifo_sell(price=price, quantity=quantity)
    except Exception as exc:
        await message.reply_text(f"❌ Error simulando FIFO: {exc}")
        return

    await message.reply_text(f"🧮 *SIMULACIÓN FIFO*\n```\n{text}\n```", parse_mode="Markdown")


async def cmd_taxaddlot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    args = context.args or []
    if len(args) < 3:
        await message.reply_text(
            "Uso: `/taxaddlot fecha precio cantidad_btc [nota]`\n"
            "Fecha: `YYYYMMDD`, `YYYY-MM-DD` o `YYYY-MM-DDTHH:MM:SS`\n"
            "Ejemplo: `/taxaddlot 20250110 43000 0.001 compra_previa`",
            parse_mode="Markdown",
        )
        return

    try:
        buy_ts = _normalize_tax_timestamp(args[0])
        buy_price = Decimal(args[1])
        quantity = Decimal(args[2])
        note = " ".join(args[3:]).strip() or "telegram"
        if buy_price <= 0 or quantity <= 0:
            raise ValueError("precio y cantidad deben ser mayores que cero")
        lot = import_manual_lot(
            buy_ts=buy_ts,
            buy_price=buy_price,
            quantity=quantity,
            note=note,
        )
    except Exception as exc:
        await message.reply_text(f"❌ No se pudo importar el lote: {exc}")
        return

    await message.reply_text(
        "✅ Lote FIFO importado\n"
        f"`{lot.buy_ts}`\n"
        f"`{lot.qty_total} BTC @ {lot.buy_price} USDC`\n"
        f"ID: `{lot.source_order_id}`",
        parse_mode="Markdown",
    )


async def cmd_taxreport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    paths = [TAX_LOTS_PATH, TAX_SALES_PATH, TAX_UNMATCHED_SALES_PATH]
    existing = [path for path in paths if path.exists()]
    if not existing:
        await message.reply_text("No hay archivos fiscales todavía.")
        return

    for path in existing:
        try:
            with path.open("rb") as fh:
                await message.reply_document(document=fh, filename=path.name)
        except Exception as exc:
            await message.reply_text(f"❌ No se pudo enviar {path.name}: {exc}")



async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Gestiona los flujos de conversación multi-paso (add_order, cancel)."""
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None or message.text is None:
        return

    text = message.text.strip()

    if _state.add_order_step == "price":
        try:
            price = Decimal(text)
            _state.add_order_data["price"] = price
            _state.add_order_step = "side"
            await message.reply_text("Lado (buy / sell):")
        except Exception:
            await message.reply_text("Precio inválido. Introduce un número:")
        return

    if _state.add_order_step == "side":
        if text.lower() not in ("buy", "sell"):
            await message.reply_text("Debe ser buy o sell:")
            return
        _state.add_order_data["side"] = text.lower()
        _state.add_order_step = "size"
        eng = _get_engine()
        if eng is None:
            await message.reply_text("⚪ Engine no disponible.")
            return
        default = fmt_amount(eng.get_runtime_snapshot()["base_size"])
        await message.reply_text(f"Tamaño BTC (Enter para {default}):")
        return

    if _state.add_order_step == "size":
        eng = _get_engine()
        if eng is None:
            await message.reply_text("⚪ Engine no disponible.")
            return
        default_size = eng.get_runtime_snapshot()["base_size"]
        try:
            size = Decimal(text) if text else default_size
        except Exception:
            await message.reply_text("Tamaño inválido:")
            return

        _state.add_order_data["size"] = size
        _state.add_order_step = "confirm"

        data = _state.add_order_data
        await message.reply_text(
            f"¿Confirmar orden?\n"
            f"`{str(data['side']).upper()} {fmt_amount(cast(Decimal, data['size']))} BTC @ {_price_key(cast(Decimal, data['price']))} USDC`\n\n"
            f"Responde /confirm o /abort",
            parse_mode="Markdown",
        )
        return

    if _state.add_order_step == "confirm":
        await message.reply_text("Usa /confirm para confirmar o /abort para cancelar.")
        return

    if _state.pending_confirm is not None:
        action, _ = _state.pending_confirm
        if action == "cancel_select":
            eng = _get_engine()
            if eng is None:
                await message.reply_text("⚪ Engine no disponible.")
                return
            try:
                target_key = _price_key(Decimal(text))
            except Exception:
                await message.reply_text("Precio inválido.")
                return

            info = eng.get_order_info(target_key)
            if info is None:
                await message.reply_text(f"No hay orden en {target_key}.")
                _state.pending_confirm = None
                return

            if str(info["order_id"]) in {"virtual", "pending_post_only", "pending_manual"}:
                await message.reply_text("Esa orden no se puede cancelar desde aquí.")
                _state.pending_confirm = None
                return

            _state.pending_confirm = (
                "cancel_order",
                {"key": target_key, "order_id": info["order_id"]},
            )
            await message.reply_text(
                f"¿Cancelar orden {str(info['side']).upper()} en `{target_key}`?\n"
                f"Responde /confirm o /abort",
                parse_mode="Markdown",
            )


# =========================================================
# Arranque del bot
# =========================================================

def start_telegram_bot() -> None:
    """
    Arranca el bot de Telegram en un hilo separado.
    Llama a esta función desde cli.py al iniciar el programa.
    """
    token = _read_token()
    if not token:
        log_event("[TELEGRAM] Bot no iniciado — token no disponible.", "warning")
        return

    def _run() -> None:
        global _app, _bot_loop

        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("telegram").setLevel(logging.WARNING)

        _bot_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_bot_loop)

        app = Application.builder().token(token).build()
        _app = app

        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("grid", cmd_grid))
        app.add_handler(CommandHandler("balance", cmd_balance))
        app.add_handler(CommandHandler("config", cmd_config))
        app.add_handler(CommandHandler("set_config", cmd_set_config))
        app.add_handler(CommandHandler("trailings", cmd_trailings))
        app.add_handler(CommandHandler("start_engine", cmd_start_engine))
        app.add_handler(CommandHandler("stop", cmd_stop))
        app.add_handler(CommandHandler("add_order", cmd_add_order))
        app.add_handler(CommandHandler("cancel", cmd_cancel))
        app.add_handler(CommandHandler("confirm", cmd_confirm))
        app.add_handler(CommandHandler("abort", cmd_abort))
        app.add_handler(CommandHandler("analyze", cmd_analyze))
        app.add_handler(CommandHandler("taxstatus", cmd_taxstatus))
        app.add_handler(CommandHandler("taxlots", cmd_taxlots))
        app.add_handler(CommandHandler("taxunmatched", cmd_taxunmatched))
        app.add_handler(CommandHandler("taxsim", cmd_taxsim))
        app.add_handler(CommandHandler("taxaddlot", cmd_taxaddlot))
        app.add_handler(CommandHandler("taxreport", cmd_taxreport))
        app.add_error_handler(telegram_error_handler)
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

        log_event("[TELEGRAM] Bot iniciado.", "info")
        app.run_polling(stop_signals=None)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
