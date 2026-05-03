"""
telegram_bot.py — Interfaz de control y monitorización via Telegram.

Comandos disponibles:
  /status    — precio actual, órdenes activas, fills de sesión y trailings
  /grid      — niveles del grid con órdenes
  /balance   — balances disponibles y fondos comprometidos en el grid
  /trailings — muestra o configura trailing up/down
  /start_engine  — arranca el engine (recupera estado si existe)
  /stop      — detiene el engine (requiere confirmación)
  /add_order — añade una orden manual (guiado por pasos)
  /cancel    — cancela una orden por precio (requiere confirmación)

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
from decimal import Decimal
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

from api import _price_key, fmt_amount, get_current_price
from cli import format_balances_live
from config import (
    DEFAULT_BASE_SIZE,
    DEFAULT_GRID_LEVELS_ABOVE,
    DEFAULT_GRID_LEVELS_BELOW,
    DEFAULT_STEP_PERCENT,
    STATE_PATH,
)
from logger import log_event
from trailing import (
    normalize_trailing_down_mode,
    parse_trailing_down_mode,
    trailing_down_mode_label,
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


def _normalize_trailing_down_mode(value: object) -> str:
    """Normaliza el modo de trailing down usado por el engine."""
    return normalize_trailing_down_mode(value)


def _trailing_mode_label(mode: str) -> str:
    """Devuelve la etiqueta mostrada al usuario para trailing down."""
    return trailing_down_mode_label(mode)


def _get_trailing_down_mode(engine: "GridEngine") -> str:
    """Lee el modo de trailing down, soportando estados antiguos booleanos."""
    raw_mode = getattr(engine, "trailing_down_mode", None)
    if raw_mode is None:
        raw_mode = getattr(engine, "trailing_down_enabled", False)
    return _normalize_trailing_down_mode(raw_mode)


def _format_trailing_status(engine: "GridEngine") -> str:
    """Construye el bloque de estado de trailings para el comando /trailings."""
    up_enabled = bool(getattr(engine, "trailing_up_enabled", False))
    down_mode = _get_trailing_down_mode(engine)

    return (
        "⚙️ *TRAILINGS*\n"
        f"Trailing up   : `{'ON' if up_enabled else 'OFF'}`\n"
        f"Trailing down : `{_trailing_mode_label(down_mode)}`\n\n"
        "Cambiar configuración:\n"
        "`/trailings up on` o `/trailings up off`\n"
        "`/trailings down off`, `/trailings down on` o `/trailings down extended`"
    )


def _parse_on_off(value: str) -> Optional[bool]:
    """Parsea variantes comunes de on/off usadas en comandos de Telegram."""
    normalized = value.strip().lower()
    if normalized in {"on", "true", "1", "si", "sí", "s", "enable", "enabled"}:
        return True
    if normalized in {"off", "false", "0", "no", "n", "disable", "disabled"}:
        return False
    return None


def _parse_trailing_down_mode(value: str) -> Optional[str]:
    """Parsea un modo de trailing down recibido por comando."""
    return parse_trailing_down_mode(value)


def _apply_trailing_config(
    engine: "GridEngine",
    *,
    trailing_up: Optional[bool] = None,
    trailing_down: Optional[str] = None,
) -> tuple[bool, str]:
    """Aplica cambios de trailing en caliente usando GridEngine.set_trailing()."""
    if not hasattr(engine, "set_trailing"):
        return False, "El engine no expone set_trailing()."

    current_up = bool(getattr(engine, "trailing_up_enabled", False))
    current_down = _get_trailing_down_mode(engine)

    new_up = current_up if trailing_up is None else trailing_up
    new_down = current_down if trailing_down is None else _normalize_trailing_down_mode(trailing_down)

    if new_down not in {"off", "on", "extended"}:
        return False, "Modo de trailing down inválido."

    engine.set_trailing(new_up, new_down)
    return True, _format_trailing_status(engine)


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
        f"Trailing up   : `{'ON' if bool(getattr(eng, 'trailing_up_enabled', False)) else 'OFF'}`",
        f"Trailing down : `{_trailing_mode_label(_get_trailing_down_mode(eng))}`",
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
            oid = str(info["order_id"])
            if oid == "virtual":
                tag = "[V]"
            elif oid == "pending_post_only":
                tag = "[P]"
            elif oid == "pending_manual":
                tag = "[M]"
            else:
                tag = ""
            lines.append(f"{key:>12}  {side:<4} {tag}")
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
            "`/trailings up on|off`\n"
            "`/trailings down off|on|extended`",
            parse_mode="Markdown",
        )
        return

    target, value = args

    if target == "up":
        parsed = _parse_on_off(value)
        if parsed is None:
            await message.reply_text("Valor inválido. Usa `on` u `off`.", parse_mode="Markdown")
            return
        ok, response = _apply_trailing_config(eng, trailing_up=parsed)

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
    del context
    if not _authorized(update):
        return

    message = _get_message(update)
    if message is None:
        return

    if _engine_running():
        await message.reply_text("⚠️ El engine ya está corriendo.")
        return

    from engine import GridEngine

    recover_state = STATE_PATH.exists()
    initial_price: Optional[Decimal] = None

    if recover_state:
        await message.reply_text("📂 Estado previo detectado — recuperando grid...")
    else:
        current_price, logs = get_current_price()
        for log_item in logs:
            log_event(f"[TELEGRAM] {log_item['msg']}", log_item.get("level", "info"))

        if current_price is None:
            await message.reply_text("❌ No se pudo obtener precio actual para iniciar el grid.")
            return

        initial_price = current_price
        await message.reply_text(
            f"🚀 Iniciando grid desde cero con precio inicial `{_price_key(initial_price)} USDC`...",
            parse_mode="Markdown",
        )

    engine = GridEngine(
        levels_below=DEFAULT_GRID_LEVELS_BELOW,
        levels_above=DEFAULT_GRID_LEVELS_ABOVE,
        step_percent=DEFAULT_STEP_PERCENT,
        base_size=DEFAULT_BASE_SIZE,
        initial_price=initial_price,
    )

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

    log_event("[TELEGRAM] Engine iniciado via Telegram.", "info")
    await message.reply_text("✅ Engine en marcha.")

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
        from analyze_fills import detect_step, load_fills, pair_fills

        fills = load_fills(fills_path)
        step = detect_step(fills)
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

        lines = [
            "📊 *ANALYZE FILLS*",
            f"Step detectado  : `{step} USDC`",
            f"Pares completos : `{len(pairs)}`",
            f"BUYs abiertos   : `{len(open_buys)}`",
            f"Beneficio total : `{total_profit:.4f} USDC`",
            f"Media por par   : `{avg_profit:.4f} USDC`",
            "",
            "*Desglose por día:*",
            "```",
        ]
        for day in sorted(days):
            profits = days[day]
            day_total = sum(profits)
            day_avg = day_total / len(profits)
            lines.append(f"{day}  {len(profits):>4} pares  {day_total:>8.4f}$  {day_avg:.4f}$/par")
        lines.append("```")

        await message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as exc:
        await message.reply_text(f"❌ Error en analyze: {exc}")


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

        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("grid", cmd_grid))
        app.add_handler(CommandHandler("balance", cmd_balance))
        app.add_handler(CommandHandler("trailings", cmd_trailings))
        app.add_handler(CommandHandler("start_engine", cmd_start_engine))
        app.add_handler(CommandHandler("stop", cmd_stop))
        app.add_handler(CommandHandler("add_order", cmd_add_order))
        app.add_handler(CommandHandler("cancel", cmd_cancel))
        app.add_handler(CommandHandler("confirm", cmd_confirm))
        app.add_handler(CommandHandler("abort", cmd_abort))
        app.add_handler(CommandHandler("analyze", cmd_analyze))
        app.add_error_handler(telegram_error_handler)
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

        log_event("[TELEGRAM] Bot iniciado.", "info")
        app.run_polling(stop_signals=None)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
