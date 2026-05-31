"""
ARC RailGuard — Этап 3 + донаты CloudTips + /help
@barysh_pereezd_bot

Добавлено к Этапу 1.6:
- База SQLite (railguard.db) — события переживают перезапуск
- /log [N]   — последние события
- /stats     — за сегодня: закрытий, средняя/суммарная длительность
- /longest   — самые долгие закрытия за всё время
- /peak      — в какие часы чаще закрывается
- Раздел «📊 Статистика» в админ-панели на кнопках

ESP32 перезаливать НЕ нужно.
"""

import os
import uuid
import sqlite3
import asyncio
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from fastapi import FastAPI, Query, Header, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Все секреты читаются из окружения (на Railway — Variables), с fallback для локального запуска
ADMIN_ID = int(os.getenv("ADMIN_ID", "1066928889"))
DEVICE_SECRET = os.getenv("DEVICE_SECRET", "")  # пусто = проверка выключена (локально)
PORT = int(os.getenv("PORT", "8000"))           # Railway сам подставит свой порт

STEP_BUFFER = 2.0
MIN_BUFFER_GAP = 4.0

# Путь к файлу базы данных (создастся рядом с bot.py)
DB_PATH = os.getenv("DB_PATH",
                     os.path.join(os.path.dirname(os.path.abspath(__file__)), "railguard.db"))

logging.basicConfig(
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

CROSSING_CODES = {"g": "Гурьевский", "c": "Центральный"}
CODE_BY_NAME = {"Гурьевский": "g", "Центральный": "c"}

# ─── База данных ────────────────────────────────────────────────────────────

def init_db():
    """Создаёт таблицу событий если её ещё нет."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crossing TEXT NOT NULL,
            state TEXT NOT NULL,           -- новое состояние: open/closed
            prev_state TEXT,               -- предыдущее состояние
            prev_duration REAL,            -- сколько длилось предыдущее (сек)
            ts TEXT NOT NULL               -- ISO-время события (локальное)
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"База данных готова: {DB_PATH}")


def record_event(crossing: str, new_state: str, prev_state: str,
                 prev_duration: float, ts: datetime):
    """Записывает событие смены статуса в базу."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO events (crossing, state, prev_state, prev_duration, ts) VALUES (?,?,?,?,?)",
        (crossing, new_state, prev_state, prev_duration, ts.isoformat())
    )
    conn.commit()
    conn.close()


def db_recent_events(n: int = 10):
    """Последние n событий (для /log)."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT crossing, state, prev_state, prev_duration, ts FROM events ORDER BY id DESC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close()
    return rows


def db_today_stats(crossing: str):
    """
    Статистика закрытий за сегодня для переезда.
    Закрытие считается завершённым когда пришло 'open' после 'closed'.
    Возвращает (count, avg_sec, sum_sec).
    """
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        """SELECT COUNT(*), AVG(prev_duration), SUM(prev_duration)
           FROM events
           WHERE crossing=? AND state='open' AND prev_state='closed'
                 AND date(ts)=?""",
        (crossing, today)
    ).fetchone()
    conn.close()
    count = row[0] or 0
    avg = row[1] or 0
    total = row[2] or 0
    return count, avg, total


def db_longest(crossing: str, n: int = 5):
    """Самые долгие завершённые закрытия за всё время."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """SELECT prev_duration, ts FROM events
           WHERE crossing=? AND state='open' AND prev_state='closed'
           ORDER BY prev_duration DESC LIMIT ?""",
        (crossing, n)
    ).fetchall()
    conn.close()
    return rows


def db_peak_hours(crossing: str, n: int = 5):
    """В какие часы чаще НАЧИНАЛИСЬ закрытия (событие state='closed')."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """SELECT strftime('%H', ts) AS hour, COUNT(*) AS cnt
           FROM events
           WHERE crossing=? AND state='closed'
           GROUP BY hour ORDER BY cnt DESC LIMIT ?""",
        (crossing, n)
    ).fetchall()
    conn.close()
    return rows


def format_duration(sec: float) -> str:
    """Секунды → человеко-читаемо: '5 мин 12 сек' / '1 ч 7 мин'."""
    sec = int(sec)
    if sec < 60:
        return f"{sec} сек"
    if sec < 3600:
        m = sec // 60
        s = sec % 60
        return f"{m} мин {s} сек" if s else f"{m} мин"
    h = sec // 3600
    m = (sec % 3600) // 60
    return f"{h} ч {m} мин" if m else f"{h} ч"


# ─── Состояние переездов ───────────────────────────────────────────────────
now = datetime.now()

def _fresh(status, mins):
    return {
        "status": status,
        "since": now - timedelta(minutes=mins),
        "last_heartbeat": now,
        "offline": False,
        "last_angle": None,
        "last_angle_state": None,
        "closed_th": None,
        "open_th": None,
        "measured_closed": None,
        "measured_open": None,
    }

state = {
    "Центральный": _fresh("open", 14),
    "Гурьевский": _fresh("closed", 7),
}

HEARTBEAT_TIMEOUT = timedelta(minutes=10)

active_messages: dict[int, int] = {}
bot_app: Application = None
awaiting_input: dict[int, str] = {}

command_queue: dict[str, list] = {"Центральный": [], "Гурьевский": []}
pending_commands: dict[str, dict] = {}


def queue_command(crossing, cmd_type, params="", edit_chat_id=None,
                  edit_message_id=None, silent=False, save_as=None) -> str:
    cmd_id = uuid.uuid4().hex[:8]
    command_queue[crossing].append({"id": cmd_id, "type": cmd_type, "params": params})
    pending_commands[cmd_id] = {
        "created": datetime.now(), "type": cmd_type, "crossing": crossing,
        "edit_chat_id": edit_chat_id, "edit_message_id": edit_message_id,
        "silent": silent, "save_as": save_as,
    }
    logger.info(f"Команда в очередь {crossing}: {cmd_type} (id={cmd_id})")
    return cmd_id


# ─── Статусный текст ────────────────────────────────────────────────────────

def get_minutes(since: datetime) -> int:
    return int((datetime.now() - since).total_seconds() // 60)


def build_status_text() -> str:
    lines = ["🚦 Барыш — переезды прямо сейчас\n"]
    for name, data in state.items():
        if data["offline"]:
            lines.append(f"⚠️ {name}")
            lines.append(f"НЕТ СВЯЗИ · {get_minutes(data['last_heartbeat'])} мин\n")
        else:
            minutes = get_minutes(data["since"])
            if data["status"] == "open":
                emoji = "✅"; status_text = "ОТКРЫТ"
            else:
                emoji = "⛔"; status_text = "ЗАКРЫТ"
            lines.append(f"{emoji} {name}")
            lines.append(f"{status_text} · {minutes} мин\n")
    return "\n".join(lines)


SUPPORT_URL = "https://pay.cloudtips.ru/p/4fff8826"

def build_keyboard() -> InlineKeyboardMarkup:
    # Прямая ссылка — открывает оплату в один тап, бот ничего не присылает
    return InlineKeyboardMarkup([[InlineKeyboardButton("☕ Поддержать", url=SUPPORT_URL)]])


# ─── Админ-панель ───────────────────────────────────────────────────────────

def build_admin_menu_text() -> str:
    return ("🔧 Админ-панель ARC RailGuard\n\n"
            "Выбери переезд для калибровки или раздел.")


def build_admin_menu_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for code, name in CROSSING_CODES.items():
        rows.append([InlineKeyboardButton(f"📐 {name}", callback_data=f"adm:cal:{code}")])
        rows.append([
            InlineKeyboardButton("🏓 Пинг", callback_data=f"adm:png:{code}"),
            InlineKeyboardButton("🔄 Перезагрузка", callback_data=f"adm:rbt:{code}"),
        ])
    rows.append([InlineKeyboardButton("📊 Статистика", callback_data="adm:stat")])
    rows.append([InlineKeyboardButton("🚦 Статус переездов", callback_data="adm:sts")])
    return InlineKeyboardMarkup(rows)


def build_stat_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 Сегодня", callback_data="adm:st_today"),
            InlineKeyboardButton("🏆 Топ закрытий", callback_data="adm:st_long"),
        ],
        [
            InlineKeyboardButton("⏰ Пиковые часы", callback_data="adm:st_peak"),
            InlineKeyboardButton("📜 Лог событий", callback_data="adm:st_log"),
        ],
        [InlineKeyboardButton("◀️ Назад", callback_data="adm:menu")],
    ])


def build_calib_text(crossing: str) -> str:
    d = state[crossing]
    lines = [f"📐 Калибровка · {crossing}\n"]
    if d["last_angle"] is None:
        lines.append("Угол сейчас: ⏳ запрашиваю...")
    else:
        st = d["last_angle_state"]
        st_ru = {"open": "ОТКРЫТ ✅", "closed": "ЗАКРЫТ ⛔", "buffer": "буфер"}.get(st, st)
        lines.append(f"Угол сейчас: {d['last_angle']}° → {st_ru}")
    lines.append("")
    mc, mo = d["measured_closed"], d["measured_open"]
    lines.append("Замеры:")
    lines.append(f"• закрыт: {mc}°" if mc is not None else "• закрыт: — не задан")
    lines.append(f"• открыт: {mo}°" if mo is not None else "• открыт: — не задан")
    lines.append("")
    if d["closed_th"] is not None:
        gap = round(d["open_th"] - d["closed_th"], 1)
        lines.append("Пороги:")
        lines.append(f"закрыт < {d['closed_th']}°")
        lines.append(f"открыт > {d['open_th']}°")
        lines.append(f"буфер: {d['closed_th']}–{d['open_th']}° (ширина {gap}°)")
    else:
        lines.append("Пороги: — нажми «Обновить угол»")
    return "\n".join(lines)


def build_calib_keyboard(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Обновить угол", callback_data=f"adm:cal:{code}")],
        [InlineKeyboardButton("📍 Зафиксировать ЗАКРЫТ", callback_data=f"adm:mc:{code}")],
        [InlineKeyboardButton("📍 Зафиксировать ОТКРЫТ", callback_data=f"adm:mo:{code}")],
        [InlineKeyboardButton("⚙️ Рассчитать пороги", callback_data=f"adm:auto:{code}")],
        [InlineKeyboardButton("✏️ Ввести вручную", callback_data=f"adm:man:{code}")],
        [
            InlineKeyboardButton("➖ Буфер уже", callback_data=f"adm:bn:{code}"),
            InlineKeyboardButton("➕ Буфер шире", callback_data=f"adm:bw:{code}"),
        ],
        [InlineKeyboardButton("◀️ Назад", callback_data="adm:menu")],
    ])


# ─── Тексты статистики ──────────────────────────────────────────────────────

def build_stats_today_text() -> str:
    lines = ["📅 Статистика за сегодня\n"]
    for name in state:
        count, avg, total = db_today_stats(name)
        lines.append(f"— {name} —")
        if count == 0:
            lines.append("Закрытий пока не было\n")
        else:
            lines.append(f"Закрытий: {count}")
            lines.append(f"Среднее: {format_duration(avg)}")
            lines.append(f"Всего закрыт: {format_duration(total)}\n")
    return "\n".join(lines)


def build_longest_text() -> str:
    lines = ["🏆 Самые долгие закрытия\n"]
    for name in state:
        rows = db_longest(name, 3)
        lines.append(f"— {name} —")
        if not rows:
            lines.append("Данных пока нет\n")
        else:
            for i, (dur, ts) in enumerate(rows, 1):
                try:
                    dt = datetime.fromisoformat(ts)
                    when = dt.strftime("%d.%m %H:%M")
                except Exception:
                    when = ts
                lines.append(f"{i}. {format_duration(dur)} ({when})")
            lines.append("")
    return "\n".join(lines)


def build_peak_text() -> str:
    lines = ["⏰ Пиковые часы закрытий\n"]
    for name in state:
        rows = db_peak_hours(name, 5)
        lines.append(f"— {name} —")
        if not rows:
            lines.append("Данных пока нет\n")
        else:
            for hour, cnt in rows:
                lines.append(f"{hour}:00–{hour}:59 — {cnt} закрытий")
            lines.append("")
    return "\n".join(lines)


def build_log_text(n: int = 10) -> str:
    rows = db_recent_events(n)
    if not rows:
        return "📜 Лог пуст — событий ещё не было."
    lines = [f"📜 Последние {len(rows)} событий\n"]
    for crossing, st, prev_state, prev_dur, ts in rows:
        try:
            dt = datetime.fromisoformat(ts)
            when = dt.strftime("%d.%m %H:%M")
        except Exception:
            when = ts
        st_ru = "ОТКРЫТ ✅" if st == "open" else "ЗАКРЫТ ⛔"
        extra = ""
        if prev_dur and prev_state:
            prev_ru = "был закрыт" if prev_state == "closed" else "был открыт"
            extra = f" ({prev_ru} {format_duration(prev_dur)})"
        lines.append(f"{when} · {crossing} → {st_ru}{extra}")
    return "\n".join(lines)


# ─── Обновление сообщений ──────────────────────────────────────────────────

async def edit_or_skip(bot, chat_id, message_id, text, keyboard) -> bool:
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id,
                                    text=text, reply_markup=keyboard)
        return True
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            return True
        return False
    except Forbidden:
        return False
    except Exception as e:
        logger.warning(f"Ошибка chat_id={chat_id}: {e}")
        return True


async def push_update_to_all() -> None:
    if not active_messages or not bot_app:
        return
    text = build_status_text()
    keyboard = build_keyboard()
    to_remove = []
    for chat_id, message_id in list(active_messages.items()):
        ok = await edit_or_skip(bot_app.bot, chat_id, message_id, text, keyboard)
        if not ok:
            to_remove.append(chat_id)
    for chat_id in to_remove:
        active_messages.pop(chat_id, None)


async def send_status_message(chat_id, bot, reply_to=None) -> None:
    if chat_id in active_messages:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=active_messages[chat_id])
        except Exception:
            pass
    if reply_to:
        msg = await reply_to.reply_text(text=build_status_text(), reply_markup=build_keyboard())
    else:
        msg = await bot.send_message(chat_id=chat_id, text=build_status_text(), reply_markup=build_keyboard())
    active_messages[chat_id] = msg.message_id


# ─── FastAPI ────────────────────────────────────────────────────────────────

api = FastAPI()

async def check_key(x_device_key: str = Header(default=None)):
    """Проверяет секретный ключ устройства. Если DEVICE_SECRET пуст — пропускаем (локально)."""
    if DEVICE_SECRET and x_device_key != DEVICE_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    return True

class EventModel(BaseModel):
    crossing: str
    state: str

class CommandResult(BaseModel):
    crossing: str
    command_id: str
    result: str


@api.post("/event")
async def receive_event(event: EventModel, _: bool = Depends(check_key)):
    if event.crossing not in state:
        return {"ok": False, "error": f"Неизвестный переезд: {event.crossing}"}
    if event.state not in ("open", "closed"):
        return {"ok": False, "error": f"Неизвестный статус: {event.state}"}

    d = state[event.crossing]
    ts = datetime.now()
    # Длительность предыдущего состояния — пишем в базу
    prev_state = d["status"]
    prev_duration = (ts - d["since"]).total_seconds()

    # Записываем только если статус реально сменился
    if prev_state != event.state:
        record_event(event.crossing, event.state, prev_state, prev_duration, ts)

    d["status"] = event.state
    d["since"] = ts
    d["last_heartbeat"] = ts
    d["offline"] = False

    logger.info(f"[{ts.strftime('%H:%M:%S')}] EVENT {event.crossing} → {event.state}")
    await push_update_to_all()
    return {"ok": True, "crossing": event.crossing, "state": event.state}


@api.get("/heartbeat")
async def receive_heartbeat(crossing: str = Query(...), _: bool = Depends(check_key)):
    if crossing not in state:
        return {"ok": False, "error": f"Неизвестный переезд: {crossing}"}
    was_offline = state[crossing]["offline"]
    state[crossing]["last_heartbeat"] = datetime.now()
    state[crossing]["offline"] = False
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] HEARTBEAT {crossing}")
    if was_offline:
        await push_update_to_all()
    return {"ok": True, "crossing": crossing}


@api.get("/commands", response_class=PlainTextResponse)
async def get_commands(crossing: str = Query(...), _: bool = Depends(check_key)):
    if crossing not in state:
        return ""
    was_offline = state[crossing]["offline"]
    state[crossing]["last_heartbeat"] = datetime.now()
    state[crossing]["offline"] = False
    if was_offline:
        await push_update_to_all()
    cmds = command_queue[crossing]
    command_queue[crossing] = []
    if not cmds:
        return ""
    lines = []
    for c in cmds:
        line = c["type"] + "|" + c["id"]
        if c["params"]:
            line += "|" + c["params"]
        lines.append(line)
    return "\n".join(lines)


async def update_calib_view(meta: dict, crossing: str):
    code = CODE_BY_NAME[crossing]
    text = build_calib_text(crossing)
    kb = build_calib_keyboard(code)
    if meta.get("edit_message_id"):
        try:
            await bot_app.bot.edit_message_text(
                chat_id=meta["edit_chat_id"], message_id=meta["edit_message_id"],
                text=text, reply_markup=kb)
            return
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                return
        except Exception:
            pass
    await bot_app.bot.send_message(chat_id=ADMIN_ID, text=text, reply_markup=kb)


@api.post("/command_result")
async def command_result(res: CommandResult, _: bool = Depends(check_key)):
    meta = pending_commands.pop(res.command_id, None)
    if not meta:
        return {"ok": True}
    cmd_type = meta["type"]
    crossing = res.crossing
    elapsed = (datetime.now() - meta["created"]).total_seconds()

    if cmd_type == "get_angle":
        parts = res.result.split("|")
        d = state[crossing]
        try:
            d["last_angle"] = float(parts[0])
            d["last_angle_state"] = parts[1]
            d["closed_th"] = float(parts[2])
            d["open_th"] = float(parts[3])
        except (ValueError, IndexError):
            pass
        if meta.get("save_as") == "closed" and d["last_angle"] is not None:
            d["measured_closed"] = d["last_angle"]
        elif meta.get("save_as") == "open" and d["last_angle"] is not None:
            d["measured_open"] = d["last_angle"]
        await update_calib_view(meta, crossing)

    elif cmd_type == "set_threshold":
        parts = res.result.split("|")
        d = state[crossing]
        try:
            d["closed_th"] = float(parts[1])
            d["open_th"] = float(parts[2])
        except (ValueError, IndexError):
            pass
        if not meta.get("silent"):
            text = (f"✅ {crossing} — пороги обновлены\n\n"
                    f"закрыт <{d['closed_th']}° · открыт >{d['open_th']}°\n"
                    f"Сохранено в память устройства.")
            await bot_app.bot.send_message(chat_id=ADMIN_ID, text=text)

    elif cmd_type == "ping":
        await bot_app.bot.send_message(
            chat_id=ADMIN_ID, text=f"🏓 {crossing} на связи!\nОтклик: {elapsed:.1f} сек")

    elif cmd_type == "reboot":
        await bot_app.bot.send_message(chat_id=ADMIN_ID, text=f"🔄 {crossing} перезагружается...")

    logger.info(f"Результат {cmd_type} от {crossing}: {res.result}")
    return {"ok": True}


@api.get("/status")
async def get_status():
    result = {}
    for name, data in state.items():
        result[name] = {
            "status": data["status"], "minutes": get_minutes(data["since"]),
            "offline": data["offline"], "last_heartbeat_ago": get_minutes(data["last_heartbeat"]),
        }
    return result


# ─── Команды бота ─────────────────────────────────────────────────────────

def is_admin(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == ADMIN_ID


async def start(update, context) -> None:
    logger.info(f"Пользователь {update.effective_user.id} написал /start")
    await send_status_message(update.effective_chat.id, context.bot, reply_to=update.message)


async def status_cmd(update, context) -> None:
    await send_status_message(update.effective_chat.id, context.bot, reply_to=update.message)


async def help_cmd(update, context) -> None:
    """Подсказка по командам. Для админа — полный список, для остальных — базовый."""
    if is_admin(update):
        text = (
            "📖 Команды ARC RailGuard\n\n"
            "Публичные:\n"
            "/start — статус переездов\n"
            "/status — то же самое\n\n"
            "Админские:\n"
            "/admin — панель управления (калибровка, пинг, перезагрузка, статистика)\n\n"
            "Калибровка на месте:\n"
            "1. Открой /admin → нужный переезд\n"
            "2. Опусти шлагбаум → «Зафиксировать ЗАКРЫТ»\n"
            "3. Подними → «Зафиксировать ОТКРЫТ»\n"
            "4. «Рассчитать пороги» — готово\n\n"
            "Быстрые команды (без панели):\n"
            "/angle — текущий угол\n"
            "/threshold 20 55 — задать пороги\n"
            "/ping — связь с устройством\n"
            "/reboot — перезагрузить ESP32\n"
            "/log 20 — последние события\n"
            "/stats — статистика за сегодня\n"
            "/longest — самые долгие закрытия\n"
            "/peak — пиковые часы\n\n"
            "Все команды по умолчанию — для Гурьевского.\n"
            "Для Центрального добавь имя: /angle Центральный"
        )
    else:
        text = (
            "🚦 ARC RailGuard — статусы переездов Барыша\n\n"
            "/start — показать переезды прямо сейчас\n"
            "/status — то же самое\n\n"
            "Сообщение обновляется само каждую минуту."
        )
    await update.message.reply_text(text)


async def admin_cmd(update, context) -> None:
    if not is_admin(update):
        return
    await update.message.reply_text(build_admin_menu_text(),
                                    reply_markup=build_admin_menu_keyboard())


async def admin_callback(update, context) -> None:
    query = update.callback_query
    if update.effective_user.id != ADMIN_ID:
        await query.answer("Недоступно", show_alert=True)
        return

    parts = query.data.split(":")
    action = parts[1]
    code = parts[2] if len(parts) > 2 else None
    crossing = CROSSING_CODES.get(code) if code else None

    if action == "menu":
        await query.answer()
        awaiting_input.pop(query.message.chat_id, None)
        await query.edit_message_text(build_admin_menu_text(),
                                      reply_markup=build_admin_menu_keyboard())
        return

    if action == "sts":
        await query.answer()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm:menu")]])
        await query.edit_message_text(build_status_text(), reply_markup=kb)
        return

    # ── Статистика ──
    if action == "stat":
        await query.answer()
        await query.edit_message_text("📊 Статистика\n\nВыбери раздел:",
                                      reply_markup=build_stat_menu_keyboard())
        return

    if action == "st_today":
        await query.answer()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm:stat")]])
        await query.edit_message_text(build_stats_today_text(), reply_markup=kb)
        return

    if action == "st_long":
        await query.answer()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm:stat")]])
        await query.edit_message_text(build_longest_text(), reply_markup=kb)
        return

    if action == "st_peak":
        await query.answer()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm:stat")]])
        await query.edit_message_text(build_peak_text(), reply_markup=kb)
        return

    if action == "st_log":
        await query.answer()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm:stat")]])
        await query.edit_message_text(build_log_text(15), reply_markup=kb)
        return

    # ── Калибровка ──
    if action == "cal":
        await query.answer("Запрашиваю угол...")
        try:
            await query.edit_message_text(build_calib_text(crossing),
                                          reply_markup=build_calib_keyboard(code))
        except Exception:
            pass
        queue_command(crossing, "get_angle",
                      edit_chat_id=query.message.chat_id,
                      edit_message_id=query.message.message_id)
        return

    if action in ("mc", "mo"):
        save_as = "closed" if action == "mc" else "open"
        label = "ЗАКРЫТ" if action == "mc" else "ОТКРЫТ"
        await query.answer(f"Замеряю угол для «{label}»...")
        queue_command(crossing, "get_angle",
                      edit_chat_id=query.message.chat_id,
                      edit_message_id=query.message.message_id, save_as=save_as)
        return

    if action == "auto":
        d = state[crossing]
        mc, mo = d["measured_closed"], d["measured_open"]
        if mc is None or mo is None:
            await query.answer("Сначала зафиксируй ЗАКРЫТ и ОТКРЫТ", show_alert=True)
            return
        lo, hi = min(mc, mo), max(mc, mo)
        span = hi - lo
        if span < 10:
            await query.answer("Замеры слишком близко — проверь положения", show_alert=True)
            return
        closed_th = round(lo + span * 0.33, 1)
        open_th = round(lo + span * 0.67, 1)
        d["closed_th"], d["open_th"] = closed_th, open_th
        await query.answer("Пороги рассчитаны!")
        try:
            await query.edit_message_text(build_calib_text(crossing),
                                          reply_markup=build_calib_keyboard(code))
        except Exception:
            pass
        queue_command(crossing, "set_threshold", f"{closed_th}|{open_th}", silent=True)
        return

    if action == "man":
        await query.answer()
        awaiting_input[query.message.chat_id] = crossing
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Отмена", callback_data=f"adm:cal:{code}")]])
        await query.edit_message_text(
            f"✏️ Ввод порогов · {crossing}\n\n"
            "Отправь два числа через пробел:\n<закрыт> <открыт>\n\n"
            "Например: 20 55", reply_markup=kb)
        return

    if action in ("bw", "bn"):
        d = state[crossing]
        if d["closed_th"] is None or d["open_th"] is None:
            await query.answer("Сначала нажми «Обновить угол»", show_alert=True)
            return
        closed_th, open_th = d["closed_th"], d["open_th"]
        if action == "bw":
            closed_th -= STEP_BUFFER; open_th += STEP_BUFFER
        else:
            if (open_th - closed_th) - 2 * STEP_BUFFER < MIN_BUFFER_GAP:
                await query.answer("Буфер уже минимального", show_alert=True)
                return
            closed_th += STEP_BUFFER; open_th -= STEP_BUFFER
        d["closed_th"], d["open_th"] = round(closed_th, 1), round(open_th, 1)
        await query.answer("Готово")
        try:
            await query.edit_message_text(build_calib_text(crossing),
                                          reply_markup=build_calib_keyboard(code))
        except Exception:
            pass
        queue_command(crossing, "set_threshold", f"{d['closed_th']}|{d['open_th']}", silent=True)
        return

    if action == "png":
        await query.answer("Пингую...")
        queue_command(crossing, "ping")
        return

    if action == "rbt":
        await query.answer()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, перезагрузить", callback_data=f"adm:rbtok:{code}")],
            [InlineKeyboardButton("◀️ Отмена", callback_data="adm:menu")],
        ])
        await query.edit_message_text(f"⚠️ Перезагрузить «{crossing}»?", reply_markup=kb)
        return

    if action == "rbtok":
        await query.answer()
        queue_command(crossing, "reboot")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В меню", callback_data="adm:menu")]])
        await query.edit_message_text(f"🔄 Команда перезагрузки отправлена на «{crossing}».",
                                      reply_markup=kb)
        return


async def text_input_handler(update, context) -> None:
    if not is_admin(update):
        return
    chat_id = update.effective_chat.id
    crossing = awaiting_input.get(chat_id)
    if not crossing:
        return
    txt = update.message.text.replace(",", " ").split()
    if len(txt) < 2:
        await update.message.reply_text("Нужно два числа. Пример: 20 55")
        return
    try:
        closed_th = float(txt[0]); open_th = float(txt[1])
    except ValueError:
        await update.message.reply_text("Это должны быть числа. Пример: 20 55")
        return
    if closed_th >= open_th:
        await update.message.reply_text("Порог «закрыт» должен быть меньше «открыт».")
        return
    awaiting_input.pop(chat_id, None)
    d = state[crossing]
    d["closed_th"], d["open_th"] = round(closed_th, 1), round(open_th, 1)
    queue_command(crossing, "set_threshold", f"{d['closed_th']}|{d['open_th']}", silent=True)
    code = CODE_BY_NAME[crossing]
    await update.message.reply_text(build_calib_text(crossing),
                                    reply_markup=build_calib_keyboard(code))


# ─── Текстовые команды статистики ──────────────────────────────────────────

async def angle_cmd(update, context) -> None:
    if not is_admin(update):
        return
    crossing = "Гурьевский"
    if context.args and " ".join(context.args) in state:
        crossing = " ".join(context.args)
    queue_command(crossing, "get_angle")
    await update.message.reply_text(f"⏳ Запрашиваю угол у «{crossing}»...")


async def threshold_cmd(update, context) -> None:
    if not is_admin(update):
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /threshold <закрыт> <открыт>\nПример: /threshold 20 55")
        return
    try:
        closed_th = float(context.args[0]); open_th = float(context.args[1])
    except ValueError:
        await update.message.reply_text("Пороги должны быть числами.")
        return
    if closed_th >= open_th:
        await update.message.reply_text("Порог «закрыт» должен быть меньше «открыт».")
        return
    crossing = "Гурьевский"
    if len(context.args) >= 3 and context.args[2] in state:
        crossing = context.args[2]
    queue_command(crossing, "set_threshold", f"{closed_th}|{open_th}")
    await update.message.reply_text(f"⏳ Применяю пороги на «{crossing}»...")


async def ping_cmd(update, context) -> None:
    if not is_admin(update):
        return
    crossing = "Гурьевский"
    if context.args and " ".join(context.args) in state:
        crossing = " ".join(context.args)
    queue_command(crossing, "ping")
    await update.message.reply_text(f"⏳ Пингую «{crossing}»...")


async def reboot_cmd(update, context) -> None:
    if not is_admin(update):
        return
    crossing = "Гурьевский"
    if context.args and " ".join(context.args) in state:
        crossing = " ".join(context.args)
    queue_command(crossing, "reboot")
    await update.message.reply_text(f"⏳ Отправляю перезагрузку на «{crossing}»...")


async def log_cmd(update, context) -> None:
    if not is_admin(update):
        return
    n = 10
    if context.args:
        try:
            n = max(1, min(50, int(context.args[0])))
        except ValueError:
            pass
    await update.message.reply_text(build_log_text(n))


async def stats_cmd(update, context) -> None:
    if not is_admin(update):
        return
    await update.message.reply_text(build_stats_today_text())


async def longest_cmd(update, context) -> None:
    if not is_admin(update):
        return
    await update.message.reply_text(build_longest_text())


async def peak_cmd(update, context) -> None:
    if not is_admin(update):
        return
    await update.message.reply_text(build_peak_text())


# ─── Фоновые задачи ────────────────────────────────────────────────────────

async def heartbeat_watchdog() -> None:
    logger.info("Watchdog запущен.")
    while True:
        await asyncio.sleep(60)
        changed = False
        for name, data in state.items():
            silence = datetime.now() - data["last_heartbeat"]
            if silence > HEARTBEAT_TIMEOUT and not data["offline"]:
                data["offline"] = True
                changed = True
                logger.warning(f"⚠️ {name} — нет связи!")
        if changed:
            await push_update_to_all()


async def updater_loop(app: Application) -> None:
    logger.info("Фоновый обновлятор запущен.")
    while True:
        await asyncio.sleep(60)
        if not active_messages:
            continue
        text = build_status_text()
        keyboard = build_keyboard()
        to_remove = []
        for chat_id, message_id in list(active_messages.items()):
            ok = await edit_or_skip(app.bot, chat_id, message_id, text, keyboard)
            if not ok:
                to_remove.append(chat_id)
        for chat_id in to_remove:
            active_messages.pop(chat_id, None)
        logger.info(f"Обновлено сообщений: {len(active_messages)}")


async def run_api_server() -> None:
    config = uvicorn.Config(api, host="0.0.0.0", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def post_init(app: Application) -> None:
    global bot_app
    bot_app = app
    app.create_task(updater_loop(app))
    app.create_task(run_api_server())
    app.create_task(heartbeat_watchdog())
    logger.info("HTTP сервер запущен на http://localhost:8000")
    logger.info(f"Админ chat_id: {ADMIN_ID}")


def main() -> None:
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не найден! Проверь файл .env")
        return

    init_db()  # создаём базу при старте

    logger.info("Запускаю бота ARC RailGuard (Этап 2 — статистика)...")
    app = (Application.builder().token(BOT_TOKEN).post_init(post_init).build())

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("angle", angle_cmd))
    app.add_handler(CommandHandler("threshold", threshold_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("reboot", reboot_cmd))
    app.add_handler(CommandHandler("log", log_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("longest", longest_cmd))
    app.add_handler(CommandHandler("peak", peak_cmd))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^adm:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler))

    logger.info("Бот запущен. Нажми Ctrl+C для остановки.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
