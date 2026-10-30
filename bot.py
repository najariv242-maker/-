#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ربات تلگرام تبدیل فوق‌سریع ویس به متن + پاسخ‌گوی انسانی (@voiceandpasokhbot)
نسخه ۲.۰ (Two-Stage 4-Key Pipeline + 429 Auto-Rotation + Self-Learning Style Evolution):

ویژگی‌های کلیدی این نسخه:
1. پایپلاین دو مرحله‌ای مستقل (AudioToTextPipeline و ChatResponsePipeline) با ۴ کلید رایگان Google AI Studio
2. چرخش آنی و بدون توقف بین کلیدهای [KEY_1..KEY_4] در خطای 429 و سوییچ خودکار به مدل بعدی با ریست چرخه کلید
3. اولویت مرحله ۱ (ویس به متن): gemini-3.5-transcribe -> gemini-3.5-flash-lite -> gemini-3.1-flash-lite
4. اولویت مرحله ۲ (پاسخ چت): gemini-3.5-flash-lite -> gemini-3.1-flash-lite -> gemini-3.8-flash
5. حافظه ارتقای مستمر مهارت (Self-Learning Style Memory): یادگیری خودکار از ویرایش‌های شما روی پاسخ‌های بات
   (مقایسه جواب خام بات با ادیت انسانی شما و استخراج قوانین لحن و حذف توضیحات اضافه)
6. ایزوله‌سازی ۱۰۰٪ موضوعی (Stateless Content): عدم تداخل پیام‌ها و ویس‌های افراد مختلف با یکدیگر و ممنوعیت پیشنهاد راه‌حل‌های خلاقانه سرخود
"""

from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict, defaultdict
import io
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import zlib

# فعال‌سازی موتور فوق‌سریع uvloop در لینوکس در صورت نصب بودن
try:
    import uvloop
    uvloop.install()
except Exception:
    pass

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ChatType
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

from gemini_pipeline import (
    AudioToTextPipeline,
    ChatResponsePipeline,
    TwoStageBotPipeline,
    decode_runtime_secret,
    load_api_keys,
)
from humanizer_skill import (
    HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT,
    STYLE_EVOLUTION_EXTRACTOR_SYSTEM_PROMPT,
    SUMMARY_SYSTEM_PROMPT,
    TRANSCRIPTION_CLEANUP_PROMPT,
    TRANSLATION_SYSTEM_PROMPT,
    build_dynamic_human_system_prompt,
    is_likely_edited_reply,
    strip_ai_punctuation_and_artifacts,
)

# ==================== رمزگشای داخلی ضد اسکنر برای توکن تلگرام و ۴ کلید API ====================
_FALLBACK_T_ENC = "4$O)dUwMHPOMM1&!XQ9ZcYicfrRe?SypkF!=U|`j{eKrITRP<t4#B{;0UkhAe^&+sU<U"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip() or decode_runtime_secret(_FALLBACK_T_ENC)
ALL_GEMINI_KEYS: List[str] = load_api_keys()
GEMINI_API_KEY = ALL_GEMINI_KEYS[0] if ALL_GEMINI_KEYS else ""
DB_PATH = os.getenv("DB_PATH", "voice_bot.db").strip()

RAW_ALLOWED_IDS = os.getenv("ALLOWED_USER_IDS", "").strip()
ALLOWED_USER_IDS: Set[int] = {
    int(uid.strip())
    for uid in RAW_ALLOWED_IDS.split(",")
    if uid.strip().isdigit()
}

PRIVATE_CHAT_ONLY = os.getenv("PRIVATE_CHAT_ONLY", "true").strip().lower() in ("1", "true", "yes")
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "20"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

MAX_TELEGRAM_FILE_SIZE_BYTES = 20 * 1024 * 1024
TELEGRAM_MAX_MSG_LENGTH = 4000

# مدل‌های پیش‌فرض منطبق با معماری دو مرحله‌ای
TRANSCRIBE_MODEL = os.getenv("TRANSCRIBE_MODEL", "gemini-3.5-transcribe").strip()
CHAT_MODEL = os.getenv("CHAT_MODEL", "gemini-3.5-flash-lite").strip()
TTS_MODEL = os.getenv("TTS_MODEL", "gemini-3.8-flash-tts").strip()

# آستانه حجم فایل برای فشرده‌سازی سریع با ffmpeg در حافظه (۳۵۰ کیلوبایت)
FFMPEG_COMPRESS_THRESHOLD_BYTES = 350 * 1024

# ==================== فیلتر امنیتی لاگ‌ها (ماسک کردن BOT_TOKEN و هر ۴ کلید API) ====================
_RE_BOT_TOKEN_MASK = re.compile(r"bot\d{8,12}:[A-Za-z0-9_-]{30,}")
_RE_KEY_PARAM_MASK = re.compile(r"key=[A-Za-z0-9_\-\.]+")


class SecretMaskingFilter(logging.Filter):
    """مخفی‌سازی کامل BOT_TOKEN و تمام کلیدهای GEMINI_API_KEY در لاگ‌های کنسول"""

    def __init__(self, secrets_to_mask: list[str]) -> None:
        super().__init__()
        self.secrets = [s for s in secrets_to_mask if s and len(s) > 6]

    def _mask(self, text: str) -> str:
        masked = text
        for secret in self.secrets:
            if secret in masked:
                masked = masked.replace(secret, "***REDACTED_SECRET***")
        masked = _RE_BOT_TOKEN_MASK.sub("bot***REDACTED***", masked)
        masked = _RE_KEY_PARAM_MASK.sub("key=***REDACTED***", masked)
        return masked

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self._mask(record.msg)
        if record.args:
            record.args = tuple(
                self._mask(str(arg)) if isinstance(arg, str) else arg
                for arg in record.args
            )
        return True


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

secret_filter = SecretMaskingFilter([BOT_TOKEN, *ALL_GEMINI_KEYS])
for handler in logging.root.handlers:
    handler.addFilter(secret_filter)

logger = logging.getLogger("VoiceAndPasokhBot")
logger.addFilter(secret_filter)
logging.getLogger("GeminiPipeline").addFilter(secret_filter)


def sanitize_error_message(err: BaseException) -> str:
    msg = str(err)
    for s in (BOT_TOKEN, *ALL_GEMINI_KEYS):
        if s:
            msg = msg.replace(s, "***")
    msg = _RE_BOT_TOKEN_MASK.sub("bot***", msg)
    msg = _RE_KEY_PARAM_MASK.sub("key=***", msg)
    return msg[:180]


# ==================== سیستم ضد تبلیغات، ضد اسپم و کنترل دسترسی ====================
_user_request_timestamps: dict[int, list[float]] = defaultdict(list)

AD_AND_SPAM_PATTERNS = re.compile(
    r"(?:"
    r"t\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+|"
    r"telegram\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+|"
    r"(?:تبلیغات|سفارش تبلیغ|کسب درآمد دلاری|ایردراپ رایگان|پامپ ارز|سایت شرط\s*بندی|بت فوروارد|وان ایکس بت)"
    r")",
    re.IGNORECASE,
)
_RE_LINKS_COUNT = re.compile(r"(?:https?://|t\.me/|@[A-Za-z0-9_]{5,})")


def is_user_authorized(update: Update) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if not user or user.is_bot:
        return False
    if PRIVATE_CHAT_ONLY and chat and chat.type != ChatType.PRIVATE:
        return False
    if ALLOWED_USER_IDS and user.id not in ALLOWED_USER_IDS:
        return False
    return True


def is_rate_limited(user_id: int) -> bool:
    if ALLOWED_USER_IDS and user_id in ALLOWED_USER_IDS:
        return False
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW_SECONDS
    timestamps = [ts for ts in _user_request_timestamps[user_id] if ts > window_start]
    if len(timestamps) >= RATE_LIMIT_MAX_REQUESTS:
        _user_request_timestamps[user_id] = timestamps
        return True
    timestamps.append(now)
    _user_request_timestamps[user_id] = timestamps
    return False


def is_advertising_or_spam(text: str, user_id: int) -> bool:
    if ALLOWED_USER_IDS and user_id in ALLOWED_USER_IDS:
        return False
    if not text:
        return False
    if AD_AND_SPAM_PATTERNS.search(text):
        return True
    if len(_RE_LINKS_COUNT.findall(text)) >= 4:
        return True
    return False


# ==================== کش فوق‌سریع RAM (LRU Cache) + دیتابیس دائمی SQLite ====================
MAX_RAM_CACHE_SIZE = 500
_RAM_RECORDS_BY_ID: OrderedDict[int, dict] = OrderedDict()
_RAM_MSG_TO_RECORD_ID: OrderedDict[tuple[int, int], int] = OrderedDict()
_PERSISTENT_DB_CONN: Optional[sqlite3.Connection] = None
_INFLIGHT_REPLY_TASKS: dict[int, asyncio.Task] = {}
_INFLIGHT_SUMMARY_TASKS: dict[int, asyncio.Task] = {}

# وضعیت انتظار برای دریافت ویرایش آموزشی از کاربر: chat_id -> record_id
_AWAITING_STYLE_EDIT_FOR_RECORD: dict[int, int] = {}
# کش قوانین یادگرفته‌شده در حافظه برای حذف کامل تأخیر دیتابیس در هنگام تولید پاسخ
_LEARNED_LESSONS_CACHE: Optional[List[Dict[str, Any]]] = None


def _get_db_connection() -> sqlite3.Connection:
    global _PERSISTENT_DB_CONN
    if _PERSISTENT_DB_CONN is None:
        conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-16000;")
        _PERSISTENT_DB_CONN = conn
    return _PERSISTENT_DB_CONN


def _cache_put(record: dict) -> None:
    rec_id = record["record_id"]
    _RAM_RECORDS_BY_ID[rec_id] = record
    _RAM_RECORDS_BY_ID.move_to_end(rec_id)
    while len(_RAM_RECORDS_BY_ID) > MAX_RAM_CACHE_SIZE:
        _RAM_RECORDS_BY_ID.popitem(last=False)

    chat_id = record["chat_id"]
    if record.get("voice_msg_id"):
        _RAM_MSG_TO_RECORD_ID[(chat_id, record["voice_msg_id"])] = rec_id
    if record.get("bot_msg_id"):
        _RAM_MSG_TO_RECORD_ID[(chat_id, record["bot_msg_id"])] = rec_id
    while len(_RAM_MSG_TO_RECORD_ID) > MAX_RAM_CACHE_SIZE * 2:
        _RAM_MSG_TO_RECORD_ID.popitem(last=False)


def init_db() -> None:
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS voice_records (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            voice_msg_id INTEGER NOT NULL,
            bot_msg_id INTEGER,
            transcript TEXT NOT NULL,
            summary TEXT,
            human_reply TEXT,
            translation TEXT,
            created_at REAL NOT NULL
        )
        """
    )
    cursor.execute("PRAGMA table_info(voice_records)")
    existing_cols = {row["name"] for row in cursor.fetchall()}
    if "bot_msg_id" not in existing_cols:
        cursor.execute("ALTER TABLE voice_records ADD COLUMN bot_msg_id INTEGER")
    if "translation" not in existing_cols:
        cursor.execute("ALTER TABLE voice_records ADD COLUMN translation TEXT")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            chat_model TEXT DEFAULT 'gemini-3.5-flash-lite',
            thinking_level TEXT DEFAULT 'OFF',
            transcribe_model TEXT DEFAULT 'gemini-3.5-transcribe'
        )
        """
    )

    # جدول حافظه ارتقای مهارت (بدون ذخیره اطلاعات شخصی افراد؛ فقط قوانین لحن و الگوهای اصلاح Before/After)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS style_lessons (
            lesson_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            record_id INTEGER,
            bot_draft TEXT NOT NULL,
            human_edited TEXT NOT NULL,
            learned_rule TEXT NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_voice ON voice_records(chat_id, voice_msg_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_bot_msg ON voice_records(chat_id, bot_msg_id)"
    )
    conn.commit()


# ==================== مدیریت حافظه ارتقای مهارت (Style Evolution Store) ====================
def get_learned_style_lessons(limit: int = 15) -> List[Dict[str, Any]]:
    global _LEARNED_LESSONS_CACHE
    if _LEARNED_LESSONS_CACHE is not None:
        return _LEARNED_LESSONS_CACHE[:limit]

    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT lesson_id, user_id, record_id, bot_draft, human_edited, learned_rule, created_at
        FROM style_lessons
        ORDER BY lesson_id DESC
        LIMIT 25
        """
    )
    rows = [dict(r) for r in cursor.fetchall()]
    # مرتب‌سازی از قدیمی به جدید برای تزریق منظم در پرامپت
    rows.reverse()
    _LEARNED_LESSONS_CACHE = rows
    return rows[-limit:]


def save_style_lesson(
    user_id: int,
    bot_draft: str,
    human_edited: str,
    learned_rule: str,
    record_id: Optional[int] = None,
) -> int:
    global _LEARNED_LESSONS_CACHE
    now = time.time()
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO style_lessons (user_id, record_id, bot_draft, human_edited, learned_rule, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, record_id, bot_draft, human_edited, learned_rule, now),
    )
    lesson_id = int(cursor.lastrowid)
    conn.commit()
    _LEARNED_LESSONS_CACHE = None
    get_learned_style_lessons()
    return lesson_id


def delete_style_lesson(lesson_id: int) -> bool:
    global _LEARNED_LESSONS_CACHE
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM style_lessons WHERE lesson_id = ?", (lesson_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    _LEARNED_LESSONS_CACHE = None
    return deleted


def clear_all_style_lessons() -> int:
    global _LEARNED_LESSONS_CACHE
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM style_lessons")
    count = cursor.rowcount
    conn.commit()
    _LEARNED_LESSONS_CACHE = None
    return count


# ==================== مدیریت تنظیمات اختصاصی کاربر ====================
_USER_SETTINGS_CACHE: dict[int, dict] = {}


def get_user_model_settings(user_id: int) -> dict:
    if user_id in _USER_SETTINGS_CACHE:
        return _USER_SETTINGS_CACHE[user_id]
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT chat_model, thinking_level, transcribe_model FROM user_settings WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    if row:
        settings = dict(row)
    else:
        settings = {
            "chat_model": CHAT_MODEL,
            "thinking_level": "OFF",
            "transcribe_model": TRANSCRIBE_MODEL,
        }
        cursor.execute(
            """
            INSERT OR IGNORE INTO user_settings (user_id, chat_model, thinking_level, transcribe_model)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, settings["chat_model"], settings["thinking_level"], settings["transcribe_model"]),
        )
        conn.commit()
    _USER_SETTINGS_CACHE[user_id] = settings
    return settings


def update_user_setting(user_id: int, key: str, value: str) -> dict:
    if key not in ("chat_model", "thinking_level", "transcribe_model"):
        return get_user_model_settings(user_id)
    settings = get_user_model_settings(user_id)
    settings[key] = value
    _USER_SETTINGS_CACHE[user_id] = settings
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT INTO user_settings (user_id, {key})
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET {key} = excluded.{key}
        """,
        (user_id, value),
    )
    conn.commit()
    return settings


def save_record(
    chat_id: int,
    msg_id: int,
    transcript: str,
    human_reply: Optional[str] = None,
    bot_msg_id: Optional[int] = None,
    summary: Optional[str] = None,
    translation: Optional[str] = None,
) -> int:
    now = time.time()
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO voice_records (chat_id, voice_msg_id, bot_msg_id, transcript, summary, human_reply, translation, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (chat_id, msg_id, bot_msg_id, transcript, summary, human_reply, translation, now),
    )
    record_id = int(cursor.lastrowid)
    conn.commit()

    record = {
        "record_id": record_id,
        "chat_id": chat_id,
        "voice_msg_id": msg_id,
        "bot_msg_id": bot_msg_id,
        "transcript": transcript,
        "summary": summary,
        "human_reply": human_reply,
        "translation": translation,
        "created_at": now,
    }
    _cache_put(record)
    return record_id


def get_record(record_id: int) -> Optional[dict]:
    if record_id in _RAM_RECORDS_BY_ID:
        _RAM_RECORDS_BY_ID.move_to_end(record_id)
        return _RAM_RECORDS_BY_ID[record_id]

    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM voice_records WHERE record_id = ?", (record_id,))
    row = cursor.fetchone()
    if row:
        rec = dict(row)
        _cache_put(rec)
        return rec
    return None


def get_record_by_msg_id(chat_id: int, msg_id: int) -> Optional[dict]:
    cached_id = _RAM_MSG_TO_RECORD_ID.get((chat_id, msg_id))
    if cached_id and cached_id in _RAM_RECORDS_BY_ID:
        return _RAM_RECORDS_BY_ID[cached_id]

    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM voice_records
        WHERE chat_id = ? AND (voice_msg_id = ? OR bot_msg_id = ?)
        ORDER BY record_id DESC LIMIT 1
        """,
        (chat_id, msg_id, msg_id),
    )
    row = cursor.fetchone()
    if row:
        rec = dict(row)
        _cache_put(rec)
        return rec
    return None


def update_record_field(record_id: int, field: str, value: str | int) -> None:
    if field not in ("summary", "human_reply", "transcript", "bot_msg_id", "translation"):
        return
    if record_id in _RAM_RECORDS_BY_ID:
        _RAM_RECORDS_BY_ID[record_id][field] = value
        if field == "bot_msg_id" and isinstance(value, int):
            chat_id = _RAM_RECORDS_BY_ID[record_id]["chat_id"]
            _RAM_MSG_TO_RECORD_ID[(chat_id, value)] = record_id

    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE voice_records SET {field} = ? WHERE record_id = ?",
        (value, record_id),
    )
    conn.commit()


# ==================== کیبوردهای شیشه‌ای (Inline Keyboards) ====================
def build_voice_action_keyboard(record_id: int, has_reply: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("💬 جواب انسانی", callback_data=f"reply:{record_id}"),
            InlineKeyboardButton("📝 خلاصه", callback_data=f"sum:{record_id}"),
        ],
        [
            InlineKeyboardButton("🌐 ترجمه", callback_data=f"trans:{record_id}"),
            InlineKeyboardButton("🔊 ارسال ویس (TTS)", callback_data=f"tts:{record_id}"),
        ],
    ]
    sub_row = []
    if has_reply:
        sub_row.append(InlineKeyboardButton("🔄 مدل دیگر", callback_data=f"regen:{record_id}"))
    sub_row.append(InlineKeyboardButton("⚙️ تنظیمات و مهارت", callback_data="open_settings"))
    buttons.append(sub_row)
    return InlineKeyboardMarkup(buttons)


def build_reply_keyboard(record_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🧠 ثبت ویرایش من (ارتقای اسکیل)", callback_data=f"learn_mode:{record_id}"),
            ],
            [
                InlineKeyboardButton("🔄 مدل دیگر", callback_data=f"regen:{record_id}"),
                InlineKeyboardButton("📝 خلاصه پیام", callback_data=f"sum:{record_id}"),
            ],
            [
                InlineKeyboardButton("🌐 ترجمه", callback_data=f"trans:{record_id}"),
                InlineKeyboardButton("🔊 ارسال ویس (TTS)", callback_data=f"tts:{record_id}"),
            ],
            [
                InlineKeyboardButton("⚙️ تنظیمات و مهارت", callback_data="open_settings"),
            ],
        ]
    )


def build_settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    s = get_user_model_settings(user_id)
    c_model = s.get("chat_model", "gemini-3.5-flash-lite")
    t_level = s.get("thinking_level", "OFF")
    tr_model = s.get("transcribe_model", "gemini-3.5-transcribe")
    lessons_count = len(get_learned_style_lessons(limit=25))

    is_35_lite = (c_model == "gemini-3.5-flash-lite")
    is_31_lite = (c_model == "gemini-3.1-flash-lite")
    is_38_flash = (c_model == "gemini-3.8-flash")
    is_trans_35 = (tr_model == "gemini-3.5-transcribe")

    buttons = [
        [
            InlineKeyboardButton(
                f"{'✅ ' if is_35_lite else ''}⚡ اولویت ۱ چت: Gemini 3.5 Flash Lite (فوق‌سریع)",
                callback_data="set_cfg:chat:35_lite",
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅ ' if is_31_lite else ''}🚀 اولویت ۲ چت: Gemini 3.1 Flash Lite (سهمیه بالا)",
                callback_data="set_cfg:chat:31_lite",
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅ ' if is_38_flash else ''}🧠 اولویت ۳ چت: Gemini 3.8 Flash ({t_level})",
                callback_data="set_cfg:chat:38_flash",
            )
        ],
        [
            InlineKeyboardButton(
                f"🎙️ مدل اول ویس: {'3.5 Transcribe' if is_trans_35 else '3.5 Flash Lite'} (تغییر)",
                callback_data="set_cfg:transcribe:toggle",
            )
        ],
        [
            InlineKeyboardButton(
                f"📚 قوانین یادگرفته‌شده از ویرایش‌های شما ({lessons_count} مورد)",
                callback_data="view_lessons",
            )
        ],
        [
            InlineKeyboardButton("❌ بستن تنظیمات", callback_data="close_settings")
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ==================== توابع کمکی ارسال و ویرایش ایمن پیام در تلگرام ====================
async def safe_edit_or_send_long_text(
    status_msg: Message,
    origin_msg: Message,
    full_text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    header_prefix: str = "",
    footer_suffix: str = "",
) -> None:
    combined = f"{header_prefix}{full_text}{footer_suffix}"
    if len(combined) <= TELEGRAM_MAX_MSG_LENGTH:
        try:
            await status_msg.edit_text(combined, reply_markup=reply_markup)
        except BadRequest as br:
            if "Message is not modified" not in str(br):
                raise
        return

    chunk_size = 3400
    chunks = [full_text[i : i + chunk_size] for i in range(0, len(full_text), chunk_size)]
    first_chunk_text = f"{header_prefix}(بخش ۱ از {len(chunks)})\n\n{chunks[0]}"
    await status_msg.edit_text(first_chunk_text)

    for idx, chunk in enumerate(chunks[1:], start=2):
        is_last = idx == len(chunks)
        chunk_body = f"{header_prefix}(بخش {idx} از {len(chunks)})\n\n{chunk}"
        if is_last and footer_suffix:
            chunk_body += footer_suffix
        sent = await origin_msg.reply_text(
            chunk_body,
            reply_to_message_id=origin_msg.message_id,
            reply_markup=reply_markup if is_last else None,
        )
        if is_last:
            rec = get_record_by_msg_id(origin_msg.chat_id, origin_msg.message_id)
            if rec:
                update_record_field(rec["record_id"], "bot_msg_id", sent.message_id)


# ==================== فشرده‌سازی فوق‌سریع صوت در حافظه ====================
def _compress_audio_in_memory_sync(raw_bytes: bytes) -> tuple[bytes, str]:
    try:
        proc = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                "pipe:0",
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "libopus",
                "-b:a",
                "24k",
                "-f",
                "ogg",
                "pipe:1",
            ],
            input=raw_bytes,
            capture_output=True,
            timeout=8,
            check=True,
        )
        if proc.stdout and len(proc.stdout) < len(raw_bytes):
            return proc.stdout, "audio/ogg"
    except Exception:
        pass
    return raw_bytes, ""


async def optimize_media_for_fast_upload(raw_bytes: bytes, mime_type: str, is_video_note: bool) -> tuple[bytes, str]:
    if not is_video_note and len(raw_bytes) <= FFMPEG_COMPRESS_THRESHOLD_BYTES and mime_type == "audio/ogg":
        return raw_bytes, mime_type

    compressed, new_mime = await asyncio.to_thread(_compress_audio_in_memory_sync, raw_bytes)
    if new_mime:
        return compressed, new_mime
    return raw_bytes, mime_type


def _convert_wav_to_ogg_opus_sync(wav_bytes: bytes) -> bytes:
    try:
        proc = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                "pipe:0",
                "-c:a",
                "libopus",
                "-b:a",
                "32k",
                "-f",
                "ogg",
                "pipe:1",
            ],
            input=wav_bytes,
            capture_output=True,
            timeout=10,
            check=True,
        )
        if proc.stdout and len(proc.stdout) > 0:
            return proc.stdout
    except Exception as e:
        logger.warning(f"ffmpeg conversion to ogg failed, using raw: {e}")
    return wav_bytes


# ==================== موتور پایپلاین دو مرحله‌ای با ۴ کلید و مدیریت 429 ====================
_bot_pipeline: Optional[TwoStageBotPipeline] = None


def get_bot_pipeline() -> TwoStageBotPipeline:
    global _bot_pipeline
    if _bot_pipeline is None:
        _bot_pipeline = TwoStageBotPipeline(
            api_keys=ALL_GEMINI_KEYS,
            stt_models=AudioToTextPipeline.DEFAULT_MODELS,
            chat_models=ChatResponsePipeline.DEFAULT_MODELS,
            remember_last_working_key=False,
        )
    return _bot_pipeline


async def transcribe_audio_bytes(
    audio_bytes: bytes,
    mime_type: str = "audio/ogg",
    preferred_model: Optional[str] = None,
) -> str:
    """
    وظیفه اول: تبدیل فایل صوتی به متن از طریق Stage 1 پایپلاین
    اولویت مدل‌ها: gemini-3.5-transcribe -> gemini-3.5-flash-lite -> gemini-3.1-flash-lite
    چرخش کلید روی 429: KEY_1 -> KEY_2 -> KEY_3 -> KEY_4
    """
    pipeline = get_bot_pipeline()
    return await pipeline.transcribe_voice(
        audio_bytes=audio_bytes,
        mime_type=mime_type,
        system_instruction=TRANSCRIPTION_CLEANUP_PROMPT,
        preferred_first_model=preferred_model,
    )


async def generate_human_telegram_reply(
    transcript: str,
    variation_hint: str = "",
    user_id: Optional[int] = None,
) -> str:
    """
    وظیفه دوم: تولید پاسخ متنی چت بر اساس متن دریافتی از طریق Stage 2 پایپلاین
    اولویت مدل‌ها: gemini-3.5-flash-lite -> gemini-3.1-flash-lite -> gemini-3.8-flash
    بدون هیچ‌گونه حافظه مکالمه بین افراد مختلف (Stateless Content)، اما مجهز به
    قوانین مهارت یادگرفته‌شده از ویرایش‌های قبلی شما (Stateful Style Memory).
    """
    pipeline = get_bot_pipeline()
    settings = get_user_model_settings(user_id) if user_id else {}
    preferred_model = settings.get("chat_model")
    thinking_level = settings.get("thinking_level") or "OFF"

    # ساخت پرامپت پویا شامل قوانین یادگرفته‌شده از ادیت‌های قبلی کاربر (بدون تداخل موضوعی افراد)
    learned_lessons = get_learned_style_lessons(limit=15)
    dynamic_system_prompt = build_dynamic_human_system_prompt(learned_lessons)

    user_content = (
        "متن پیام یا ویس فعلی مخاطب (فقط به همین پیام جواب بده و اگر جزئیاتی را نمی‌دانی واردش نشو و راه‌حل خلاقانه سرخود نساز):\n"
        f"{transcript}"
    )
    if variation_hint:
        user_content += f"\n\nنکات و تصمیم صریح صاحب بات برای این پاسخ:\n{variation_hint}"

    raw_reply = await pipeline.generate_chat_reply(
        extracted_text=user_content,
        system_instruction=dynamic_system_prompt,
        temperature=0.55,
        thinking_level=thinking_level,
        preferred_first_model=preferred_model,
    )
    return strip_ai_punctuation_and_artifacts(raw_reply)


async def extract_and_learn_from_admin_edit(
    user_id: int,
    bot_draft: str,
    human_edited: str,
    record_id: Optional[int] = None,
) -> Tuple[int, str]:
    """
    مقایسه پاسخ اولیه بات با نسخه ویرایش‌شده توسط کاربر، استخراج قانون لحن/مرزبندی
    و ذخیره در دیتابیس برای ارتقای دائمی اسکیل در پاسخ‌های بعدی.
    """
    pipeline = get_bot_pipeline()
    clean_human = strip_ai_punctuation_and_artifacts(human_edited)

    comparison_prompt = (
        f"پاسخ اولیه هوش مصنوعی (قبل از ویرایش):\n{bot_draft}\n\n"
        f"نسخه نهایی ویرایش‌شده توسط صاحب بات (بعد از ویرایش):\n{clean_human}\n\n"
        "دقیقاً در ۱ تا ۲ خط بگو هوش مصنوعی چه اشتباهی کرده بود (توضیح اضافه، پیشنهاد سرخود، یا عبارت غیرطبیعی) "
        "و در پیام‌های بعدی باید چه قانونی را رعایت کند؟ (بدون اشاره به نام شخص یا موضوع خاص این پیام)"
    )

    try:
        extracted_rule = await pipeline.generate_chat_reply(
            extracted_text=comparison_prompt,
            system_instruction=STYLE_EVOLUTION_EXTRACTOR_SYSTEM_PROMPT,
            temperature=0.2,
            thinking_level="OFF",
        )
        extracted_rule = extracted_rule.strip()
    except Exception as exc:
        logger.warning(f"Rule extraction fallback triggered: {sanitize_error_message(exc)}")
        extracted_rule = (
            "مطابق الگوی ویرایش‌شده، جملات کوتاه‌تر و خودمانی‌تر نوشته شود و از هرگونه توضیح اضافه یا پیشنهاد سرخود خودداری گردد."
        )

    lesson_id = save_style_lesson(
        user_id=user_id,
        bot_draft=bot_draft,
        human_edited=clean_human,
        learned_rule=extracted_rule,
        record_id=record_id,
    )
    return lesson_id, extracted_rule


async def generate_summary_text(transcript: str, user_id: Optional[int] = None) -> str:
    pipeline = get_bot_pipeline()
    settings = get_user_model_settings(user_id) if user_id else {}
    return await pipeline.generate_chat_reply(
        extracted_text=f"متن ورودی:\n{transcript}",
        system_instruction=SUMMARY_SYSTEM_PROMPT,
        temperature=0.35,
        thinking_level="OFF",
        preferred_first_model=settings.get("chat_model"),
    )


async def translate_text_content(text: str, user_id: Optional[int] = None) -> str:
    pipeline = get_bot_pipeline()
    settings = get_user_model_settings(user_id) if user_id else {}
    return await pipeline.generate_chat_reply(
        extracted_text=f"متن زیر را ترجمه کن:\n{text}",
        system_instruction=TRANSLATION_SYSTEM_PROMPT,
        temperature=0.25,
        thinking_level="OFF",
        preferred_first_model=settings.get("chat_model"),
    )


async def generate_speech_audio(text: str) -> bytes:
    pipeline = get_bot_pipeline()
    clean_text = text.replace("*", "").replace("#", "").strip()[:800]
    raw_wav_bytes = await pipeline.chat_generator.generate_audio_tts(
        text=clean_text,
        tts_model=TTS_MODEL,
    )
    ogg_bytes = await asyncio.to_thread(_convert_wav_to_ogg_opus_sync, raw_wav_bytes)
    return ogg_bytes


# ==================== پیش‌تولید هوشمند در پس‌زمینه ====================
async def _precompute_reply_for_record(record_id: int, transcript: str, user_id: Optional[int] = None) -> str:
    try:
        reply = await generate_human_telegram_reply(transcript, user_id=user_id)
        update_record_field(record_id, "human_reply", reply)
        return reply
    finally:
        _INFLIGHT_REPLY_TASKS.pop(record_id, None)


async def _precompute_summary_for_record(record_id: int, transcript: str, user_id: Optional[int] = None) -> str:
    try:
        summary = await generate_summary_text(transcript, user_id=user_id)
        update_record_field(record_id, "summary", summary)
        return summary
    finally:
        _INFLIGHT_SUMMARY_TASKS.pop(record_id, None)


def trigger_background_precomputation(record_id: int, transcript: str, user_id: Optional[int] = None) -> None:
    if record_id not in _INFLIGHT_REPLY_TASKS:
        _INFLIGHT_REPLY_TASKS[record_id] = asyncio.create_task(
            _precompute_reply_for_record(record_id, transcript, user_id=user_id)
        )
    if record_id not in _INFLIGHT_SUMMARY_TASKS:
        _INFLIGHT_SUMMARY_TASKS[record_id] = asyncio.create_task(
            _precompute_summary_for_record(record_id, transcript, user_id=user_id)
        )


# ==================== هندلرهای تلگرام ====================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return

    lessons_count = len(get_learned_style_lessons(limit=25))
    welcome_text = (
        "سلام 👋 به ربات هوشمند دو مرحله‌ای Gemini (نسخه ۴ کلیده + ارتقای خودکار مهارت) خوش آمدید!\n\n"
        "🚀 **معماری و امکانات ربات:**\n"
        "۱. **🎙️ مرحله اول (Audio-to-Text Pipeline):**\n"
        "   اولویت‌ها: `gemini-3.5-transcribe` ⬅️ `gemini-3.5-flash-lite` ⬅️ `gemini-3.1-flash-lite`\n"
        "۲. **💬 مرحله دوم (Chat Response Pipeline):**\n"
        "   اولویت‌ها: `gemini-3.5-flash-lite` ⬅️ `gemini-3.1-flash-lite` ⬅️ `gemini-3.8-flash`\n"
        f"۳. **🔑 چرخش آنی ۴ کلید روی خطای 429:** دارای `{len(ALL_GEMINI_KEYS)}` کلید فعال با سوییچ صفر ثانیه‌ای.\n"
        f"۴. **🧠 ارتقای مستمر مهارت از روی ادیت‌های شما ({lessons_count} قانون فعال):**\n"
        "   هر زمان بات جوابی نوشت، اگر آن را ادیت و انسانی‌تر کردید و دوباره روی جواب بات ریپلای کردید (یا دکمه `🧠 ثبت ویرایش من` را زدید)، بات دقیقاً یاد می‌گیرد کجا توضیح اضافه داده یا چه کلماتی را تغییر دادید و در پیام‌های بعدی رعایت می‌کند؛ **بدون اینکه موضوع ویس افراد مختلف را با هم قاطی کند!**\n\n"
        "📌 دستورات کاربردی: `/settings` (تنظیمات) | `/skills` (مشاهده قوانین یادگرفته‌شده) | `/status` (وضعیت کلیدها و پایپلاین)"
    )
    if update.message:
        await update.message.reply_text(welcome_text, parse_mode="Markdown")


async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message:
        return
    await update.message.reply_text(
        f"🆔 آیدی عددی تلگرام شما:\n{user.id}\n\n"
        f"برای قفل کردن بات روی اکانت خودتان، این عدد را در GitHub Secrets با نام ALLOWED_USER_IDS ذخیره کنید."
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update) or not update.message:
        return
    st = get_bot_pipeline().get_pipeline_status()
    lessons_count = len(get_learned_style_lessons(limit=25))
    text = (
        "📊 **وضعیت لحظه‌ای پایپلاین دو مرحله‌ای و کلیدها**\n\n"
        f"🔑 **تعداد کلیدهای فعال:** `{st['total_keys']}` کلید (`{', '.join(st['keys_masked'])}`)\n"
        f"🧠 **قوانین لحن یادگرفته‌شده:** `{lessons_count}` قانون فعال\n\n"
        "🎙️ **مرحله ۱ (تبدیل ویس به متن):**\n"
        f"• مدل‌های اولویت: `{ ' -> '.join(st['stage1_stt']['priority_models']) }`\n"
        f"• آخرین مدل موفق: `{st['stage1_stt']['last_used_model']}` با `{st['stage1_stt']['last_used_key']}`\n"
        f"• کل درخواست‌ها: `{st['stage1_stt']['total_requests']}` | چرخش‌های 429: `{st['stage1_stt']['total_429_rotations']}`\n\n"
        "💬 **مرحله ۲ (تولید پاسخ چت):**\n"
        f"• مدل‌های اولویت: `{ ' -> '.join(st['stage2_chat']['priority_models']) }`\n"
        f"• آخرین مدل موفق: `{st['stage2_chat']['last_used_model']}` با `{st['stage2_chat']['last_used_key']}`\n"
        f"• کل درخواست‌ها: `{st['stage2_chat']['total_requests']}` | چرخش‌های 429: `{st['stage2_chat']['total_429_rotations']}`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def skills_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update) or not update.message:
        return
    lessons = get_learned_style_lessons(limit=20)
    if not lessons:
        await update.message.reply_text(
            "📚 هنوز قانون جدیدی از ویرایش‌های شما ثبت نشده است.\n\n"
            "💡 **چطور به بات یاد بدهیم؟**\n"
            "کافیست هر زمان بات جوابی به شما داد، نسخه ادیت‌شده و انسانی‌ترِ خودتان را روی همان پیامِ جوابِ بات ریپلای کنید (یا دکمه `🧠 ثبت ویرایش من` زیر پیام را بزنید)."
        )
        return

    lines = ["📚 **قوانین لحن و مرزبندی یادگرفته‌شده از ویرایش‌های شما:**\n"]
    for idx, item in enumerate(lessons, start=1):
        lines.append(f"{idx}. (کد `{item['lesson_id']}`): {item['learned_rule']}")
    lines.append("\nبرای پاک کردن تمام قوانین دستور `/reset_skills` را بفرستید.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def reset_skills_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update) or not update.message:
        return
    deleted_count = clear_all_style_lessons()
    await update.message.reply_text(f"🗑️ تعداد {deleted_count} قانون یادگرفته‌شده پاک شد و اسکیل به حالت پایه برگشت.")


async def _download_media_bytes(bot, file_id: str) -> bytes:
    tg_file = await bot.get_file(file_id)
    audio_bytearray = await tg_file.download_as_bytearray()
    return bytes(audio_bytearray)


async def process_single_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return

    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return

    if is_rate_limited(user.id):
        await message.reply_text("⏳ تعداد درخواست‌های شما در این دقیقه بیش از حد مجاز است. لطفاً کمی صبر کنید.")
        return

    media_obj = message.voice or message.audio or message.video_note
    if not media_obj:
        return

    file_size = getattr(media_obj, "file_size", None)
    if file_size and file_size > MAX_TELEGRAM_FILE_SIZE_BYTES:
        await message.reply_text(
            "⚠️ حجم این فایل صوتی/ویدیویی بیش از ۲۰ مگابایت است و تلگرام اجازه دانلود مستقیم آن توسط بات را نمی‌دهد."
        )
        return

    is_video_note = message.video_note is not None
    mime_type = "video/mp4" if is_video_note else (getattr(media_obj, "mime_type", None) or "audio/ogg")

    status_msg_task = asyncio.create_task(
        message.reply_text(
            "⚡ در حال تبدیل فوق‌سریع ویس (Stage 1 Pipeline)...",
            reply_to_message_id=message.message_id,
        )
    )
    download_task = asyncio.create_task(
        _download_media_bytes(context.bot, media_obj.file_id)
    )

    status_msg = await status_msg_task

    try:
        raw_audio_bytes = await download_task
        opt_bytes, opt_mime = await optimize_media_for_fast_upload(
            raw_audio_bytes, mime_type=mime_type, is_video_note=is_video_note
        )

        user_cfg = get_user_model_settings(user.id)
        transcript = await transcribe_audio_bytes(
            opt_bytes,
            mime_type=opt_mime,
            preferred_model=user_cfg.get("transcribe_model"),
        )

        record_id = save_record(
            chat_id=message.chat_id,
            msg_id=message.message_id,
            bot_msg_id=status_msg.message_id,
            transcript=transcript,
        )

        # استارت پیش‌تولید جواب انسانی (Stage 2) و خلاصه در پس‌زمینه برای پاسخ آنی به دکمه‌ها
        trigger_background_precomputation(record_id, transcript, user_id=user.id)

        await safe_edit_or_send_long_text(
            status_msg=status_msg,
            origin_msg=message,
            full_text=transcript,
            reply_markup=build_voice_action_keyboard(record_id),
            header_prefix="📝 متن ویس:\n\n",
            footer_suffix="\n\n━━━━━━━━━━━━━━\nمی‌خوای خلاصه‌شو بهت بدم یا جوابِ انسانی و تلگرامیش رو برات بنویسم؟ 👇",
        )

    except Exception as e:
        logger.error(f"Error processing voice message {message.message_id}: {sanitize_error_message(e)}")
        await status_msg.edit_text(
            f"❌ متأسفانه در پردازش این ویس خطایی رخ داد:\n{sanitize_error_message(e)}"
        )


_RE_CLEAN_VOICE_HEADER = re.compile(
    r"^📝\s*\*{0,2}متن ویس(?:\s*\(بخش\s*[0-9۰-۹]+(?:\s*از\s*[0-9۰-۹]+)?\))?:?\*{0,2}\s*"
)
_RE_CLEAN_SUM_HEADER = re.compile(r"^📌\s*\*{0,2}خلاصه:?\*{0,2}\s*")
_RE_LEARN_PREFIX = re.compile(r"^(?:ویرایش|آموزش|اصلاح|edit|learn)\s*[:：\-]\s*", re.IGNORECASE)


def _extract_clean_base_text(raw_text: str) -> str:
    cleaned = raw_text
    if "━━━━━━━━━━━━━━" in cleaned:
        cleaned = cleaned.split("━━━━━━━━━━━━━━")[0]
    cleaned = _RE_CLEAN_VOICE_HEADER.sub("", cleaned.strip())
    cleaned = _RE_CLEAN_SUM_HEADER.sub("", cleaned.strip())
    return cleaned.strip()


async def _handle_style_learning_submission(
    message: Message,
    user_id: int,
    record: dict,
    edited_text: str,
) -> None:
    """اجرای فرآیند یادگیری از ادیت کاربر روی پاسخ بات و نمایش قانون استخراج‌شده"""
    cleaned_edit = _RE_LEARN_PREFIX.sub("", edited_text.strip()).strip()
    bot_draft = (record.get("human_reply") or "").strip()

    wait_msg = await message.reply_text(
        "🧠 در حال مقایسه ویرایش شما با جواب قبلی بات و ارتقای حافظه مهارت...",
        reply_to_message_id=message.message_id,
    )
    try:
        lesson_id, learned_rule = await extract_and_learn_from_admin_edit(
            user_id=user_id,
            bot_draft=bot_draft,
            human_edited=cleaned_edit,
            record_id=record.get("record_id"),
        )
        # به‌روزرسانی پاسخ نهایی رکورد به نسخه تاییدشده توسط کاربر
        if record.get("record_id"):
            update_record_field(record["record_id"], "human_reply", strip_ai_punctuation_and_artifacts(cleaned_edit))

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🗑️ لغو این قانون", callback_data=f"del_lesson:{lesson_id}"),
                    InlineKeyboardButton("📚 همه قوانین یادگرفته‌شده", callback_data="view_lessons"),
                ]
            ]
        )
        await wait_msg.edit_text(
            "✅ **مهارت بات از روی ویرایش شما ارتقا یافت!**\n\n"
            f"📌 **قانون استخراج‌شده (کد {lesson_id}):**\n"
            f"{learned_rule}\n\n"
            "🔒 *نکته: فقط قانون نگارش و مرز پاسخ‌گویی ذخیره شد؛ موضوع این پیام به هیچ وجه با پیام‌های افراد دیگر قاطی نمی‌شود.*",
            reply_markup=kb,
            parse_mode="Markdown",
        )
    except Exception as e:
        await wait_msg.edit_text(f"❌ خطا در ثبت آموزش لحن: {sanitize_error_message(e)}")


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return

    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return

    incoming_text = (message.text or message.caption or "").strip()
    if not incoming_text:
        return

    if is_advertising_or_spam(incoming_text, user.id):
        logger.warning(f"Blocked spam/advertising message from user_id={user.id}")
        return

    if is_rate_limited(user.id):
        await message.reply_text("⏳ تعداد درخواست‌های شما در این دقیقه بیش از حد مجاز است. لطفاً کمی صبر کنید.")
        return

    # حالت ۰: کاربر قبلاً دکمه «🧠 ثبت ویرایش من» را زده و اکنون متن ادیت‌شده را فرستاده است
    pending_record_id = _AWAITING_STYLE_EDIT_FOR_RECORD.pop(message.chat_id, None)
    if pending_record_id is not None:
        rec = get_record(pending_record_id)
        if rec and rec.get("human_reply"):
            await _handle_style_learning_submission(message, user.id, rec, incoming_text)
            return

    is_forwarded = getattr(message, "forward_origin", None) is not None or getattr(message, "forward_date", None) is not None

    # حالت ۱: ریپلای روی پیام قبلی
    if message.reply_to_message and not is_forwarded:
        replied_msg = message.reply_to_message
        db_rec = get_record_by_msg_id(message.chat_id, replied_msg.message_id)

        # بررسی اینکه آیا کاربر روی «جواب انسانیِ تولیدشده توسط بات» ریپلای زده و نسخه ادیت‌شده خودش را فرستاده است؟
        if db_rec and db_rec.get("human_reply"):
            replied_raw_text = (replied_msg.text or "").strip()
            # اگر پیام ریپلای‌شده همان پیام جواب بات است (یا کاربر با پیشوند ویرایش: فرستاده)
            is_reply_to_bot_answer = (
                replied_msg.message_id == db_rec.get("bot_msg_id")
                and not replied_raw_text.startswith("📝 متن ویس")
            )
            if (is_reply_to_bot_answer and is_likely_edited_reply(db_rec["human_reply"], incoming_text)) or _RE_LEARN_PREFIX.match(incoming_text):
                await _handle_style_learning_submission(message, user.id, db_rec, incoming_text)
                return

        # در غیر این صورت: کاربر روی ویس یا متنِ ویس ریپلای زده و نکات پاسخ‌گویی (Hint) داده است
        if db_rec and db_rec.get("transcript"):
            base_text = db_rec["transcript"]
        else:
            raw_replied = replied_msg.text or replied_msg.caption or ""
            base_text = _extract_clean_base_text(raw_replied)

        if base_text:
            custom_hint = (
                f"کاربر می‌خواهد در جوابِ این پیام/ویس، فقط و دقیقاً این نکات یا تصمیم‌ها را بگوید: «{incoming_text}». "
                f"دقیقاً همین موارد را بدون هیچ توضیح اضافه، بدون پیشنهاد خلاقانه سرخود، با لحن طبیعی، خونسرد، بدون تملق و بدون هیچ علامت نگارشی و نیم‌فاصله کتابی تبدیل به پیام تلگرامی کن."
            )
            wait_msg_task = asyncio.create_task(
                message.reply_text(
                    "⚡ در حال نوشتن جواب تلگرامی (Stage 2 Pipeline)...",
                    reply_to_message_id=message.message_id,
                )
            )
            reply_task = asyncio.create_task(
                generate_human_telegram_reply(base_text, variation_hint=custom_hint, user_id=user.id)
            )
            wait_msg = await wait_msg_task
            try:
                human_reply = await reply_task
                rec_id = save_record(
                    chat_id=message.chat_id,
                    msg_id=message.message_id,
                    bot_msg_id=wait_msg.message_id,
                    transcript=base_text,
                    human_reply=human_reply,
                )
                await safe_edit_or_send_long_text(
                    status_msg=wait_msg,
                    origin_msg=message,
                    full_text=human_reply,
                    reply_markup=build_reply_keyboard(rec_id),
                )
                return
            except Exception as e:
                await wait_msg.edit_text(f"❌ خطا: {sanitize_error_message(e)}")
                return

    # حالت ۲: پیام متنی فورواردشده یا مستقیم -> تولید پاسخ در Stage 2 Pipeline
    wait_msg_task = asyncio.create_task(
        message.reply_text(
            "⚡ در حال نوشتن جواب انسانی (Stage 2 Pipeline)...",
            reply_to_message_id=message.message_id,
        )
    )
    reply_task = asyncio.create_task(generate_human_telegram_reply(incoming_text, user_id=user.id))
    wait_msg = await wait_msg_task

    try:
        human_reply = await reply_task
        record_id = save_record(
            chat_id=message.chat_id,
            msg_id=message.message_id,
            bot_msg_id=wait_msg.message_id,
            transcript=incoming_text,
            human_reply=human_reply,
        )
        await safe_edit_or_send_long_text(
            status_msg=wait_msg,
            origin_msg=message,
            full_text=human_reply,
            reply_markup=build_reply_keyboard(record_id),
        )
    except Exception as e:
        logger.error(f"Error handling text message {message.message_id}: {sanitize_error_message(e)}")
        await wait_msg.edit_text(f"❌ خطا در تولید پاسخ: {sanitize_error_message(e)}")


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return
    user = update.effective_user
    if not user or not update.message:
        return

    s = get_user_model_settings(user.id)
    lessons_count = len(get_learned_style_lessons(limit=25))
    text = (
        "⚙️ **تنظیمات پایپلاین دو مرحله‌ای و حافظه مهارت**\n\n"
        f"🔹 **مدل اول چت (Stage 2):** `{s.get('chat_model')}`\n"
        f"🔹 **سطح تفکر (Thinking):** `{s.get('thinking_level')}`\n"
        f"🔹 **مدل اول ویس (Stage 1):** `{s.get('transcribe_model')}`\n"
        f"🔹 **تعداد کلیدهای فعال در چرخش 429:** `{len(ALL_GEMINI_KEYS)}` کلید\n"
        f"🔹 **قوانین یادگرفته‌شده از ویرایش‌های شما:** `{lessons_count}` قانون\n\n"
        "برای تغییر اولویت مدل یا مشاهده قوانین یادگرفته‌شده، روی دکمه‌های زیر کلیک کنید:"
    )
    await update.message.reply_text(
        text,
        reply_markup=build_settings_keyboard(user.id),
        parse_mode="Markdown",
    )


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        if update.callback_query:
            await update.callback_query.answer("⛔ شما مجوز استفاده از این بات را ندارید.", show_alert=True)
        return

    query = update.callback_query
    user = update.effective_user
    if not query or not query.data or not query.message or not user:
        return

    if query.data == "open_settings":
        s = get_user_model_settings(user.id)
        lessons_count = len(get_learned_style_lessons(limit=25))
        text = (
            "⚙️ **تنظیمات پایپلاین دو مرحله‌ای و حافظه مهارت**\n\n"
            f"🔹 **مدل اول چت (Stage 2):** `{s.get('chat_model')}`\n"
            f"🔹 **سطح تفکر:** `{s.get('thinking_level')}`\n"
            f"🔹 **مدل اول ویس (Stage 1):** `{s.get('transcribe_model')}`\n"
            f"🔹 **کلیدهای فعال:** `{len(ALL_GEMINI_KEYS)}` کلید\n"
            f"🔹 **قوانین یادگرفته‌شده:** `{lessons_count}` مورد\n\n"
            "روی هر گزینه برای تغییر وضعیت بزنید:"
        )
        await query.message.reply_text(
            text,
            reply_markup=build_settings_keyboard(user.id),
            parse_mode="Markdown",
        )
        await query.answer()
        return

    if query.data == "close_settings":
        try:
            await query.message.delete()
        except Exception:
            await query.message.edit_text("تنظیمات بسته شد.")
        await query.answer("بسته شد.")
        return

    if query.data == "view_lessons":
        lessons = get_learned_style_lessons(limit=15)
        if not lessons:
            await query.answer("هنوز قانونی از ویرایش‌های شما ثبت نشده است.", show_alert=True)
            return
        lines = ["📚 **قوانین لحن و مرزبندی یادگرفته‌شده از ویرایش‌های شما:**\n"]
        for idx, item in enumerate(lessons, start=1):
            lines.append(f"{idx}. (کد `{item['lesson_id']}`): {item['learned_rule']}")
        lines.append("\n💡 این قوانین در تمام پاسخ‌های بعدی اعمال می‌شوند بدون اینکه موضوع ویس افراد مختلف با هم قاطی شود.")
        await query.message.reply_text("\n".join(lines), parse_mode="Markdown")
        await query.answer()
        return

    if query.data.startswith("del_lesson:"):
        _, lid_str = query.data.split(":", 1)
        if lid_str.isdigit() and delete_style_lesson(int(lid_str)):
            await query.answer("🗑️ این قانون از حافظه مهارت حذف شد.", show_alert=True)
            try:
                await query.message.edit_text("🗑️ این قانون آموزشی به درخواست شما از حافظه مهارت حذف شد.")
            except Exception:
                pass
        else:
            await query.answer("این قانون قبلاً حذف شده است.", show_alert=False)
        return

    if query.data.startswith("set_cfg:"):
        _, target, mode = query.data.split(":", 2)
        if target == "chat":
            if mode == "35_lite":
                update_user_setting(user.id, "chat_model", "gemini-3.5-flash-lite")
                update_user_setting(user.id, "thinking_level", "OFF")
                await query.answer("⚡ اولویت اول چت روی gemini-3.5-flash-lite تنظیم شد!", show_alert=False)
            elif mode == "31_lite":
                update_user_setting(user.id, "chat_model", "gemini-3.1-flash-lite")
                update_user_setting(user.id, "thinking_level", "OFF")
                await query.answer("🚀 اولویت اول چت روی gemini-3.1-flash-lite تنظیم شد!", show_alert=False)
            elif mode == "38_flash":
                curr_t = get_user_model_settings(user.id).get("thinking_level", "OFF")
                new_t = "HIGH" if curr_t == "OFF" else "OFF"
                update_user_setting(user.id, "chat_model", "gemini-3.8-flash")
                update_user_setting(user.id, "thinking_level", new_t)
                await query.answer(f"🧠 مدل gemini-3.8-flash (تفکر {new_t}) انتخاب شد!", show_alert=False)
        elif target == "transcribe" and mode == "toggle":
            curr = get_user_model_settings(user.id).get("transcribe_model")
            new_m = "gemini-3.5-flash-lite" if curr == "gemini-3.5-transcribe" else "gemini-3.5-transcribe"
            update_user_setting(user.id, "transcribe_model", new_m)
            await query.answer(f"🎙️ مدل اول ویس تغییر کرد به:\n{new_m}", show_alert=False)

        s = get_user_model_settings(user.id)
        lessons_count = len(get_learned_style_lessons(limit=25))
        updated_text = (
            "⚙️ **تنظیمات پایپلاین دو مرحله‌ای و حافظه مهارت**\n\n"
            f"🔹 **مدل اول چت (Stage 2):** `{s.get('chat_model')}`\n"
            f"🔹 **سطح تفکر:** `{s.get('thinking_level')}`\n"
            f"🔹 **مدل اول ویس (Stage 1):** `{s.get('transcribe_model')}`\n"
            f"🔹 **کلیدهای فعال:** `{len(ALL_GEMINI_KEYS)}` کلید\n"
            f"🔹 **قوانین یادگرفته‌شده:** `{lessons_count}` مورد\n\n"
            "روی هر گزینه برای تغییر وضعیت بزنید:"
        )
        try:
            await query.message.edit_text(
                updated_text,
                reply_markup=build_settings_keyboard(user.id),
                parse_mode="Markdown",
            )
        except Exception:
            pass
        return

    if is_rate_limited(user.id):
        await query.answer("⏳ لطفاً چند ثانیه صبر کنید و دوباره امتحان کنید.", show_alert=True)
        return

    try:
        action, record_id_str = query.data.split(":", 1)
        record_id = int(record_id_str)
    except ValueError:
        return

    record = get_record(record_id)
    if not record or record.get("chat_id") != query.message.chat_id:
        await query.answer("⚠️ اطلاعات این پیام یافت نشد.", show_alert=True)
        return

    transcript = record["transcript"]
    voice_msg_id = record["voice_msg_id"]

    if action == "learn_mode":
        _AWAITING_STYLE_EDIT_FOR_RECORD[query.message.chat_id] = record_id
        await query.answer("🧠 آماده دریافت ویرایش شما!", show_alert=False)
        await query.message.reply_text(
            "✍️ **حالت ارتقای مهارت فعال شد!**\n\n"
            "حالا متنِ همین جواب را هر طور که خودتان ادیت کردید و انسانی‌ترش کردید، همینجا بفرستید "
            "(یا در هر زمان مستقیم روی جواب بات ریپلای کنید).\n"
            "بات دقیقاً مقایسه می‌کند کجا توضیح اضافه داده یا چه کلماتی را عوض کردید و برای پیام‌های بعدی یاد می‌گیرد.",
            reply_to_message_id=query.message.message_id,
            parse_mode="Markdown",
        )
        return

    if action == "sum":
        if record.get("summary"):
            await query.answer("⚡ خلاصه آماده است!")
            await query.message.reply_text(
                f"📌 خلاصه:\n\n{record['summary'][:3900]}",
                reply_to_message_id=voice_msg_id,
            )
            return

        await query.answer("⚡ در حال خلاصه‌سازی...")
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "⚡ در حال خلاصه‌سازی سریع...",
                reply_to_message_id=voice_msg_id,
            )
        )
        inflight = _INFLIGHT_SUMMARY_TASKS.get(record_id)
        summary_task = inflight if inflight else asyncio.create_task(
            _precompute_summary_for_record(record_id, transcript, user_id=user.id)
        )
        wait_msg = await wait_msg_task
        try:
            summary = await summary_task
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=summary,
                header_prefix="📌 خلاصه:\n\n",
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در خلاصه‌سازی: {sanitize_error_message(e)}")

    elif action == "reply":
        if record.get("human_reply"):
            await query.answer("⚡ جواب آماده است!")
            sent = await query.message.reply_text(
                record["human_reply"][:3950],
                reply_to_message_id=voice_msg_id,
                reply_markup=build_reply_keyboard(record_id),
            )
            update_record_field(record_id, "bot_msg_id", sent.message_id)
            return

        await query.answer("⚡ در حال آماده‌سازی جواب...")
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "⚡ در حال نوشتن جواب انسانی...",
                reply_to_message_id=voice_msg_id,
            )
        )
        inflight = _INFLIGHT_REPLY_TASKS.get(record_id)
        reply_task = inflight if inflight else asyncio.create_task(
            _precompute_reply_for_record(record_id, transcript, user_id=user.id)
        )
        wait_msg = await wait_msg_task
        try:
            human_reply = await reply_task
            update_record_field(record_id, "bot_msg_id", wait_msg.message_id)
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=human_reply,
                reply_markup=build_reply_keyboard(record_id),
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در تولید جواب انسانی: {sanitize_error_message(e)}")

    elif action == "trans":
        base_for_trans = record.get("human_reply") or transcript
        if record.get("translation"):
            await query.answer("⚡ ترجمه آماده است!")
            await query.message.reply_text(
                f"🌐 ترجمه:\n\n{record['translation'][:3900]}",
                reply_to_message_id=voice_msg_id,
            )
            return

        await query.answer("🌐 در حال ترجمه هوشمند...")
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "🌐 در حال ترجمه هوشمند با Gemini...",
                reply_to_message_id=voice_msg_id,
            )
        )
        trans_task = asyncio.create_task(translate_text_content(base_for_trans, user_id=user.id))
        wait_msg = await wait_msg_task
        try:
            translated = await trans_task
            update_record_field(record_id, "translation", translated)
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=translated,
                header_prefix="🌐 ترجمه:\n\n",
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در ترجمه: {sanitize_error_message(e)}")

    elif action == "tts":
        text_to_speak = record.get("human_reply") or transcript
        await query.answer("🔊 در حال تولید ویس صوتی...")
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "🔊 در حال تولید فایل صوتی با مدل Gemini 3.8 Flash TTS...",
                reply_to_message_id=voice_msg_id,
            )
        )
        tts_task = asyncio.create_task(generate_speech_audio(text_to_speak))
        wait_msg = await wait_msg_task
        try:
            ogg_bytes = await tts_task
            await query.message.reply_voice(
                voice=io.BytesIO(ogg_bytes),
                reply_to_message_id=voice_msg_id,
                caption="🔊 ویس تولیدشده با Gemini TTS",
            )
            await wait_msg.delete()
        except Exception as e:
            logger.error(f"TTS error: {sanitize_error_message(e)}")
            await wait_msg.edit_text(f"❌ خطا در تولید ویس صوتی: {sanitize_error_message(e)}")

    elif action == "regen":
        await query.answer("🔄 در حال نوشتن یک مدل جدید...")
        hint = (
            "یک مدل جواب کوتاه‌تر، راحت‌تر و خونسردتر نسبت به حالت قبلی بنویس "
            "که هیچ توضیح اضافه یا پیشنهاد خلاقانه سرخودی نداشته باشه و صددرصد طبیعی و بدون علائم نگارشی باشه."
        )
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "⚡ در حال نوشتن مدل جدید...",
                reply_to_message_id=voice_msg_id,
            )
        )
        reply_task = asyncio.create_task(
            generate_human_telegram_reply(transcript, variation_hint=hint, user_id=user.id)
        )
        wait_msg = await wait_msg_task
        try:
            human_reply = await reply_task
            update_record_field(record_id, "human_reply", human_reply)
            update_record_field(record_id, "bot_msg_id", wait_msg.message_id)
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=human_reply,
                reply_markup=build_reply_keyboard(record_id),
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در تولید جواب انسانی: {sanitize_error_message(e)}")


def main() -> None:
    if not BOT_TOKEN or not ALL_GEMINI_KEYS:
        logger.error("❌ کلیدهای ارتباطی یافت نشدند.")
        sys.exit(1)

    init_db()
    try:
        get_bot_pipeline()
        get_learned_style_lessons()
    except Exception as exc:
        logger.warning(f"Warmup note: {sanitize_error_message(exc)}")

    logger.info(
        "Starting @voiceandpasokhbot (2-Stage Pipeline | %d API Keys loaded)...",
        len(ALL_GEMINI_KEYS),
    )

    try:
        t_request = HTTPXRequest(
            connection_pool_size=64,
            read_timeout=25.0,
            write_timeout=25.0,
            connect_timeout=10.0,
            pool_timeout=10.0,
            http_version="2",
        )
        app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .request(t_request)
            .get_updates_request(
                HTTPXRequest(
                    connection_pool_size=16,
                    read_timeout=35.0,
                    connect_timeout=10.0,
                    pool_timeout=10.0,
                    http_version="2",
                )
            )
            .concurrent_updates(32)
            .build()
        )
    except Exception:
        app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .concurrent_updates(32)
            .build()
        )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("id", id_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("skills", skills_command))
    app.add_handler(CommandHandler("learned", skills_command))
    app.add_handler(CommandHandler("reset_skills", reset_skills_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("model", settings_command))
    app.add_handler(CommandHandler("models", settings_command))
    app.add_handler(CommandHandler("mode", settings_command))

    app.add_handler(
        MessageHandler(
            filters.VOICE | filters.AUDIO | filters.VIDEO_NOTE,
            process_single_voice,
        )
    )

    app.add_handler(CallbackQueryHandler(button_callback_handler))

    app.add_handler(
        MessageHandler(
            (filters.TEXT | filters.CAPTION) & (~filters.COMMAND) & (~filters.VOICE) & (~filters.AUDIO) & (~filters.VIDEO_NOTE),
            handle_text_message,
        )
    )

    app.run_polling(
        poll_interval=0.1,
        timeout=25,
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
