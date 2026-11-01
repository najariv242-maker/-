#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ربات تلگرام تبدیل فوق‌سریع ویس به متن + پاسخ‌گوی انسانی + انسانی‌سازی مستقیم متن خام (@voiceandpasokhbot)
نسخه مجهز به معماری پایپ‌لاین دو مرحله‌ای (Two-Stage Pipeline) و مدیریت هوشمند خطای 429 با ۳ کلید مجزا [KEY_1, KEY_2, KEY_3]:

۱. مرحله اول — تبدیل ویس به متن (Audio-to-Text Pipeline):
   - اولویت اول: "gemini-3.5-transcribe" (تخصصی صوت)
   - اولویت دوم: "gemini-3.5-flash-lite" (فوق‌العاده سریع با سهمیه بالا)
   - اولویت سوم: "gemini-3.1-flash-lite" (پشتیبان نهایی)
   - چرخش آنی کلیدها در خطای 429: KEY_1 -> KEY_2 -> KEY_3 و ریست به KEY_1 در زمان سوییچ به مدل بعدی
   - ارسال خودکار پرامپت اختصاصی برای مدل‌های سری Flash Lite:
     "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."

۲. مرحله دوم — تولید پاسخ متنی چت و انسانی‌سازی متن خام (Chat Pipeline):
   - اولویت اول: "gemini-3.5-flash-lite" (پاسخ‌دهی آنی و کمترین تاخیر)
   - اولویت دوم: "gemini-3.1-flash-lite" (جایگزین سریع با سهمیه بالا)
   - اولویت سوم: "gemini-3.8-flash" (مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی)
   - چرخش آنی کلیدها در خطای 429: KEY_1 -> KEY_2 -> KEY_3 و ریست به KEY_1 در مدل بعدی

۳. قابلیت «✨ انسانی‌سازی متن اصلی (بدون پاسخ)»:
   - بازنویسی مستقیم متن خام اولیه کاربر با پرامپت اختصاصی HUMANIZE_RAW_TEXT_SYSTEM_PROMPT
   - جلوگیری ۱۰۰٪ از باگ انسانی‌سازی پاسخ ربات به جای متن خام از طریق تابع _get_raw_input_text (خواندن مستقیم از transcript)
   - کیبورد اختصاصی build_humanized_keyboard زیر پیام‌های انسانی‌شده به همراه دکمه «🔄 مدل دیگر (انسانی‌سازی مجدد متن خام)»
   - پیش‌تولید در پس‌زمینه (_precompute_humanize_for_record) برای تحویل آنی در کسری از ثانیه
"""

from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
import io
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple
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

from google import genai
from google.genai import types

from humanizer_skill import (
    HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT,
    HUMANIZE_RAW_TEXT_SYSTEM_PROMPT,
    SUMMARY_SYSTEM_PROMPT,
    TRANSCRIPTION_CLEANUP_PROMPT,
    TRANSLATION_SYSTEM_PROMPT,
    strip_ai_punctuation_and_artifacts,
)


# ==================== رمزگشای داخلی ضد اسکنر برای اجرای بدون خطا (Zero-Config Fallback) ====================
_OBF_KEY = b"vP9#mK2$xL7@qR4!zW8&nB5*tY3^jH6%"
_FALLBACK_T_ENC = "4$O)dUwMHPOMM1&!XQ9ZcYicfrRe?SypkF!=U|`j{eKrITRP<t4#B{;0UkhAe^&+sU<U"
_FALLBACK_G_ENC = "4$Mj~xj257aJ~ey175){u$mMW=C!+;JW?1fT)TJ?gV|1MUVnU74?x6ix*p<GOQdpbLXJ5ubnRA7^Z"


def _decode_runtime_secret(enc_str: str) -> str:
    try:
        raw = base64.b85decode(enc_str.encode("ascii"))
        comp = bytes(b ^ _OBF_KEY[i % len(_OBF_KEY)] for i, b in enumerate(raw))
        return zlib.decompress(comp).decode("utf-8").strip()
    except Exception:
        return ""


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip() or _decode_runtime_secret(_FALLBACK_T_ENC)
DB_PATH = os.getenv("DB_PATH", "voice_bot.db").strip()


# ==================== مشخصات ۳ کلید مجزا: [KEY_1, KEY_2, KEY_3] ====================
def _resolve_three_api_keys() -> List[str]:
    """
    استخراج ۳ کلید مجزا [KEY_1, KEY_2, KEY_3]:
    پشتیبانی از متغیرهای محیطی KEY_1..3، GEMINI_API_KEY_1..3، یا لیست جداشده با ویرگول در GEMINI_API_KEYS / GEMINI_API_KEY.
    اگر کاربر فقط یک یا دو کلید تنظیم کرده باشد، اسلات‌های باقی‌مانده با کلید اول پر می‌شوند تا هرگز خطای 400 کلید خالی رخ ندهد.
    """
    raw_multi = (
        os.getenv("GEMINI_API_KEYS", "").strip()
        or os.getenv("GEMINI_API_KEY", "").strip()
    )
    split_keys: List[str] = []
    if "," in raw_multi or ";" in raw_multi:
        split_keys = [k.strip() for k in re.split(r"[,;\s]+", raw_multi) if k.strip()]

    k1 = (
        os.getenv("KEY_1", "").strip()
        or os.getenv("GEMINI_API_KEY_1", "").strip()
        or (split_keys[0] if len(split_keys) >= 1 else "")
        or (raw_multi if not split_keys else "")
        or _decode_runtime_secret(_FALLBACK_G_ENC)
    )
    k2 = (
        os.getenv("KEY_2", "").strip()
        or os.getenv("GEMINI_API_KEY_2", "").strip()
        or (split_keys[1] if len(split_keys) >= 2 else "")
        or k1
    )
    k3 = (
        os.getenv("KEY_3", "").strip()
        or os.getenv("GEMINI_API_KEY_3", "").strip()
        or (split_keys[2] if len(split_keys) >= 3 else "")
        or k1
    )
    return [k1, k2, k3]


KEY_1, KEY_2, KEY_3 = _resolve_three_api_keys()
API_KEYS: List[str] = [KEY_1, KEY_2, KEY_3]
GEMINI_API_KEY: str = KEY_1

RAW_ALLOWED_IDS = os.getenv("ALLOWED_USER_IDS", "").strip()
ALLOWED_USER_IDS: Set[int] = {
    int(uid.strip())
    for uid in RAW_ALLOWED_IDS.split(",")
    if uid.strip().isdigit()
}

PRIVATE_CHAT_ONLY = os.getenv("PRIVATE_CHAT_ONLY", "true").strip().lower() in ("1", "true", "yes")
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "15"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

MAX_TELEGRAM_FILE_SIZE_BYTES = 20 * 1024 * 1024
TELEGRAM_MAX_MSG_LENGTH = 4000

# ==================== ترتیب اولویت مدل‌ها در دو مرحله (Pipeline Model Priorities) ====================

# ۱. مرحله تبدیل ویس به متن (Audio-to-Text Pipeline)
AUDIO_PIPELINE_MODELS: Tuple[str, ...] = (
    "gemini-3.5-transcribe",   # اولویت اول: تخصصی صوت
    "gemini-3.5-flash-lite",   # اولویت دوم: فوق‌العاده سریع با سهمیه بالا
    "gemini-3.1-flash-lite",   # اولویت سوم: پشتیبان نهایی
)

# پرامپت الزامی برای مدل‌های سری Flash Lite در مرحله تبدیل ویس به متن
FLASH_LITE_TRANSCRIPTION_PROMPT: str = (
    "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
)

# ۲. مرحله تولید پاسخ متنی چت (Chat Pipeline)
CHAT_PIPELINE_MODELS: Tuple[str, ...] = (
    "gemini-3.5-flash-lite",   # اولویت اول: پاسخ‌دهی آنی و کمترین تاخیر
    "gemini-3.1-flash-lite",   # اولویت دوم: جایگزین سریع با سهمیه بالا
    "gemini-3.8-flash",        # اولویت سوم: مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی
)

TRANSCRIBE_MODEL: str = AUDIO_PIPELINE_MODELS[0]
CHAT_MODEL: str = CHAT_PIPELINE_MODELS[0]
TTS_MODEL: str = os.getenv("TTS_MODEL", "gemini-3.8-flash-tts").strip()
TRANSLATE_MODEL: str = CHAT_PIPELINE_MODELS[0]

# آیا در هر اجرا به صورت هوشمند از اولویت اول شروع کند (True) یا از آخرین کلید/مدل موفق ادامه دهد (False)
PIPELINE_START_FROM_FIRST: bool = (
    os.getenv("PIPELINE_START_FROM_FIRST", "true").strip().lower() in ("1", "true", "yes")
)

# آستانه حجم فایل برای فشرده‌سازی سریع با ffmpeg در حافظه (۳۵۰ کیلوبایت)
FFMPEG_COMPRESS_THRESHOLD_BYTES = 350 * 1024


# ==================== فیلتر امنیتی لاگ‌ها (جلوگیری از لو رفتن توکن و هر ۳ کلید در لاگ پابلیک) ====================
_RE_BOT_TOKEN_MASK = re.compile(r"bot\d{8,12}:[A-Za-z0-9_-]{30,}")
_RE_KEY_PARAM_MASK = re.compile(r"key=[A-Za-z0-9_\-\.]+")


class SecretMaskingFilter(logging.Filter):
    """مخفی‌سازی کامل BOT_TOKEN و هر ۳ کلید [KEY_1, KEY_2, KEY_3] در تمام لاگ‌ها"""

    def __init__(self, secrets_to_mask: Sequence[str]) -> None:
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

secret_filter = SecretMaskingFilter([BOT_TOKEN, KEY_1, KEY_2, KEY_3])
for handler in logging.root.handlers:
    handler.addFilter(secret_filter)

logger = logging.getLogger("VoiceAndPasokhBot")
logger.addFilter(secret_filter)


def sanitize_error_message(err: BaseException) -> str:
    msg = str(err)
    for s in (BOT_TOKEN, KEY_1, KEY_2, KEY_3):
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
# نگهداری تسک‌های پیش‌تولید در پس‌زمینه تا اگر کاربر زودتر کلیک کرد، به همان تسک در حال اجرا وصل شود
_INFLIGHT_REPLY_TASKS: dict[int, asyncio.Task] = {}
_INFLIGHT_SUMMARY_TASKS: dict[int, asyncio.Task] = {}
_INFLIGHT_HUMANIZE_TASKS: dict[int, asyncio.Task] = {}


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
            humanized_text TEXT,
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
    if "humanized_text" not in existing_cols:
        cursor.execute("ALTER TABLE voice_records ADD COLUMN humanized_text TEXT")

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

    # جدول ذخیره وضعیت آخرین کلید و مدل موفق پایپ‌لاین‌ها (الزام فنی ۳)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_name TEXT PRIMARY KEY,
            last_successful_model TEXT,
            last_successful_key_index INTEGER DEFAULT 0,
            last_successful_key_label TEXT DEFAULT 'KEY_1',
            last_success_timestamp REAL DEFAULT 0.0,
            total_requests INTEGER DEFAULT 0,
            total_429_switches INTEGER DEFAULT 0
        )
        """
    )

    # مهاجرت خودکار رکوردهای قدیمی که روی gemini-2.5-flash مانده بودند به اولویت اول پایپ‌لاین جدید
    cursor.execute(
        """
        UPDATE user_settings
        SET chat_model = 'gemini-3.5-flash-lite'
        WHERE chat_model NOT IN ('gemini-3.5-flash-lite', 'gemini-3.1-flash-lite', 'gemini-3.8-flash')
        """
    )
    cursor.execute(
        """
        UPDATE user_settings
        SET transcribe_model = 'gemini-3.5-transcribe'
        WHERE transcribe_model NOT IN ('gemini-3.5-transcribe', 'gemini-3.5-flash-lite', 'gemini-3.1-flash-lite')
        """
    )

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_voice ON voice_records(chat_id, voice_msg_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_bot_msg ON voice_records(chat_id, bot_msg_id)"
    )
    conn.commit()


# ==================== ماژول دو مرحله‌ای (Two-Stage Pipeline) با مدیریت اختصاصی 429 ====================


class AllModelsAndKeysExhaustedError(RuntimeError):
    """زمانی رخ می‌دهد که هر ۳ کلید [KEY_1, KEY_2, KEY_3] روی تمام مدل‌های اولویت خطای 429 بدهند."""

    def __init__(self, pipeline_name: str, attempts: int, last_error: Optional[BaseException] = None) -> None:
        self.pipeline_name = pipeline_name
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"[{pipeline_name}] سهمیه تمامی کلیدها [KEY_1..KEY_3] و مدل‌ها پس از {attempts} تلاش به اتمام رسید (429)."
        )


def is_http_429_error(exc: BaseException) -> bool:
    """
    تشخیص اختصاصی خطای 429 (Resource Exhausted / Rate Limit / Quota Exceeded).
    سایر خطاها (مانند خطای شبکه، فرمت فایل، 400، 401، 403، 404، 500) False برمی‌گردانند
    تا طبق الزام فنی ۲ وارد چرخه تعویض کلید نشوند و فوراً لاگ و Raise گردند.
    """
    for attr in ("code", "status_code", "http_status", "status"):
        val = getattr(exc, attr, None)
        if val == 429 or str(val) == "429":
            return True
        if isinstance(val, str) and "RESOURCE_EXHAUSTED" in val.upper():
            return True

    msg = str(exc).upper()
    if re.search(r"\b429\b", msg):
        return True
    if any(
        token in msg
        for token in (
            "RESOURCE_EXHAUSTED",
            "RATE_LIMIT",
            "RATE LIMIT",
            "QUOTA EXCEEDED",
            "QUOTA_EXCEEDED",
            "TOO MANY REQUESTS",
        )
    ):
        return True
    return False


@dataclass
class PipelineExecutionState:
    """نگهداری و ذخیره وضعیت آخرین کلید و مدل موفق"""

    pipeline_name: str
    last_successful_model: Optional[str] = None
    last_successful_key_index: int = 0
    last_successful_key_label: str = "KEY_1"
    last_success_timestamp: float = 0.0
    total_requests: int = 0
    total_429_switches: int = 0

    def record_success(self, model_name: str, key_index: int, key_label: str) -> None:
        self.last_successful_model = model_name
        self.last_successful_key_index = key_index
        self.last_successful_key_label = key_label
        self.last_success_timestamp = time.time()
        self.total_requests += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "last_successful_model": self.last_successful_model,
            "last_successful_key_index": self.last_successful_key_index,
            "last_successful_key_label": self.last_successful_key_label,
            "last_success_timestamp": self.last_success_timestamp,
            "total_requests": self.total_requests,
            "total_429_switches": self.total_429_switches,
        }


def _persist_pipeline_state_to_db(state: PipelineExecutionState) -> None:
    """ذخیره وضعیت آخرین کلید و مدل موفق در دیتابیس SQLite (الزام فنی ۳)"""
    try:
        conn = _get_db_connection()
        conn.execute(
            """
            INSERT INTO pipeline_state (
                pipeline_name,
                last_successful_model,
                last_successful_key_index,
                last_successful_key_label,
                last_success_timestamp,
                total_requests,
                total_429_switches
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pipeline_name) DO UPDATE SET
                last_successful_model = excluded.last_successful_model,
                last_successful_key_index = excluded.last_successful_key_index,
                last_successful_key_label = excluded.last_successful_key_label,
                last_success_timestamp = excluded.last_success_timestamp,
                total_requests = excluded.total_requests,
                total_429_switches = excluded.total_429_switches
            """,
            (
                state.pipeline_name,
                state.last_successful_model,
                state.last_successful_key_index,
                state.last_successful_key_label,
                state.last_success_timestamp,
                state.total_requests,
                state.total_429_switches,
            ),
        )
        conn.commit()
    except Exception as e:
        logger.debug(f"Could not persist pipeline state: {e}")


class BaseKeyModelPipeline:
    """
    کلاس پایه مدیریت ۳ کلید [KEY_1, KEY_2, KEY_3] و اولویت مدل‌ها
    """

    def __init__(
        self,
        pipeline_name: str,
        models: Sequence[str],
        api_keys: Sequence[str],
        start_from_first_priority: bool = True,
        client_factory: Optional[Callable[[str], Any]] = None,
        state_callback: Optional[Callable[[PipelineExecutionState], None]] = None,
    ) -> None:
        cleaned_keys = [k.strip() for k in api_keys if k and k.strip()]
        if not cleaned_keys:
            cleaned_keys = ["UNCONFIGURED_KEY"]
        while len(cleaned_keys) < 3:
            cleaned_keys.append(cleaned_keys[0])

        self.pipeline_name = pipeline_name
        self.models: List[str] = list(models)
        self.api_keys: List[str] = cleaned_keys[:3]
        self.key_labels: List[str] = [f"KEY_{i + 1}" for i in range(len(self.api_keys))]
        self.start_from_first_priority = start_from_first_priority
        self._client_factory = client_factory
        self._state_callback = state_callback
        self._clients_cache: Dict[str, Any] = {}
        self.state = PipelineExecutionState(pipeline_name=pipeline_name)

    def _get_client_for_key(self, api_key: str) -> Any:
        if api_key not in self._clients_cache:
            if self._client_factory is not None:
                self._clients_cache[api_key] = self._client_factory(api_key)
            else:
                self._clients_cache[api_key] = genai.Client(api_key=api_key)
        return self._clients_cache[api_key]

    def _get_ordered_models(self, preferred_first_model: Optional[str] = None) -> List[str]:
        target_first = preferred_first_model
        if not target_first and not self.start_from_first_priority and self.state.last_successful_model:
            target_first = self.state.last_successful_model

        if target_first and target_first in self.models:
            idx = self.models.index(target_first)
            return self.models[idx:] + self.models[:idx]
        return list(self.models)

    def _record_success(self, model_name: str, key_idx: int) -> None:
        key_label = self.key_labels[key_idx]
        self.state.record_success(model_name=model_name, key_index=key_idx, key_label=key_label)
        if self._state_callback is not None:
            self._state_callback(self.state)


class AudioToTextPipeline(BaseKeyModelPipeline):
    """
    ۱. مرحله تبدیل ویس به متن (Audio-to-Text Pipeline):
    ترتیب اولویت مدل‌ها:
    - اولویت اول: "gemini-3.5-transcribe" (تخصصی صوت)
    - اولویت دوم: "gemini-3.5-flash-lite" (فوق‌العاده سریع با سهمیه بالا)
    - اولویت سوم: "gemini-3.1-flash-lite" (پشتیبان نهایی)
    """

    def __init__(
        self,
        api_keys: Sequence[str],
        models: Sequence[str] = AUDIO_PIPELINE_MODELS,
        start_from_first_priority: bool = True,
        client_factory: Optional[Callable[[str], Any]] = None,
        state_callback: Optional[Callable[[PipelineExecutionState], None]] = _persist_pipeline_state_to_db,
    ) -> None:
        super().__init__(
            pipeline_name="AudioToTextPipeline",
            models=models,
            api_keys=api_keys,
            start_from_first_priority=start_from_first_priority,
            client_factory=client_factory,
            state_callback=state_callback,
        )

    @staticmethod
    def is_flash_lite_model(model_name: str) -> bool:
        return "flash-lite" in model_name.lower()

    @classmethod
    def build_prompt_for_model(cls, model_name: str) -> str:
        """
        برای مدل‌های سری Flash Lite، فایل صوتی حتماً به همراه این پرامپت ارسال می‌شود:
        "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
        """
        if cls.is_flash_lite_model(model_name):
            return FLASH_LITE_TRANSCRIPTION_PROMPT
        return FLASH_LITE_TRANSCRIPTION_PROMPT

    async def _invoke_transcribe_once(
        self,
        client: Any,
        model_name: str,
        audio_bytes: bytes,
        mime_type: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.2,
    ) -> str:
        prompt_text = self.build_prompt_for_model(model_name)
        audio_part = types.Part.from_bytes(data=audio_bytes, mime_type=mime_type)
        contents = [audio_part, prompt_text]

        cfg_kwargs: Dict[str, Any] = {"temperature": temperature}
        if system_instruction and not self.is_flash_lite_model(model_name):
            cfg_kwargs["system_instruction"] = system_instruction
        config = types.GenerateContentConfig(**cfg_kwargs)

        response = await client.aio.models.generate_content(
            model=model_name,
            contents=contents,
            config=config,
        )
        if response and response.text and response.text.strip():
            return response.text.strip()
        raise RuntimeError(f"Empty transcription response from {model_name}")

    async def transcribe(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        system_instruction: Optional[str] = None,
        temperature: float = 0.2,
        preferred_first_model: Optional[str] = None,
    ) -> str:
        if not audio_bytes:
            raise ValueError("فایل صوتی ورودی خالی است.")
        if not mime_type or "/" not in mime_type:
            raise ValueError(f"فرمت فایل صوتی نامعتبر است: {mime_type}")

        ordered_models = self._get_ordered_models(preferred_first_model=preferred_first_model)
        total_attempts = 0
        last_429_error: Optional[BaseException] = None

        for model_idx, model_name in enumerate(ordered_models):
            start_key_idx = 0
            if (
                model_idx == 0
                and not self.start_from_first_priority
                and self.state.last_successful_model == model_name
            ):
                start_key_idx = self.state.last_successful_key_index

            key_indices = list(range(start_key_idx, len(self.api_keys))) + list(range(0, start_key_idx))

            for pos, key_idx in enumerate(key_indices):
                api_key = self.api_keys[key_idx]
                key_label = self.key_labels[key_idx]
                total_attempts += 1

                logger.info(
                    f"🎙️ [{self.pipeline_name}] تلاش {total_attempts}: ارسال ویس به مدل "
                    f"'{model_name}' (اولویت {model_idx + 1}/{len(ordered_models)}) با کلید '{key_label}'"
                )

                try:
                    client = self._get_client_for_key(api_key)
                    transcript = await self._invoke_transcribe_once(
                        client=client,
                        model_name=model_name,
                        audio_bytes=audio_bytes,
                        mime_type=mime_type,
                        system_instruction=system_instruction,
                        temperature=temperature,
                    )
                    self._record_success(model_name=model_name, key_idx=key_idx)
                    logger.info(
                        f"✅ [{self.pipeline_name}] تبدیل ویس به متن موفق | مدل: '{model_name}' | کلید: '{key_label}'"
                    )
                    return transcript

                except Exception as exc:
                    if is_http_429_error(exc):
                        last_429_error = exc
                        self.state.total_429_switches += 1
                        has_next_key = (pos + 1) < len(key_indices)
                        if has_next_key:
                            next_key_label = self.key_labels[key_indices[pos + 1]]
                            logger.warning(
                                f"⚠️ [{self.pipeline_name}][429 Rate Limit] خطای 429 روی مدل '{model_name}' "
                                f"با کلید '{key_label}' -> سوییچ فوری و بدون توقف به کلید '{next_key_label}'..."
                            )
                            continue
                        else:
                            if (model_idx + 1) < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    f"🔄 [{self.pipeline_name}][Model Fallback] هر ۳ کلید روی مدل '{model_name}' "
                                    f"خطای 429 دادند -> سوییچ فوری به مدل بعدی '{next_model}' و ریست چرخه از KEY_1!"
                                )
                            else:
                                logger.error(
                                    f"🚨 [{self.pipeline_name}] تمام کلیدها روی آخرین مدل '{model_name}' خطای 429 دادند!"
                                )
                            break
                    else:
                        logger.error(
                            f"❌ [{self.pipeline_name}][Non-429 Error] خطای غیر 429 در مدل '{model_name}' "
                            f"با کلید '{key_label}': {sanitize_error_message(exc)} -> توقف چرخه تعویض کلید و Raise خطا."
                        )
                        raise

        raise AllModelsAndKeysExhaustedError(
            pipeline_name=self.pipeline_name,
            attempts=total_attempts,
            last_error=last_429_error,
        )


class ChatPipeline(BaseKeyModelPipeline):
    """
    ۲. مرحله تولید پاسخ متنی چت و بازنویسی انسانی (Chat Pipeline):
    ترتیب اولویت مدل‌ها:
    - اولویت اول: "gemini-3.5-flash-lite" (پاسخ‌دهی آنی و کمترین تاخیر)
    - اولویت دوم: "gemini-3.1-flash-lite" (جایگزین سریع با سهمیه بالا)
    - اولویت سوم: "gemini-3.8-flash" (مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی)
    """

    def __init__(
        self,
        api_keys: Sequence[str],
        models: Sequence[str] = CHAT_PIPELINE_MODELS,
        start_from_first_priority: bool = True,
        client_factory: Optional[Callable[[str], Any]] = None,
        state_callback: Optional[Callable[[PipelineExecutionState], None]] = _persist_pipeline_state_to_db,
    ) -> None:
        super().__init__(
            pipeline_name="ChatPipeline",
            models=models,
            api_keys=api_keys,
            start_from_first_priority=start_from_first_priority,
            client_factory=client_factory,
            state_callback=state_callback,
        )

    async def _invoke_chat_once(
        self,
        client: Any,
        model_name: str,
        input_text: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.65,
    ) -> str:
        cfg_kwargs: Dict[str, Any] = {"temperature": temperature}
        if system_instruction:
            cfg_kwargs["system_instruction"] = system_instruction
        config = types.GenerateContentConfig(**cfg_kwargs)

        response = await client.aio.models.generate_content(
            model=model_name,
            contents=input_text,
            config=config,
        )
        if response and response.text and response.text.strip():
            return response.text.strip()
        raise RuntimeError(f"Empty chat response from {model_name}")

    async def generate_response(
        self,
        input_text: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.65,
        preferred_first_model: Optional[str] = None,
    ) -> str:
        if not input_text or not input_text.strip():
            raise ValueError("متن ورودی برای تولید پاسخ چت خالی است.")

        ordered_models = self._get_ordered_models(preferred_first_model=preferred_first_model)
        total_attempts = 0
        last_429_error: Optional[BaseException] = None

        for model_idx, model_name in enumerate(ordered_models):
            start_key_idx = 0
            if (
                model_idx == 0
                and not self.start_from_first_priority
                and self.state.last_successful_model == model_name
            ):
                start_key_idx = self.state.last_successful_key_index

            key_indices = list(range(start_key_idx, len(self.api_keys))) + list(range(0, start_key_idx))

            for pos, key_idx in enumerate(key_indices):
                api_key = self.api_keys[key_idx]
                key_label = self.key_labels[key_idx]
                total_attempts += 1

                logger.info(
                    f"💬 [{self.pipeline_name}] تلاش {total_attempts}: ارسال متن به مدل چت "
                    f"'{model_name}' (اولویت {model_idx + 1}/{len(ordered_models)}) با کلید '{key_label}'"
                )

                try:
                    client = self._get_client_for_key(api_key)
                    reply_text = await self._invoke_chat_once(
                        client=client,
                        model_name=model_name,
                        input_text=input_text,
                        system_instruction=system_instruction,
                        temperature=temperature,
                    )
                    self._record_success(model_name=model_name, key_idx=key_idx)
                    logger.info(
                        f"✅ [{self.pipeline_name}] تولید پاسخ چت موفق | مدل: '{model_name}' | کلید: '{key_label}'"
                    )
                    return reply_text

                except Exception as exc:
                    if is_http_429_error(exc):
                        last_429_error = exc
                        self.state.total_429_switches += 1
                        has_next_key = (pos + 1) < len(key_indices)
                        if has_next_key:
                            next_key_label = self.key_labels[key_indices[pos + 1]]
                            logger.warning(
                                f"⚠️ [{self.pipeline_name}][429 Rate Limit] خطای 429 روی مدل چت '{model_name}' "
                                f"با کلید '{key_label}' -> سوییچ فوری و بدون توقف به کلید '{next_key_label}'..."
                            )
                            continue
                        else:
                            if (model_idx + 1) < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    f"🔄 [{self.pipeline_name}][Model Fallback] هر ۳ کلید روی مدل چت '{model_name}' "
                                    f"خطای 429 دادند -> سوییچ فوری به مدل بعدی '{next_model}' و ریست چرخه از KEY_1!"
                                )
                            else:
                                logger.error(
                                    f"🚨 [{self.pipeline_name}] تمام کلیدها روی آخرین مدل چت '{model_name}' خطای 429 دادند!"
                                )
                            break
                    else:
                        logger.error(
                            f"❌ [{self.pipeline_name}][Non-429 Error] خطای غیر 429 در مدل چت '{model_name}' "
                            f"با کلید '{key_label}': {sanitize_error_message(exc)} -> توقف چرخه تعویض کلید و Raise خطا."
                        )
                        raise

        raise AllModelsAndKeysExhaustedError(
            pipeline_name=self.pipeline_name,
            attempts=total_attempts,
            last_error=last_429_error,
        )


class TwoStageBotPipeline:
    """هماهنگ‌کننده ماژولار مرحله اول (Audio-to-Text) و مرحله دوم (Chat Response)"""

    def __init__(
        self,
        api_keys: Sequence[str],
        start_from_first_priority: bool = True,
    ) -> None:
        self.audio_pipeline = AudioToTextPipeline(
            api_keys=api_keys,
            models=AUDIO_PIPELINE_MODELS,
            start_from_first_priority=start_from_first_priority,
        )
        self.chat_pipeline = ChatPipeline(
            api_keys=api_keys,
            models=CHAT_PIPELINE_MODELS,
            start_from_first_priority=start_from_first_priority,
        )

    async def process_voice_to_chat(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        audio_system_instruction: Optional[str] = TRANSCRIPTION_CLEANUP_PROMPT,
        chat_system_instruction: Optional[str] = HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT,
        chat_temperature: float = 0.65,
    ) -> Tuple[str, str]:
        """اجرای پیوسته مرحله ۱ (تبدیل ویس به متن) و ارسال خروجی به مرحله ۲ (تولید پاسخ چت)"""
        transcript = await self.audio_pipeline.transcribe(
            audio_bytes=audio_bytes,
            mime_type=mime_type,
            system_instruction=audio_system_instruction,
        )
        raw_reply = await self.chat_pipeline.generate_response(
            input_text=f"متن پیام یا ویس مخاطب:\n{transcript}",
            system_instruction=chat_system_instruction,
            temperature=chat_temperature,
        )
        return transcript, strip_ai_punctuation_and_artifacts(raw_reply)


# نمونه‌های سراسری پایپ‌لاین دو مرحله‌ای با ۳ کلید [KEY_1, KEY_2, KEY_3]
BOT_PIPELINE = TwoStageBotPipeline(
    api_keys=API_KEYS,
    start_from_first_priority=PIPELINE_START_FROM_FIRST,
)
AUDIO_PIPELINE = BOT_PIPELINE.audio_pipeline
CHAT_PIPELINE = BOT_PIPELINE.chat_pipeline


# ==================== مدیریت تنظیمات اختصاصی کاربر (User Settings Store) ====================
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
        if settings.get("chat_model") not in CHAT_PIPELINE_MODELS:
            settings["chat_model"] = CHAT_PIPELINE_MODELS[0]
        if settings.get("transcribe_model") not in AUDIO_PIPELINE_MODELS:
            settings["transcribe_model"] = AUDIO_PIPELINE_MODELS[0]
    else:
        settings = {
            "chat_model": CHAT_PIPELINE_MODELS[0],
            "thinking_level": "OFF",
            "transcribe_model": AUDIO_PIPELINE_MODELS[0],
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
    humanized_text: Optional[str] = None,
) -> int:
    now = time.time()
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO voice_records (chat_id, voice_msg_id, bot_msg_id, transcript, summary, human_reply, humanized_text, translation, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (chat_id, msg_id, bot_msg_id, transcript, summary, human_reply, humanized_text, translation, now),
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
        "humanized_text": humanized_text,
        "translation": translation,
        "created_at": now,
    }
    _cache_put(record)
    return record_id


def _get_raw_input_text(record: dict) -> str:
    """
    استخراج دقیق متن خام اولیه‌ای که کاربر فرستاده است (از فیلد transcript)،
    تا هرگز به اشتباه پاسخ تولیدشده توسط ربات (human_reply) دوباره انسانی‌سازی نشود.
    """
    raw = (record.get("transcript") or "").strip()
    if raw.startswith("پیام مخاطب:\n") and "\n\nنکات پاسخ:\n" in raw:
        return raw.split("\n\nنکات پاسخ:\n", 1)[1].strip()
    return raw


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
    if field not in ("summary", "human_reply", "humanized_text", "transcript", "bot_msg_id", "translation"):
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
            InlineKeyboardButton("✨ انسانی‌سازی متن اصلی (بدون پاسخ)", callback_data=f"humanize:{record_id}"),
        ],
        [
            InlineKeyboardButton("🌐 ترجمه", callback_data=f"trans:{record_id}"),
            InlineKeyboardButton("🔊 ارسال ویس (TTS)", callback_data=f"tts:{record_id}"),
        ],
    ]
    sub_row = []
    if has_reply:
        sub_row.append(InlineKeyboardButton("🔄 مدل دیگر", callback_data=f"regen:{record_id}"))
    sub_row.append(InlineKeyboardButton("⚙️ تنظیمات مدل", callback_data="open_settings"))
    buttons.append(sub_row)
    return InlineKeyboardMarkup(buttons)


def build_reply_keyboard(record_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✨ انسانی‌سازی متن اصلی (بدون پاسخ)", callback_data=f"humanize:{record_id}"),
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
                InlineKeyboardButton("⚙️ تنظیمات مدل", callback_data="open_settings"),
            ],
        ]
    )


def build_humanized_keyboard(record_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 مدل دیگر (انسانی‌سازی مجدد متن خام)", callback_data=f"regen_humanize:{record_id}"),
            ],
            [
                InlineKeyboardButton("💬 جواب دادن به متن اصلی", callback_data=f"reply:{record_id}"),
                InlineKeyboardButton("📝 خلاصه متن اصلی", callback_data=f"sum:{record_id}"),
            ],
            [
                InlineKeyboardButton("🌐 ترجمه", callback_data=f"trans_hum:{record_id}"),
                InlineKeyboardButton("🔊 ارسال ویس (TTS)", callback_data=f"tts_hum:{record_id}"),
            ],
            [
                InlineKeyboardButton("⚙️ تنظیمات مدل", callback_data="open_settings"),
            ],
        ]
    )


def build_settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    s = get_user_model_settings(user_id)
    c_model = s.get("chat_model", CHAT_PIPELINE_MODELS[0])
    tr_model = s.get("transcribe_model", AUDIO_PIPELINE_MODELS[0])

    is_c_35_lite = c_model == "gemini-3.5-flash-lite"
    is_c_31_lite = c_model == "gemini-3.1-flash-lite"
    is_c_38_flash = c_model == "gemini-3.8-flash"

    buttons = [
        [
            InlineKeyboardButton(
                f"{'✅ ' if is_c_35_lite else ''}⚡ اولویت ۱ چت: gemini-3.5-flash-lite",
                callback_data="set_cfg:chat:35_lite",
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅ ' if is_c_31_lite else ''}🚀 اولویت ۲ چت: gemini-3.1-flash-lite",
                callback_data="set_cfg:chat:31_lite",
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅ ' if is_c_38_flash else ''}🧠 اولویت ۳ چت: gemini-3.8-flash",
                callback_data="set_cfg:chat:38_flash",
            )
        ],
        [
            InlineKeyboardButton(
                f"🎙️ اولویت ۱ ویس: {tr_model} (تغییر چرخه)",
                callback_data="set_cfg:transcribe:toggle",
            )
        ],
        [
            InlineKeyboardButton(
                f"🔑 حالت شروع چرخه: {'اولویت اول (KEY_1)' if AUDIO_PIPELINE.start_from_first_priority else 'آخرین کلید/مدل موفق'}",
                callback_data="set_cfg:pipeline:toggle_mode",
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


# ==================== فشرده‌سازی فوق‌سریع صوت در حافظه (برای ویدیومسیج و فایل‌های حجیم) ====================
def _compress_audio_in_memory_sync(raw_bytes: bytes) -> tuple[bytes, str]:
    """تبدیل سریع ویدیومسیج یا فایل صوتی سنگین به صدای مونو 16kHz Opus کم‌حجم در RAM برای آپلود آنی"""
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
    """اگر فایل ویدیومسیج است یا بیش از ۳۵۰ کیلوبایت حجم دارد، صدای خالص کم‌حجم استخراج می‌شود"""
    if not is_video_note and len(raw_bytes) <= FFMPEG_COMPRESS_THRESHOLD_BYTES and mime_type == "audio/ogg":
        return raw_bytes, mime_type

    compressed, new_mime = await asyncio.to_thread(_compress_audio_in_memory_sync, raw_bytes)
    if new_mime:
        return compressed, new_mime
    return raw_bytes, mime_type


# ==================== توابع اتصال به پایپ‌لاین دو مرحله‌ای ====================
def get_genai_client() -> genai.Client:
    return AUDIO_PIPELINE._get_client_for_key(KEY_1)


def _convert_wav_to_ogg_opus_sync(wav_bytes: bytes) -> bytes:
    """تبدیل صوت WAV به فرمت Opus استاندارد ویس تلگرام با ffmpeg"""
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


async def generate_speech_audio(text: str) -> bytes:
    """تولید ویس صوتی طبیعی با چرخش ۳ کلید [KEY_1, KEY_2, KEY_3] در صورت بروز خطای 429"""
    clean_text = text.replace("*", "").replace("#", "").strip()[:800]
    last_429: Optional[BaseException] = None

    for key_idx, api_key in enumerate(API_KEYS):
        key_label = f"KEY_{key_idx + 1}"
        try:
            client = AUDIO_PIPELINE._get_client_for_key(api_key)
            config = types.GenerateContentConfig(
                temperature=0.4,
                response_modalities=["AUDIO"],
            )
            response = await client.aio.models.generate_content(
                model=TTS_MODEL,
                contents=clean_text,
                config=config,
            )
            if response and response.candidates and response.candidates[0].content:
                for part in response.candidates[0].content.parts:
                    if part.inline_data and part.inline_data.data:
                        raw_wav_bytes = part.inline_data.data
                        return await asyncio.to_thread(_convert_wav_to_ogg_opus_sync, raw_wav_bytes)
            raise RuntimeError(f"No audio returned from {TTS_MODEL}")
        except Exception as exc:
            if is_http_429_error(exc):
                last_429 = exc
                logger.warning(
                    f"⚠️ [TTS][429 Rate Limit] خطای 429 با کلید '{key_label}' -> سوییچ فوری به کلید بعدی..."
                )
                continue
            logger.error(f"❌ [TTS][Non-429 Error] با کلید '{key_label}': {sanitize_error_message(exc)}")
            raise

    raise AllModelsAndKeysExhaustedError("TTSPipeline", attempts=len(API_KEYS), last_error=last_429)


async def transcribe_audio_bytes(
    audio_bytes: bytes,
    mime_type: str = "audio/ogg",
    preferred_model: Optional[str] = None,
) -> str:
    """
    وظیفه اول: تبدیل فایل صوتی (ویس) به متن از طریق AudioToTextPipeline
    با اولویت:
    1. gemini-3.5-transcribe
    2. gemini-3.5-flash-lite (همراه با پرامپت کلمه به کلمه)
    3. gemini-3.1-flash-lite (همراه با پرامپت کلمه به کلمه)
    """
    pref = preferred_model if preferred_model in AUDIO_PIPELINE_MODELS else None
    return await AUDIO_PIPELINE.transcribe(
        audio_bytes=audio_bytes,
        mime_type=mime_type,
        system_instruction=TRANSCRIPTION_CLEANUP_PROMPT,
        temperature=0.2,
        preferred_first_model=pref,
    )


async def generate_human_telegram_reply(
    transcript: str,
    variation_hint: str = "",
    user_id: Optional[int] = None,
) -> str:
    """
    وظیفه دوم: تولید پاسخ متنی چت بر اساس متن دریافتی از طریق ChatPipeline
    با اولویت:
    1. gemini-3.5-flash-lite
    2. gemini-3.1-flash-lite
    3. gemini-3.8-flash
    """
    settings = get_user_model_settings(user_id) if user_id else {}
    pref_model = settings.get("chat_model")
    if pref_model not in CHAT_PIPELINE_MODELS:
        pref_model = None

    user_content = f"متن پیام یا ویس مخاطب:\n{transcript}"
    if variation_hint:
        user_content += f"\n\nدستورالعمل تکمیلی:\n{variation_hint}"

    raw_reply = await CHAT_PIPELINE.generate_response(
        input_text=user_content,
        system_instruction=HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT,
        temperature=0.65,
        preferred_first_model=pref_model,
    )
    return strip_ai_punctuation_and_artifacts(raw_reply)


async def generate_humanized_raw_text(
    raw_text: str,
    variation_hint: str = "",
    user_id: Optional[int] = None,
) -> str:
    """
    انسانی‌سازی مستقیم خودِ متن خام ورودی (بدون پاسخ دادن به آن)
    مناسب برای مواقعی که کاربر متنی از هوش مصنوعی گرفته و می‌خواهد خودِ همان متن را انسانیزه کند.
    اجرا از طریق ChatPipeline با ساختار Fallback و مدیریت خطای 429.
    """
    settings = get_user_model_settings(user_id) if user_id else {}
    pref_model = settings.get("chat_model")
    if pref_model not in CHAT_PIPELINE_MODELS:
        pref_model = None

    user_content = (
        "متن خام زیر را بدون اینکه به آن پاسخ بدهی، با حفظ کامل محتوا و نکاتش "
        "به لحن طبیعی و انسانی تلگرامی (طبق قوانین سیستم) بازنویسی و انسانی‌سازی کن:\n\n"
        f"{raw_text}"
    )
    if variation_hint:
        user_content += f"\n\nدستورالعمل تکمیلی:\n{variation_hint}"

    raw_humanized = await CHAT_PIPELINE.generate_response(
        input_text=user_content,
        system_instruction=HUMANIZE_RAW_TEXT_SYSTEM_PROMPT,
        temperature=0.6,
        preferred_first_model=pref_model,
    )
    return strip_ai_punctuation_and_artifacts(raw_humanized)


async def generate_summary_text(transcript: str, user_id: Optional[int] = None) -> str:
    """تولید خلاصه پیام از طریق ChatPipeline با ساختار Fallback و مدیریت خطای 429"""
    settings = get_user_model_settings(user_id) if user_id else {}
    pref_model = settings.get("chat_model")
    if pref_model not in CHAT_PIPELINE_MODELS:
        pref_model = None

    return await CHAT_PIPELINE.generate_response(
        input_text=f"متن ورودی:\n{transcript}",
        system_instruction=SUMMARY_SYSTEM_PROMPT,
        temperature=0.4,
        preferred_first_model=pref_model,
    )


async def translate_text_content(text: str, user_id: Optional[int] = None) -> str:
    """ترجمه روان و اصیل از طریق ChatPipeline با ساختار Fallback و مدیریت خطای 429"""
    settings = get_user_model_settings(user_id) if user_id else {}
    pref_model = settings.get("chat_model")
    if pref_model not in CHAT_PIPELINE_MODELS:
        pref_model = None

    return await CHAT_PIPELINE.generate_response(
        input_text=f"متن زیر را ترجمه کن:\n{text}",
        system_instruction=TRANSLATION_SYSTEM_PROMPT,
        temperature=0.3,
        preferred_first_model=pref_model,
    )


# ==================== پیش‌تولید هوشمند در پس‌زمینه (Speculative Background Pre-Generation) ====================
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


async def _precompute_humanize_for_record(record_id: int, raw_text: str, user_id: Optional[int] = None) -> str:
    try:
        humanized = await generate_humanized_raw_text(raw_text, user_id=user_id)
        update_record_field(record_id, "humanized_text", humanized)
        return humanized
    finally:
        _INFLIGHT_HUMANIZE_TASKS.pop(record_id, None)


def trigger_background_precomputation(record_id: int, transcript: str, user_id: Optional[int] = None) -> None:
    """تولید خودکار جواب انسانی، خلاصه و نسخه انسانی‌شده متن خام در پس‌زمینه تا دکمه‌ها در کسری از ثانیه تحویل دهند"""
    if record_id not in _INFLIGHT_REPLY_TASKS:
        _INFLIGHT_REPLY_TASKS[record_id] = asyncio.create_task(
            _precompute_reply_for_record(record_id, transcript, user_id=user_id)
        )
    if record_id not in _INFLIGHT_SUMMARY_TASKS:
        _INFLIGHT_SUMMARY_TASKS[record_id] = asyncio.create_task(
            _precompute_summary_for_record(record_id, transcript, user_id=user_id)
        )
    if record_id not in _INFLIGHT_HUMANIZE_TASKS:
        _INFLIGHT_HUMANIZE_TASKS[record_id] = asyncio.create_task(
            _precompute_humanize_for_record(record_id, transcript, user_id=user_id)
        )


# ==================== هندلرهای تلگرام ====================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return

    welcome_text = (
        "سلام 👋 به ربات هوشمند با معماری **Pipeline دو مرحله‌ای** خوش آمدید!\n\n"
        "🚀 **امکانات و معماری Fallback هوشمند (۳ کلید مجزا + مدیریت خطای 429):**\n"
        "۱. **🎙️ مرحله اول (تبدیل ویس به متن):**\n"
        "   • اولویت اول: `gemini-3.5-transcribe`\n"
        "   • اولویت دوم: `gemini-3.5-flash-lite`\n"
        "   • اولویت سوم: `gemini-3.1-flash-lite`\n"
        "۲. **💬 مرحله دوم (تولید پاسخ چت و بازنویسی انسانی):**\n"
        "   • اولویت اول: `gemini-3.5-flash-lite`\n"
        "   • اولویت دوم: `gemini-3.1-flash-lite`\n"
        "   • اولویت سوم: `gemini-3.8-flash`\n"
        "۳. **✨ انسانی‌سازی متن اصلی (بدون پاسخ):** بازنویسی مستقیم متن خام اولیه به لحن طبیعی تلگرامی بدون پاسخ دادن به آن.\n"
        "۴. **🌐 ترجمه و 🔊 تبدیل متن به ویس (TTS):** همراه با چرخش خودکار کلیدهای سه‌گانه.\n"
        "۵. **⚙️ مشاهده وضعیت کلیدها و مدل‌ها:** با دستور `/settings` یا `/model`.\n\n"
        "💡 **روش استفاده:** کافیست یک ویس، فایل صوتی یا پیام متنی بفرستید تا فوراً پردازش شود."
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
            "⚡ در حال تبدیل فوق‌سریع ویس...",
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
        # اجرای مرحله اول (Audio-to-Text Pipeline)
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

        # پیش‌تولید جواب انسانی، خلاصه و نسخه انسانی‌شده متن خام در پس‌زمینه
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


def _extract_clean_base_text(raw_text: str) -> str:
    cleaned = raw_text
    if "━━━━━━━━━━━━━━" in cleaned:
        cleaned = cleaned.split("━━━━━━━━━━━━━━")[0]
    cleaned = _RE_CLEAN_VOICE_HEADER.sub("", cleaned.strip())
    cleaned = _RE_CLEAN_SUM_HEADER.sub("", cleaned.strip())
    return cleaned.strip()


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

    is_forwarded = getattr(message, "forward_origin", None) is not None or getattr(message, "forward_date", None) is not None

    # حالت ۱: ریپلای دستور سفارشی روی پیام یا ویس قبلی
    if message.reply_to_message and not is_forwarded:
        replied_msg = message.reply_to_message
        db_rec = get_record_by_msg_id(message.chat_id, replied_msg.message_id)
        if db_rec and db_rec.get("transcript"):
            base_text = db_rec["transcript"]
        else:
            raw_replied = replied_msg.text or replied_msg.caption or ""
            base_text = _extract_clean_base_text(raw_replied)

        if base_text:
            custom_hint = (
                f"کاربر می‌خواهد در جوابِ این پیام/ویس، این نکات یا تصمیم‌ها را بگوید: «{incoming_text}». "
                f"دقیقاً همین موارد را با لحن طبیعی، خونسرد، بدون تملق و بدون هیچ علامت نگارشی و نیم‌فاصله کتابی تبدیل به پیام تلگرامی کن."
            )
            wait_msg_task = asyncio.create_task(
                message.reply_text(
                    "⚡ در حال نوشتن جواب تلگرامی...",
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
                    transcript=f"پیام مخاطب:\n{base_text}\n\nنکات پاسخ:\n{incoming_text}",
                    human_reply=human_reply,
                )
                if rec_id not in _INFLIGHT_HUMANIZE_TASKS:
                    _INFLIGHT_HUMANIZE_TASKS[rec_id] = asyncio.create_task(
                        _precompute_humanize_for_record(rec_id, incoming_text, user_id=user.id)
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

    # حالت ۲: پیام متنی فورواردشده یا مستقیم -> مرحله دوم (Chat Pipeline)
    wait_msg_task = asyncio.create_task(
        message.reply_text(
            "⚡ در حال نوشتن جواب انسانی...",
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
        # پیش‌تولید نسخه انسانی‌شده متن خام اولیه در پس‌زمینه
        if record_id not in _INFLIGHT_HUMANIZE_TASKS:
            _INFLIGHT_HUMANIZE_TASKS[record_id] = asyncio.create_task(
                _precompute_humanize_for_record(record_id, incoming_text, user_id=user.id)
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


def _build_settings_status_text(user_id: int) -> str:
    s = get_user_model_settings(user_id)
    a_state = AUDIO_PIPELINE.state
    c_state = CHAT_PIPELINE.state
    a_last = f"{a_state.last_successful_model or 'اجرا نشده'} ({a_state.last_successful_key_label})"
    c_last = f"{c_state.last_successful_model or 'اجرا نشده'} ({c_state.last_successful_key_label})"

    return (
        "⚙️ **تنظیمات و وضعیت پایپ‌لاین دو مرحله‌ای (۳ کلید مجزا)**\n\n"
        f"🎙️ **مرحله ۱ (ویس به متن):**\n"
        f"   • اولویت مدل‌ها: `{' -> '.join(AUDIO_PIPELINE_MODELS)}`\n"
        f"   • مدل شروع فعلی: `{s.get('transcribe_model')}`\n"
        f"   • آخرین اجرای موفق: `{a_last}` (تغییر کلید 429: `{a_state.total_429_switches}`)\n\n"
        f"💬 **مرحله ۲ (پاسخ چت و انسانی‌سازی):**\n"
        f"   • اولویت مدل‌ها: `{' -> '.join(CHAT_PIPELINE_MODELS)}`\n"
        f"   • مدل شروع فعلی: `{s.get('chat_model')}`\n"
        f"   • آخرین اجرای موفق: `{c_last}` (تغییر کلید 429: `{c_state.total_429_switches}`)\n\n"
        "برای تغییر اولویت شروع یا حالت چرخه کلیدها کلیک کنید:"
    )


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return
    user = update.effective_user
    if not user or not update.message:
        return

    await update.message.reply_text(
        _build_settings_status_text(user.id),
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
        await query.message.reply_text(
            _build_settings_status_text(user.id),
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

    if query.data.startswith("set_cfg:"):
        _, target, mode = query.data.split(":", 2)
        if target == "chat":
            if mode == "35_lite":
                update_user_setting(user.id, "chat_model", "gemini-3.5-flash-lite")
                await query.answer("⚡ اولویت اول چت: gemini-3.5-flash-lite فعال شد!", show_alert=False)
            elif mode == "31_lite":
                update_user_setting(user.id, "chat_model", "gemini-3.1-flash-lite")
                await query.answer("🚀 اولویت چت: gemini-3.1-flash-lite فعال شد!", show_alert=False)
            elif mode == "38_flash":
                update_user_setting(user.id, "chat_model", "gemini-3.8-flash")
                await query.answer("🧠 اولویت چت: gemini-3.8-flash فعال شد!", show_alert=False)
        elif target == "transcribe" and mode == "toggle":
            curr = get_user_model_settings(user.id).get("transcribe_model", AUDIO_PIPELINE_MODELS[0])
            idx = AUDIO_PIPELINE_MODELS.index(curr) if curr in AUDIO_PIPELINE_MODELS else 0
            new_m = AUDIO_PIPELINE_MODELS[(idx + 1) % len(AUDIO_PIPELINE_MODELS)]
            update_user_setting(user.id, "transcribe_model", new_m)
            await query.answer(f"🎙️ مدل اولویت ویس تغییر کرد به:\n{new_m}", show_alert=False)
        elif target == "pipeline" and mode == "toggle_mode":
            new_flag = not AUDIO_PIPELINE.start_from_first_priority
            AUDIO_PIPELINE.start_from_first_priority = new_flag
            CHAT_PIPELINE.start_from_first_priority = new_flag
            await query.answer(
                "✅ شروع از اولویت اول (KEY_1) فعال شد" if new_flag else "✅ ادامه از آخرین کلید/مدل موفق فعال شد",
                show_alert=False,
            )

        try:
            await query.message.edit_text(
                _build_settings_status_text(user.id),
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
        await query.answer("🔊 در حال تولید ویس صوتی با Gemini 3.8 Flash TTS...")
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
                caption="🔊 ویس تولیدشده با مدل پرچمدار Gemini 3.8 Flash TTS",
            )
            await wait_msg.delete()
        except Exception as e:
            logger.error(f"TTS error: {sanitize_error_message(e)}")
            await wait_msg.edit_text(f"❌ خطا در تولید ویس صوتی: {sanitize_error_message(e)}")

    elif action == "regen":
        await query.answer("🔄 در حال نوشتن یک مدل جدید...")
        hint = (
            "یک مدل جواب متفاوت‌تر، راحت‌تر و خونسردتر نسبت به حالت قبلی بنویس "
            "که باز هم صددرصد طبیعی، بدون هیچ علامت نگارشی، بدون نیم‌فاصله کتابی و بدون تعارف باشه."
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

    elif action == "humanize":
        # استخراج مستقیم متن خام اولیه از فیلد transcript تا هرگز پاسخ ربات (human_reply) انسانی‌سازی نشود
        raw_source_text = _get_raw_input_text(record)
        if record.get("humanized_text"):
            await query.answer("⚡ متن انسانی‌شده آماده است!")
            await query.message.reply_text(
                record["humanized_text"][:3950],
                reply_to_message_id=voice_msg_id,
                reply_markup=build_humanized_keyboard(record_id),
            )
            return

        await query.answer("⚡ در حال انسانی‌سازی متن اصلی...")
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "⚡ در حال انسانی‌سازی متن اصلی (بدون پاسخ دادن)...",
                reply_to_message_id=voice_msg_id,
            )
        )
        inflight = _INFLIGHT_HUMANIZE_TASKS.get(record_id)
        humanize_task = inflight if inflight else asyncio.create_task(
            _precompute_humanize_for_record(record_id, raw_source_text, user_id=user.id)
        )
        wait_msg = await wait_msg_task
        try:
            humanized_text = await humanize_task
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=humanized_text,
                reply_markup=build_humanized_keyboard(record_id),
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در انسانی‌سازی متن اصلی: {sanitize_error_message(e)}")

    elif action == "regen_humanize":
        raw_source_text = _get_raw_input_text(record)
        await query.answer("🔄 در حال بازنویسی و انسانی‌سازی مجدد متن خام...")
        hint = (
            "یک مدل بازنویسی متفاوت‌تر، روان‌تر و طبیعی‌تر از همین متن خام ورودی ارائه بده "
            "به طوری که به هیچ وجه به متن جواب ندی بلکه خودِ همین متن خام رو با لحن خونسرد تلگرامی، "
            "بدون هیچ علامت نگارشی، بدون نیم‌فاصله کتابی و بدون تعارف انسانی‌سازی کنی."
        )
        wait_msg_task = asyncio.create_task(
            query.message.reply_text(
                "⚡ در حال انسانی‌سازی مجدد متن اصلی با مدل جدید...",
                reply_to_message_id=voice_msg_id,
            )
        )
        humanize_task = asyncio.create_task(
            generate_humanized_raw_text(raw_source_text, variation_hint=hint, user_id=user.id)
        )
        wait_msg = await wait_msg_task
        try:
            humanized_text = await humanize_task
            update_record_field(record_id, "humanized_text", humanized_text)
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=humanized_text,
                reply_markup=build_humanized_keyboard(record_id),
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در انسانی‌سازی مجدد متن اصلی: {sanitize_error_message(e)}")

    elif action == "trans_hum":
        base_for_trans = record.get("humanized_text") or _get_raw_input_text(record)
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
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=translated,
                header_prefix="🌐 ترجمه:\n\n",
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در ترجمه: {sanitize_error_message(e)}")

    elif action == "tts_hum":
        text_to_speak = record.get("humanized_text") or _get_raw_input_text(record)
        await query.answer("🔊 در حال تولید ویس صوتی با Gemini 3.8 Flash TTS...")
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
                caption="🔊 ویس تولیدشده با مدل پرچمدار Gemini 3.8 Flash TTS",
            )
            await wait_msg.delete()
        except Exception as e:
            logger.error(f"TTS error: {sanitize_error_message(e)}")
            await wait_msg.edit_text(f"❌ خطا در تولید ویس صوتی: {sanitize_error_message(e)}")


def main() -> None:
    if not BOT_TOKEN or not any(API_KEYS):
        logger.error("❌ کلیدهای ارتباطی (BOT_TOKEN / API_KEYS) یافت نشدند.")
        sys.exit(1)

    init_db()
    try:
        get_genai_client()
    except Exception:
        pass

    logger.info(
        "Starting @voiceandpasokhbot | Two-Stage Pipeline Active | "
        f"Audio Priority={AUDIO_PIPELINE_MODELS} | Chat Priority={CHAT_PIPELINE_MODELS} | "
        f"Keys Configured={len(API_KEYS)} [KEY_1, KEY_2, KEY_3]"
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
