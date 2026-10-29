#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ربات تلگرام تبدیل سریع ویس به متن + پاسخ‌گوی انسانی به ویس و پیام متنی (@voiceandpasokhbot)
نسخه دیباگ‌شده و ایمن‌سازی‌شده برای مخازن پابلیک گیت‌هاب (Zero-Leak & Anti-Spam Edition):
- بدون هیچ توکن یا کلید API هاردکدشده در سورس‌کد
- فیلتر مخفی‌سازی خودکار توکن‌ها در لاگ‌های عمومی GitHub Actions (جلوگیری از نشت توکن توسط httpx)
- سیستم کنترل دسترسی (ALLOWED_USER_IDS)، محدودیت چت خصوصی، Rate-Limit و فیلتر ضد تبلیغات/اسپم
- دیتابیس SQLite ایمن در برابر همزمانی (WAL + Context Manager + Timeout) و ثبت آیدی پیام بات برای ریپلای دقیق
- سازگاری کامل با Google GenAI SDK و پشتیبانی از ویس، فایل صوتی، ویدیومسیج و پیام‌های کپشن‌دار
"""

from collections import defaultdict
from contextlib import closing
import logging
import os
import re
import sqlite3
import sys
import time
from typing import Optional, Set

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ChatAction, ChatType
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from google import genai
from google.genai import types

from humanizer_skill import (
    HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT,
    SUMMARY_SYSTEM_PROMPT,
    TRANSCRIPTION_CLEANUP_PROMPT,
    strip_ai_punctuation_and_artifacts,
)

# ==================== بارگذاری ایمن متغیرهای محیطی (بدون هیچ مقدار هاردکدشده) ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "voice_bot.db").strip()

# لیست آیدی‌های عددی مجاز (در صورت تنظیم در GitHub Secrets، فقط همین افراد می‌توانند با بات کار کنند)
# مثال: ALLOWED_USER_IDS="123456789,987654321"
RAW_ALLOWED_IDS = os.getenv("ALLOWED_USER_IDS", "").strip()
ALLOWED_USER_IDS: Set[int] = {
    int(uid.strip())
    for uid in RAW_ALLOWED_IDS.split(",")
    if uid.strip().isdigit()
}

# آیا در صورت خالی بودن ALLOWED_USER_IDS، فقط در چت خصوصی پاسخ بدهد؟ (پیش‌فرض: بله، برای جلوگیری از اسپم در گروه‌ها)
PRIVATE_CHAT_ONLY = os.getenv("PRIVATE_CHAT_ONLY", "true").strip().lower() in ("1", "true", "yes")

# محدودیت نرخ ارسال پیام (Rate Limiting) برای جلوگیری از سوءاستفاده و سوزاندن سهمیه API
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "10"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

# حداکثر حجم مجاز دانلود فایل توسط Bot API استاندارد تلگرام (۲۰ مگابایت)
MAX_TELEGRAM_FILE_SIZE_BYTES = 20 * 1024 * 1024
TELEGRAM_MAX_MSG_LENGTH = 4000

# مدل‌های هوش مصنوعی با قابلیت جایگزینی خودکار (Fallback) به مدل‌های پایدار Gemini
TRANSCRIBE_MODEL = os.getenv("TRANSCRIBE_MODEL", "gemini-2.5-flash").strip()
CHAT_MODEL = os.getenv("CHAT_MODEL", "gemini-2.5-flash").strip()
FALLBACK_MODELS = (
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
)


# ==================== فیلتر امنیتی لاگ‌ها (جلوگیری از لو رفتن توکن در لاگ پابلیک گیت‌هاب) ====================
class SecretMaskingFilter(logging.Filter):
    """مخفی‌سازی هرگونه اثر از BOT_TOKEN و GEMINI_API_KEY در تمام لاگ‌ها و استثناها"""

    def __init__(self, secrets_to_mask: list[str]) -> None:
        super().__init__()
        self.secrets = [s for s in secrets_to_mask if s and len(s) > 6]

    def _mask(self, text: str) -> str:
        masked = text
        for secret in self.secrets:
            if secret in masked:
                masked = masked.replace(secret, "***REDACTED_SECRET***")
        # پوشش الگوی عمومی توکن تلگرام و کلیدهای API در URLها
        masked = re.sub(r"bot\d{8,12}:[A-Za-z0-9_-]{30,}", "bot***REDACTED***", masked)
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
# مسدود کردن لاگ‌های سطح INFO کتابخانه httpx و httpcore که URL حاوی BOT_TOKEN را در کنسول چاپ می‌کنند
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

secret_filter = SecretMaskingFilter([BOT_TOKEN, GEMINI_API_KEY])
for handler in logging.root.handlers:
    handler.addFilter(secret_filter)

logger = logging.getLogger("VoiceAndPasokhBot")
logger.addFilter(secret_filter)


# ==================== سیستم ضد تبلیغات، ضد اسپم و کنترل دسترسی ====================
_user_request_timestamps: dict[int, list[float]] = defaultdict(list)

# الگوهای رایج پیام‌های تبلیغاتی، لینک‌های دعوت کانال/گروه و اسپم
AD_AND_SPAM_PATTERNS = re.compile(
    r"(?:"
    r"t\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+|"
    r"telegram\.me/(?:\+|joinchat/)[A-Za-z0-9_-]+|"
    r"(?:تبلیغات|سفارش تبلیغ|کسب درآمد دلاری|ایردراپ رایگان|پامپ ارز|سایت شرط\s*بندی|بت فوروارد|وان ایکس بت)"
    r")",
    re.IGNORECASE,
)


def is_user_authorized(update: Update) -> bool:
    """بررسی مجوز کاربر، جلوگیری از ربات‌های دیگر و مسدودسازی گروه‌ها در حالت خصوصی"""
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
    """محدودسازی تعداد درخواست در دقیقه برای هر کاربر (مگر اینکه در لیست ادمین‌های مجاز باشد)"""
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
    """تشخیص پیام‌های تبلیغاتی و لینک‌های اسپم برای کاربران غیر ادمین"""
    if ALLOWED_USER_IDS and user_id in ALLOWED_USER_IDS:
        return False
    if not text:
        return False
    if AD_AND_SPAM_PATTERNS.search(text):
        return True
    # اگر پیام پر از لینک یا منشن تبلیغاتی باشد (بیش از ۳ لینک یا آیدی در یک پیام)
    links_count = len(re.findall(r"(?:https?://|t\.me/|@[A-Za-z0-9_]{5,})", text))
    if links_count >= 4:
        return True
    return False


def sanitize_error_message(err: Exception) -> str:
    """پاک‌سازی متن خطا قبل از نمایش به کاربر تا هیچ کلید یا آدرس داخلی لو نرود"""
    msg = str(err)
    for s in (BOT_TOKEN, GEMINI_API_KEY):
        if s:
            msg = msg.replace(s, "***")
    msg = re.sub(r"bot\d{8,12}:[A-Za-z0-9_-]{30,}", "bot***", msg)
    msg = re.sub(r"key=[A-Za-z0-9_\-\.]+", "key=***", msg)
    return msg[:160]


# ==================== مدیریت دیتابیس SQLite (Thread/Async-Safe با WAL) ====================
def _get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    """ایجاد جدول ذخیره متن ویس‌ها، پیام‌های متنی و آیدی پیام بات برای پایداری کامل"""
    with closing(_get_db_connection()) as conn:
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
                created_at REAL NOT NULL
            )
            """
        )
        # مهاجرت ایمن در صورت وجود جدول قدیمی بدون ستون bot_msg_id
        cursor.execute("PRAGMA table_info(voice_records)")
        existing_cols = {row["name"] for row in cursor.fetchall()}
        if "bot_msg_id" not in existing_cols:
            cursor.execute("ALTER TABLE voice_records ADD COLUMN bot_msg_id INTEGER")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_voice ON voice_records(chat_id, voice_msg_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chat_bot_msg ON voice_records(chat_id, bot_msg_id)"
        )
        conn.commit()


def save_record(
    chat_id: int,
    msg_id: int,
    transcript: str,
    human_reply: Optional[str] = None,
    bot_msg_id: Optional[int] = None,
) -> int:
    with closing(_get_db_connection()) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO voice_records (chat_id, voice_msg_id, bot_msg_id, transcript, human_reply, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chat_id, msg_id, bot_msg_id, transcript, human_reply, time.time()),
        )
        record_id = int(cursor.lastrowid)
        conn.commit()
        return record_id


def get_record(record_id: int) -> Optional[dict]:
    with closing(_get_db_connection()) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM voice_records WHERE record_id = ?", (record_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_record_by_msg_id(chat_id: int, msg_id: int) -> Optional[dict]:
    """جستجوی رکورد چه کاربر روی خودِ ویس/پیام اصلی ریپلای زده باشد و چه روی پیام پاسخ بات"""
    with closing(_get_db_connection()) as conn:
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
        return dict(row) if row else None


def update_record_field(record_id: int, field: str, value: str | int) -> None:
    if field not in ("summary", "human_reply", "transcript", "bot_msg_id"):
        return
    with closing(_get_db_connection()) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE voice_records SET {field} = ? WHERE record_id = ?",
            (value, record_id),
        )
        conn.commit()


# ==================== کیبوردهای شیشه‌ای (Inline Keyboards) ====================
def build_voice_action_keyboard(record_id: int, has_reply: bool = False) -> InlineKeyboardMarkup:
    """ساخت دکمه‌های زیر متن ویس برای خلاصه‌سازی و تولید جواب انسانی طبق اسکیل telegram-human-reply"""
    buttons = [
        [
            InlineKeyboardButton("💬 جواب انسانی (تلگرامی)", callback_data=f"reply:{record_id}"),
            InlineKeyboardButton("📝 خلاصه", callback_data=f"sum:{record_id}"),
        ]
    ]
    if has_reply:
        buttons.append(
            [
                InlineKeyboardButton(
                    "🔄 یه مدل دیگه بنویس", callback_data=f"regen:{record_id}"
                )
            ]
        )
    return InlineKeyboardMarkup(buttons)


def build_reply_keyboard(record_id: int) -> InlineKeyboardMarkup:
    """ساخت دکمه‌های زیر پیامِ پاسخ انسانی (تولید مجدد + خلاصه)"""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 یه مدل دیگه بنویس", callback_data=f"regen:{record_id}"),
                InlineKeyboardButton("📝 خلاصه پیام", callback_data=f"sum:{record_id}"),
            ]
        ]
    )


# ==================== توابع کمکی ارسال و ویرایش ایمن پیام در تلگرام ====================
async def safe_edit_or_send_long_text(
    status_msg: Message,
    origin_msg: Message,
    full_text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    header_prefix: str = "",
    footer_suffix: str = "",
) -> None:
    """ویرایش ایمن پیام و شکستن خودکار متن‌های طولانی‌تر از ۴۰۰۰ کاراکتر بدون خطای BadRequest"""
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


# ==================== توابع هوش مصنوعی (Google GenAI SDK) ====================
_genai_client: Optional[genai.Client] = None


def get_genai_client() -> genai.Client:
    global _genai_client
    if _genai_client is None:
        if not GEMINI_API_KEY:
            raise RuntimeError("متغیر محیطی GEMINI_API_KEY در تنظیمات Secrets تعریف نشده است.")
        _genai_client = genai.Client(api_key=GEMINI_API_KEY)
    return _genai_client


def _candidate_models(primary_model: str) -> list[str]:
    models = []
    for m in (primary_model, *FALLBACK_MODELS):
        if m and m not in models:
            models.append(m)
    return models


async def transcribe_audio_bytes(audio_bytes: bytes, mime_type: str = "audio/ogg") -> str:
    """تبدیل سریع فایل صوتی یا ویدیومسیج به متن با پشتیبانی از زنجیره مدل‌های پشتیبان"""
    client = get_genai_client()
    audio_part = types.Part.from_bytes(data=audio_bytes, mime_type=mime_type)

    for model_name in _candidate_models(TRANSCRIBE_MODEL):
        try:
            response = await client.aio.models.generate_content(
                model=model_name,
                contents=[audio_part, TRANSCRIPTION_CLEANUP_PROMPT],
            )
            if response and response.text and response.text.strip():
                return response.text.strip()
        except Exception as err:
            logger.warning(f"Transcription attempt with {model_name} failed: {sanitize_error_message(err)}")

    raise RuntimeError("خطا در تبدیل ویس به متن توسط سرویس Gemini.")


async def generate_summary_text(transcript: str) -> str:
    """خلاصه‌سازی سریع متن ویس یا پیام"""
    client = get_genai_client()
    prompt = f"{SUMMARY_SYSTEM_PROMPT}\n\nمتن ورودی:\n{transcript}"

    for model_name in _candidate_models(CHAT_MODEL):
        try:
            response = await client.aio.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            if response and response.text and response.text.strip():
                return response.text.strip()
        except Exception as e:
            logger.warning(f"Summary generation with {model_name} failed: {sanitize_error_message(e)}")

    raise RuntimeError("خطا در تولید خلاصه.")


async def generate_human_telegram_reply(transcript: str, variation_hint: str = "") -> str:
    """
    تولید پاسخ ۱۰۰٪ انسانی و محاوره‌ای به سبک چت تلگرام منطبق با اسکیل `telegram-human-reply`
    (بدون علائم نگارشی، بدون نیم‌فاصله کتابی، بدون تملق و بدون توضیحات اضافه)
    """
    client = get_genai_client()
    extra_instruction = ""
    if variation_hint:
        extra_instruction = f"\nدستورالعمل تکمیلی برای این پاسخ:\n{variation_hint}\n"

    prompt = (
        f"{HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT}\n"
        f"{extra_instruction}\n"
        f"متن پیام یا ویس مخاطب که باید بهش جواب بدی:\n"
        f"{transcript}\n\n"
        f"حالا جواب نهایی رو دقیقاً طبق قوانین بالا (بدون هیچ نقطه، ویرگول، دونقطه، نیم‌فاصله کتابی و بدون تعریف و تمجید مصنوعی) بنویس:"
    )

    for model_name in _candidate_models(CHAT_MODEL):
        try:
            config = types.GenerateContentConfig(temperature=0.7)
            response = await client.aio.models.generate_content(
                model=model_name,
                contents=prompt,
                config=config,
            )
            if response and response.text and response.text.strip():
                return strip_ai_punctuation_and_artifacts(response.text)
        except Exception as e:
            logger.warning(f"Human reply generation with {model_name} failed: {sanitize_error_message(e)}")

    raise RuntimeError("خطا در تولید پاسخ انسانی.")


# ==================== هندلرهای تلگرام ====================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_authorized(update):
        return

    welcome_text = (
        "سلام 👋\n"
        "بات آماده است و به دو روش کار می‌کنه:\n\n"
        "۱. ارسال یا فوروارد ویس (حتی چند ویس همزمان):\n"
        "در لحظه و به صورت موازی زیرِ هر ویس متن کاملش رو برات می‌نویسم و زیرش دکمه «💬 جواب انسانی (تلگرامی)» و «📝 خلاصه» می‌ذارم.\n\n"
        "۲. فوروارد یا ارسال مستقیم پیام متنی:\n"
        "هر پیام متنی‌ای که برام فوروارد کنی یا بفرستی، درجا پاسخش رو طبق ساختار انسانی تلگرام (بدون علائم نگارشی، بدون تعارف مصنوعی، خونسرد و محاوره‌ای) برات می‌نویسم.\n\n"
        "💡 نکته: هر وقت روی متن ویس یا پیام ریپلای بزنی و بگی چی می‌خوای بگی (مثلاً: بهش بگو این هفته تابع رو حذف کن)، همون رو با لحن انسانی تلگرامی برات می‌نویسم."
    )
    if update.message:
        await update.message.reply_text(welcome_text)


async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """نمایش آیدی عددی کاربر برای قرار دادن در ALLOWED_USER_IDS جهت خصوصی‌سازی کامل بات"""
    user = update.effective_user
    if not user or not update.message:
        return
    await update.message.reply_text(
        f"🆔 آیدی عددی تلگرام شما:\n{user.id}\n\n"
        f"برای اینکه فقط خودتان بتوانید از بات استفاده کنید، این عدد را در GitHub Secrets با نام ALLOWED_USER_IDS ذخیره کنید."
    )


async def process_single_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """پردازش غیرهمزمان (Async) هر ویس، فایل صوتی یا ویدیومسیج به صورت مستقل"""
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

    # بررسی حجم فایل قبل از دانلود (محدودیت ۲۰ مگابایت در Bot API استاندارد)
    file_size = getattr(media_obj, "file_size", None)
    if file_size and file_size > MAX_TELEGRAM_FILE_SIZE_BYTES:
        await message.reply_text(
            "⚠️ حجم این فایل صوتی/ویدیویی بیش از ۲۰ مگابایت است و تلگرام اجازه دانلود مستقیم آن توسط بات را نمی‌دهد."
        )
        return

    # تشخیص دقیق mime_type (ویدیومسیج‌ها در تلگرام ویژگی mime_type ندارند و فرمتشان video/mp4 است)
    if message.video_note is not None:
        mime_type = "video/mp4"
    else:
        mime_type = getattr(media_obj, "mime_type", None) or "audio/ogg"

    status_msg = await message.reply_text(
        "🎙 در حال تبدیل سریع ویس به متن...",
        reply_to_message_id=message.message_id,
    )

    try:
        await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.TYPING)

        tg_file = await context.bot.get_file(media_obj.file_id)
        audio_bytearray = await tg_file.download_as_bytearray()
        audio_bytes = bytes(audio_bytearray)

        transcript = await transcribe_audio_bytes(audio_bytes, mime_type=mime_type)

        record_id = save_record(
            chat_id=message.chat_id,
            msg_id=message.message_id,
            bot_msg_id=status_msg.message_id,
            transcript=transcript,
        )

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


def _extract_clean_base_text(raw_text: str) -> str:
    """حذف سرفصل‌ها و پانویس‌های بات در صورتی که متن مستقیم از پیام بات خوانده شود"""
    cleaned = raw_text
    if "━━━━━━━━━━━━━━" in cleaned:
        cleaned = cleaned.split("━━━━━━━━━━━━━━")[0]
    cleaned = re.sub(r"^📝\s*\*{0,2}متن ویس(?:\s*\(بخش\s*[0-9۰-۹]+(?:\s*از\s*[0-9۰-۹]+)?\))?:?\*{0,2}\s*", "", cleaned.strip())
    cleaned = re.sub(r"^📌\s*\*{0,2}خلاصه:?\*{0,2}\s*", "", cleaned.strip())
    return cleaned.strip()


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    پردازش پیام‌های متنی یا کپشن‌دار (فورواردشده یا ارسالی):
    ۱. فیلتر دسترسی، ضد اسپم/تبلیغات و Rate Limit
    ۲. در صورت ریپلای دستور سفارشی روی ویس یا پیام قبلی -> تولید پاسخ طبق دستور کاربر
    ۳. در صورت ارسال یا فوروارد پیام -> تولید فوری پاسخ انسانی تلگرامی
    """
    if not is_user_authorized(update):
        return

    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return

    incoming_text = (message.text or message.caption or "").strip()
    if not incoming_text:
        return

    # مسدودسازی پیام‌های تبلیغاتی و لینک‌های اسپم
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
            wait_msg = await message.reply_text(
                "✍️ در حال نوشتن جواب تلگرامی طبق نظر تو...",
                reply_to_message_id=message.message_id,
            )
            try:
                custom_hint = (
                    f"کاربر می‌خواهد در جوابِ این پیام/ویس، این نکات یا تصمیم‌ها را بگوید: «{incoming_text}». "
                    f"دقیقاً همین موارد را با لحن طبیعی، خونسرد، بدون تملق و بدون هیچ علامت نگارشی و نیم‌فاصله کتابی تبدیل به پیام تلگرامی کن."
                )
                human_reply = await generate_human_telegram_reply(
                    base_text, variation_hint=custom_hint
                )
                rec_id = save_record(
                    chat_id=message.chat_id,
                    msg_id=message.message_id,
                    bot_msg_id=wait_msg.message_id,
                    transcript=f"پیام مخاطب:\n{base_text}\n\nنکات پاسخ:\n{incoming_text}",
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

    # حالت ۲: پیام متنی فورواردشده یا پیام متنی مستقیم -> تولید فوری پاسخ انسانی تلگرامی
    wait_msg = await message.reply_text(
        "✍️ در حال نوشتن جواب انسانی...",
        reply_to_message_id=message.message_id,
    )

    try:
        await context.bot.send_chat_action(chat_id=message.chat_id, action=ChatAction.TYPING)
        human_reply = await generate_human_telegram_reply(incoming_text)
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


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """مدیریت کلیک روی دکمه‌های «خلاصه» و «جواب انسانی (تلگرامی)»"""
    if not is_user_authorized(update):
        if update.callback_query:
            await update.callback_query.answer("⛔ شما مجوز استفاده از این بات را ندارید.", show_alert=True)
        return

    query = update.callback_query
    user = update.effective_user
    if not query or not query.data or not query.message or not user:
        return

    if is_rate_limited(user.id):
        await query.answer("⏳ لطفاً چند ثانیه صبر کنید و دوباره امتحان کنید.", show_alert=True)
        return

    await query.answer("در حال آماده‌سازی...")

    try:
        action, record_id_str = query.data.split(":", 1)
        record_id = int(record_id_str)
    except ValueError:
        return

    record = get_record(record_id)
    if not record or record.get("chat_id") != query.message.chat_id:
        await query.message.reply_text("⚠️ اطلاعات این پیام در دیتابیس پیدا نشد. لطفاً دوباره بفرستید.")
        return

    transcript = record["transcript"]
    voice_msg_id = record["voice_msg_id"]

    if action == "sum":
        if record.get("summary"):
            await query.message.reply_text(
                f"📌 خلاصه:\n\n{record['summary'][:3900]}",
                reply_to_message_id=voice_msg_id,
            )
            return

        wait_msg = await query.message.reply_text(
            "⏳ در حال خلاصه‌سازی سریع...",
            reply_to_message_id=voice_msg_id,
        )
        try:
            summary = await generate_summary_text(transcript)
            update_record_field(record_id, "summary", summary)
            await safe_edit_or_send_long_text(
                status_msg=wait_msg,
                origin_msg=query.message,
                full_text=summary,
                header_prefix="📌 خلاصه:\n\n",
            )
        except Exception as e:
            await wait_msg.edit_text(f"❌ خطا در خلاصه‌سازی: {sanitize_error_message(e)}")

    elif action in ("reply", "regen"):
        wait_msg = await query.message.reply_text(
            "✍️ در حال نوشتن جواب انسانی به سبک چت تلگرام...",
            reply_to_message_id=voice_msg_id,
        )
        try:
            hint = ""
            if action == "regen":
                hint = (
                    "یک مدل جواب متفاوت‌تر، راحت‌تر و خونسردتر نسبت به حالت قبلی بنویس "
                    "که باز هم صددرصد طبیعی، بدون هیچ علامت نگارشی، بدون نیم‌فاصله کتابی و بدون تعارف باشه."
                )

            human_reply = await generate_human_telegram_reply(transcript, variation_hint=hint)
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
    if not BOT_TOKEN or not GEMINI_API_KEY:
        logger.error(
            "❌ خطای امنیتی: متغیرهای BOT_TOKEN و GEMINI_API_KEY باید در GitHub Secrets تنظیم شوند و نباید داخل کد باشند."
        )
        sys.exit(1)

    init_db()
    logger.info("Starting @voiceandpasokhbot (Secured Public-Repo Edition)...")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("id", id_command))

    # ۱. هندلر ویس، فایل صوتی و ویدیو مسیج (موازی و غیرهمزمان)
    app.add_handler(
        MessageHandler(
            filters.VOICE | filters.AUDIO | filters.VIDEO_NOTE,
            process_single_voice,
        )
    )

    # ۲. هندلر دکمه‌های شیشه‌ای خلاصه و جواب انسانی
    app.add_handler(CallbackQueryHandler(button_callback_handler))

    # ۳. هندلر پیام‌های متنی و پیام‌های فورواردشده دارای کپشن
    app.add_handler(
        MessageHandler(
            (filters.TEXT | filters.CAPTION) & (~filters.COMMAND) & (~filters.VOICE) & (~filters.AUDIO) & (~filters.VIDEO_NOTE),
            handle_text_message,
        )
    )

    app.run_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
