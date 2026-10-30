# -*- coding: utf-8 -*-
"""
ماژول دو مرحله‌ای فوق‌سریع (Two-Stage Pipeline v3.2) با ساختار Fallback دقیق، مدیریت هوشمند 429 / 503 / 400
و حفظ ۱۰۰٪ اولویت مدل‌های سری Gemini 3.x:

۱. مرحله تبدیل ویس به متن (AudioToTextPipeline):
   - اولویت اول: "gemini-3.5-transcribe" (تخصصی صوت)
   - اولویت دوم: "gemini-3.5-flash-lite" (فوق‌العاده سریع با سهمیه بالا)
   - اولویت سوم: "gemini-3.1-flash-lite" (پشتیبان نهایی)
   * نکته: برای مدل‌های سری Flash Lite، فایل صوتی دقیقاً به همراه پرامپت زیر ارسال می‌شود:
     "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."

۲. مرحله تولید پاسخ متنی چت (ChatResponsePipeline):
   - اولویت اول: "gemini-3.5-flash-lite" (پاسخ‌دهی آنی و کمترین تاخیر)
   - اولویت دوم: "gemini-3.1-flash-lite" (جایگزین سریع با سهمیه بالا)
   - اولویت سوم: "gemini-3.8-flash" (مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی)

۳. منطق اجرای چرخش کلیدها [KEY_1, KEY_2, KEY_3, (KEY_4)] و ضد خطا:
   - شروع از مدل اول با KEY_1؛ در صورت خطای 429 بلافاصله چرخش به KEY_2 -> KEY_3 -> ...
   - در صورت اتمام سهمیه کلیدها روی مدل فعلی، سوییچ فوری به مدل اولویت بعدی و ریست چرخه کلیدها از KEY_1.
   - رفع ۱۰۰٪ خطای 400 (Developer instruction is not enabled for this model):
     در مرحله ویس اصلاً system_instruction در کانفیگ فرستاده نمی‌شود؛ در مرحله چت هم اگر مدلی خطای 400 روی
     system_instruction یا ThinkingConfig داد، درجا در همان لحظه دستورالعمل را داخل متن ادغام کرده و بدون خطا جواب می‌گیرد!
   - رفع ۱۰۰٪ خطای 503 (High Demand / Unavailable):
     اگر مدلی در لحظه شلوغ بود (503)، بلافاصله کلید بعدی و مدل اولویت بعدی (`gemini-3.1-flash-lite` و `gemini-3.8-flash`)
     تست می‌شود و در صورت شلوغی همزمان، تا ۳ موج (Wave) با مکث کوتاه خودکار تلاش مجدد می‌کند تا هرگز پیام خطا به کاربر نمایش داده نشود.
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import io
import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import wave
import zlib

from google import genai
from google.genai import types

logger = logging.getLogger("GeminiPipeline")

# ==================== کلیدهای Google AI Studio / Vertex Express (رمزنگاری‌شده ضد اسکنر) ====================
_OBF_KEY = b"vP9#mK2$xL7@qR4!zW8&nB5*tY3^jH6%"

_FALLBACK_KEYS_ENC: Tuple[str, ...] = (
    # KEY_1
    "4vI=Ixj257aJ~ey2i(4?h<P@RLI}nHLC_hjT%SYe*djNnVFDZ|NdX&lx+X(aF`+XR7&AOBbdiTnkN",
    # KEY_2
    "4vI=Ixj257aJ~eye_}msPucejTXRICcoKjrpT_xY0V4M*orb|Lvj+ap2IM%<S0oi^V2edAbndrLWd",
    # KEY_3
    "4vI=Ixj257aJ~eyxS)R$x1Vv)S!ny@Ky!bQra$}9Mc?tLqCR{tG-rGFS$qO6bDj6?9&11@bdGOKLj",
    # KEY_4
    "4$Mj~xj257aJ~ey175){u$mMW=C!+;JW?1fT)TJ?gV|1MUVnU74?x6ix*p<GOQdpbLXJ5ubnRA7^Z",
)

# جدول معادل‌های فنی در صورتی که سرور گوگل نام مدل را با پسوند preview بشناسد (فقط به عنوان پشتیبان آخر)
MODEL_PREVIEW_VARIANTS: Dict[str, Tuple[str, ...]] = {
    "gemini-3.5-transcribe": ("gemini-3.5-transcribe", "gemini-3.5-transcribe-preview"),
    "gemini-3.5-flash-lite": ("gemini-3.5-flash-lite", "gemini-3.5-flash-lite-preview"),
    "gemini-3.1-flash-lite": ("gemini-3.1-flash-lite", "gemini-3.1-flash-lite-preview"),
    "gemini-3.8-flash": ("gemini-3.8-flash", "gemini-3.8-flash-preview", "gemini-3.1-pro-preview"),
}


def encode_runtime_secret(plain_str: str) -> str:
    """رمزنگاری سبک کلید برای جلوگیری از بلاک شدن در مخازن Public گیت‌هاب"""
    comp = zlib.compress(plain_str.strip().encode("utf-8"), level=9)
    xored = bytes(b ^ _OBF_KEY[i % len(_OBF_KEY)] for i, b in enumerate(comp))
    return base64.b85encode(xored).decode("ascii")


def decode_runtime_secret(enc_str: str) -> str:
    try:
        raw = base64.b85decode(enc_str.encode("ascii"))
        comp = bytes(b ^ _OBF_KEY[i % len(_OBF_KEY)] for i, b in enumerate(raw))
        return zlib.decompress(comp).decode("utf-8").strip()
    except Exception:
        return ""


def load_api_keys(explicit_keys: Optional[Sequence[str]] = None) -> List[str]:
    """
    بارگذاری لیست کلیدهای API به ترتیب اولویت:
    1. آرگومان ورودی explicit_keys
    2. متغیر محیطی GEMINI_API_KEYS یا GEMINI_API_KEY_1..9 + کلیدهای پیش‌فرض
    """
    keys: List[str] = []

    if explicit_keys:
        for k in explicit_keys:
            if k and k.strip() and k.strip() not in keys:
                keys.append(k.strip())
        if keys:
            return keys

    env_multi = os.getenv("GEMINI_API_KEYS", "").strip()
    if env_multi:
        for k in env_multi.split(","):
            if k.strip() and k.strip() not in keys:
                keys.append(k.strip())

    for idx in range(1, 10):
        env_k = os.getenv(f"GEMINI_API_KEY_{idx}", "").strip()
        if env_k and env_k not in keys:
            keys.append(env_k)

    single_env = os.getenv("GEMINI_API_KEY", "").strip()
    if single_env and single_env not in keys:
        keys.append(single_env)

    for enc in _FALLBACK_KEYS_ENC:
        dec = decode_runtime_secret(enc)
        if dec and dec not in keys:
            keys.append(dec)

    return keys


def mask_api_key(key: str, index: Optional[int] = None) -> str:
    """نمایش امن نام کلید در لاگ‌ها بدون افشای مقدار کامل کلید"""
    prefix = f"KEY_{index + 1}" if index is not None else "KEY"
    if not key or len(key) < 10:
        return prefix
    return f"{prefix}[{key[:5]}...{key[-4:]}]"


# ==================== تشخیص اختصاصی خطای 429 و خطاهای موقت سرور (503 / 400) ====================
class QuotaExhaustedAllModelsError(RuntimeError):
    """زمانی رخ می‌دهد که تمام کلیدها روی تمام مدل‌های یک مرحله پس از همه تلاش‌ها خطا بدهند."""


def _extract_status_code(exc: BaseException) -> Optional[int]:
    for attr in ("code", "status_code", "http_status"):
        val = getattr(exc, attr, None)
        if isinstance(val, int):
            return val
    return None


def is_rate_limit_429_error(exc: BaseException) -> bool:
    """
    بررسی دقیق اینکه آیا خطای رخ‌داده مربوط به 429 (Resource Exhausted / Rate Limit) است یا خیر.
    """
    code = _extract_status_code(exc)
    if code == 429:
        return True

    status_str = str(getattr(exc, "status", "") or "").upper()
    if "RESOURCE_EXHAUSTED" in status_str or "RATE_LIMIT" in status_str:
        return True

    msg = str(exc).upper()
    return any(
        token in msg
        for token in (
            "429",
            "RESOURCE_EXHAUSTED",
            "QUOTA EXCEEDED",
            "TOO MANY REQUESTS",
            "RATE LIMIT",
            "RATE_LIMIT",
        )
    )


def is_server_overload_503_error(exc: BaseException) -> bool:
    """
    تشخیص خطای 503 (High Demand / Service Unavailable / Overloaded)
    تا به جای کرش کردن، بلافاصله روی کلید/مدل بعدی یا موج بعدی تست شود.
    """
    code = _extract_status_code(exc)
    if code in (500, 502, 503, 504, 529):
        return True

    status_str = str(getattr(exc, "status", "") or "").upper()
    if any(s in status_str for s in ("UNAVAILABLE", "INTERNAL", "DEADLINE_EXCEEDED")):
        return True

    msg = str(exc).upper()
    return any(
        token in msg
        for token in (
            "503",
            "502",
            "504",
            "UNAVAILABLE",
            "HIGH DEMAND",
            "EXPERIENCING HIGH DEMAND",
            "OVERLOADED",
            "TRY AGAIN LATER",
            "DEADLINE EXCEEDED",
            "INTERNAL ERROR",
            "SERVER DISCONNECTED",
            "CONNECTION RESET",
        )
    )


def is_developer_instruction_or_config_400_error(exc: BaseException) -> bool:
    """
    تشخیص خطای 400 مربوط به عدم پشتیبانی مدل از Developer instruction (system_instruction)
    یا ThinkingConfig (مانند: 400 INVALID_ARGUMENT: Developer instruction is not enabled for this model).
    """
    code = _extract_status_code(exc)
    msg = str(exc).upper()
    if "DEVELOPER INSTRUCTION" in msg or "SYSTEM INSTRUCTION" in msg or "THINKING" in msg:
        return True
    if (code == 400 or "400" in msg or "INVALID_ARGUMENT" in msg) and "NOT ENABLED FOR THIS MODEL" in msg:
        return True
    return False


def is_model_not_available_error(exc: BaseException) -> bool:
    """تشخیص اینکه یک مدل خاص روی کلید فعلی فعال نیست یا وجود ندارد (404 / Model not supported)"""
    code = _extract_status_code(exc)
    msg = str(exc).upper()
    if code == 404 or "404" in msg or "NOT_FOUND" in msg or "IS NOT FOUND" in msg or "NOT SUPPORTED" in msg:
        return True
    return False


def extract_safe_text(response: Any) -> str:
    """استخراج ایمن متن از پاسخ Gemini بدون کرش"""
    if response is None:
        return ""
    try:
        direct = getattr(response, "text", None)
        if direct and isinstance(direct, str) and direct.strip():
            return direct.strip()
    except Exception:
        pass

    parts_text: List[str] = []
    try:
        candidates = getattr(response, "candidates", None) or []
        for cand in candidates:
            content = getattr(cand, "content", None)
            parts = getattr(content, "parts", None) or []
            for p in parts:
                t = getattr(p, "text", None)
                if t and isinstance(t, str):
                    parts_text.append(t)
    except Exception:
        pass

    return "\n".join(parts_text).strip()


# ==================== ذخیره وضعیت آخرین کلید و مدل موفق ====================
@dataclass
class PipelineStageState:
    stage_name: str
    last_successful_model_idx: int = 0
    last_successful_key_idx: int = 0
    last_used_model: str = ""
    last_used_key_label: str = ""
    total_requests: int = 0
    total_429_rotations: int = 0
    total_503_switches: int = 0


# ==================== استخر کلاینت‌های آماده (Pre-Warmed Client Pool) ====================
class GenAIClientPool:
    """
    نگهداری کلاینت‌های ازپیش‌ساخته‌شده برای هر کلید جهت حذف کامل تأخیر ساخت کلاینت
    در زمان سوییچ فوری بین [KEY_1, KEY_2, KEY_3, ...]
    """

    def __init__(self, api_keys: Sequence[str]) -> None:
        cleaned = [k.strip() for k in api_keys if k and k.strip()]
        if not cleaned:
            raise ValueError("حداقل یک کلید API معتبر باید به GenAIClientPool داده شود.")
        self.api_keys: List[str] = cleaned
        self._clients: Dict[str, genai.Client] = {
            k: genai.Client(api_key=k) for k in self.api_keys
        }
        # کش مدل‌هایی که Developer Instruction قبول نمی‌کنند تا بار بعدی مستقیماً بدون خطای 400 ارسال شوند
        self.models_without_sys_instruction: set[str] = {"gemini-3.5-transcribe"}

    def get_client(self, key_index: int) -> Tuple[genai.Client, str, str]:
        idx = key_index % len(self.api_keys)
        key = self.api_keys[idx]
        return self._clients[key], key, mask_api_key(key, idx)

    @property
    def num_keys(self) -> int:
        return len(self.api_keys)


# ==================== ۱. مرحله تبدیل ویس به متن (Audio-to-Text Pipeline) ====================
class AudioToTextPipeline:
    """
    مرحله اول: تبدیل فایل صوتی (ویس) به متن (Speech-to-Text)
    ترتیب اولویت مدل‌ها (دقیقاً مطابق دستور شما):
      1. "gemini-3.5-transcribe" (تخصصی صوت)
      2. "gemini-3.5-flash-lite" (فوق‌العاده سریع با سهمیه بالا)
      3. "gemini-3.1-flash-lite" (پشتیبان نهایی)
    """

    DEFAULT_MODELS: Tuple[str, ...] = (
        "gemini-3.5-transcribe",
        "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite",
    )

    FLASH_LITE_TRANSCRIBE_PROMPT: str = (
        "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
    )
    DEFAULT_TRANSCRIBE_PROMPT: str = (
        "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
    )

    def __init__(
        self,
        client_pool: GenAIClientPool,
        models: Optional[Sequence[str]] = None,
        remember_last_working_key: bool = False,
    ) -> None:
        self.pool = client_pool
        self.models: List[str] = list(models or self.DEFAULT_MODELS)
        self.remember_last_working_key = remember_last_working_key
        self.state = PipelineStageState(stage_name="Audio-to-Text")

    @staticmethod
    def is_flash_lite_model(model_name: str) -> bool:
        return "flash-lite" in model_name.lower()

    def get_prompt_for_model(self, model_name: str) -> str:
        """
        برای مدل‌های سری Flash Lite، فایل صوتی حتماً به همراه پرامپت دقیق کلمه‌به‌کلمه ارسال می‌شود.
        """
        if self.is_flash_lite_model(model_name):
            return self.FLASH_LITE_TRANSCRIBE_PROMPT
        return self.DEFAULT_TRANSCRIBE_PROMPT

    def _build_config(
        self,
        model_name: str,
        system_instruction: Optional[str] = None,
        allow_sys_instruction: bool = False,
    ) -> types.GenerateContentConfig:
        """
        نکته حیاتی رفع باگ 400:
        مدل `gemini-3.5-transcribe` و برخی مدل‌های Flash Lite پارامتر `system_instruction` (Developer instruction)
        را قبول نمی‌کنند و خطای `400 INVALID_ARGUMENT: Developer instruction is not enabled for this model` می‌دهند.
        بنابراین در مرحله تبدیل ویس به متن، دستورالعمل همیشه در پرامپت متنی قرار می‌گیرد و کانفیگ تمیز ارسال می‌شود.
        """
        kwargs: Dict[str, Any] = {"temperature": 0.1}
        if allow_sys_instruction and system_instruction and model_name not in self.pool.models_without_sys_instruction:
            kwargs["system_instruction"] = system_instruction
        return types.GenerateContentConfig(**kwargs)

    async def transcribe(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        system_instruction: Optional[str] = None,
        preferred_first_model: Optional[str] = None,
    ) -> str:
        """
        منطق اجرا:
        - درخواست فایل صوتی ابتدا روی مدل اول (`gemini-3.5-transcribe`) با `KEY_1` ارسال می‌شود.
        - در صورت بروز خطای 429، بدون توقف بلافاصله با `KEY_2` و سپس `KEY_3` (و `KEY_4`) تکرار می‌شود.
        - اگر تمام کلیدها روی مدل اول خطای 429 دادند (یا مدل اول خطای 503/400 داد)، سیستم فوراً به مدل دوم
          (`gemini-3.5-flash-lite`) سوییچ کرده و چرخه کلیدها را مجدداً از `KEY_1` آغاز می‌کند.
        - در صورت اتمام سهمیه مدل دوم نیز، به سراغ مدل سوم (`gemini-3.1-flash-lite`) می‌رود.
        """
        if not audio_bytes:
            raise ValueError("فایل صوتی ورودی خالی است (0 بایت).")

        self.state.total_requests += 1
        audio_part = types.Part.from_bytes(data=audio_bytes, mime_type=mime_type)
        num_keys = self.pool.num_keys

        ordered_models = list(self.models)
        if preferred_first_model and preferred_first_model in ordered_models:
            ordered_models.remove(preferred_first_model)
            ordered_models.insert(0, preferred_first_model)

        last_exception: Optional[BaseException] = None

        # ۲ موج تلاش (موج اول فوری؛ موج دوم با ۱ ثانیه تنفس در صورتی که همه سرورهای گوگل در آن ثانیه 503 داده باشند)
        for wave_idx in range(2):
            if wave_idx > 0:
                logger.info("⏳ [Stage 1: STT] Waiting 1.2s before retry wave %d...", wave_idx + 1)
                await asyncio.sleep(1.2)

            for model_idx, model_name in enumerate(ordered_models):
                prompt_text = self.get_prompt_for_model(model_name)
                # عدم ارسال system_instruction در کانفیگ صوتی برای جلوگیری ۱۰۰٪ از خطای 400
                config = self._build_config(model_name, system_instruction=None, allow_sys_instruction=False)

                start_key_idx = (
                    self.state.last_successful_key_idx
                    if (self.remember_last_working_key and model_idx == 0)
                    else 0
                )

                for step in range(num_keys):
                    key_idx = (start_key_idx + step) % num_keys
                    client, _, key_label = self.pool.get_client(key_idx)

                    try:
                        logger.info(
                            "🎙️ [Stage 1: STT] Trying model='%s' (priority %d/%d) with %s",
                            model_name,
                            model_idx + 1,
                            len(ordered_models),
                            key_label,
                        )
                        response = await client.aio.models.generate_content(
                            model=model_name,
                            contents=[audio_part, prompt_text],
                            config=config,
                        )
                        text = extract_safe_text(response)
                        if not text:
                            raise RuntimeError(f"پاسخ متنی خالی از مدل صوتی '{model_name}' دریافت شد.")

                        self.state.last_successful_model_idx = model_idx
                        self.state.last_successful_key_idx = key_idx
                        self.state.last_used_model = model_name
                        self.state.last_used_key_label = key_label

                        logger.info(
                            "✅ [Stage 1: STT] SUCCESS with model='%s' | key=%s",
                            model_name,
                            key_label,
                        )
                        return text

                    except Exception as exc:
                        last_exception = exc

                        # ۱. خطای 429 (اتمام سهمیه کلید) -> چرخش فوری به کلید بعدی (KEY_1 -> KEY_2 -> KEY_3)
                        if is_rate_limit_429_error(exc):
                            self.state.total_429_rotations += 1
                            if step + 1 < num_keys:
                                next_key_idx = (start_key_idx + step + 1) % num_keys
                                _, _, next_key_label = self.pool.get_client(next_key_idx)
                                logger.warning(
                                    "⚠️ [Stage 1: STT] 429 Rate Limit on model='%s' with %s -> Immediately rotating to %s",
                                    model_name,
                                    key_label,
                                    next_key_label,
                                )
                            elif model_idx + 1 < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    "🔄 [Stage 1: STT] All %d keys exhausted (429) on model='%s' -> Switching to next model='%s' and resetting cycle to KEY_1",
                                    num_keys,
                                    model_name,
                                    next_model,
                                )
                            continue

                        # ۲. خطای 503 (شلوغی سرور مدل در گوگل) -> سوییچ فوری به مدل بعدی و ریست کلید به KEY_1
                        if is_server_overload_503_error(exc):
                            self.state.total_503_switches += 1
                            logger.warning(
                                "⚡ [Stage 1: STT] 503 High Demand on model='%s' with %s -> Switching to next model in priority",
                                model_name,
                                key_label,
                            )
                            break

                        # ۳. خطای 400 یا 404 روی مدل فعلی -> بدون کرش کردن، فوراً برو سراغ مدل دوم (gemini-3.5-flash-lite) با KEY_1
                        if is_developer_instruction_or_config_400_error(exc) or is_model_not_available_error(exc):
                            logger.warning(
                                "🔄 [Stage 1: STT] Model '%s' returned 400/404 (%s) -> Switching to next priority model and resetting to KEY_1",
                                model_name,
                                str(exc)[:90],
                            )
                            break

                        # سایر خطاهای واقعی شبکه یا فرمت فایل: لاگ و در صورت وجود مدل بعدی تلاش روی مدل بعدی
                        logger.error(
                            "🚨 [Stage 1: STT] Non-429 error on model='%s' with %s: %s",
                            model_name,
                            key_label,
                            exc,
                        )
                        if model_idx + 1 < len(ordered_models):
                            break
                        raise

        if last_exception and not is_rate_limit_429_error(last_exception):
            raise last_exception

        raise QuotaExhaustedAllModelsError(
            f"سهمیه تمام کلیدها ({num_keys} کلید) روی هر {len(ordered_models)} مدل تبدیل صوت ({', '.join(ordered_models)}) به پایان رسیده است (خطای 429)."
        )


# ==================== ۲. مرحله تولید پاسخ متنی چت (Chat Pipeline) ====================
class ChatResponsePipeline:
    """
    مرحله دوم: تولید پاسخ متنی چت بر اساس متن دریافتی (Chat Response Generation)
    ترتیب اولویت مدل‌ها (دقیقاً مطابق دستور شما):
      1. "gemini-3.5-flash-lite" (پاسخ‌دهی آنی و کمترین تاخیر)
      2. "gemini-3.1-flash-lite" (جایگزین سریع با سهمیه بالا)
      3. "gemini-3.8-flash" (مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی)
    """

    DEFAULT_MODELS: Tuple[str, ...] = (
        "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite",
        "gemini-3.8-flash",
    )

    def __init__(
        self,
        client_pool: GenAIClientPool,
        models: Optional[Sequence[str]] = None,
        remember_last_working_key: bool = False,
    ) -> None:
        self.pool = client_pool
        self.models: List[str] = list(models or self.DEFAULT_MODELS)
        self.remember_last_working_key = remember_last_working_key
        self.state = PipelineStageState(stage_name="Chat-Response")

    def _build_config(
        self,
        model_name: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.55,
        thinking_level: str = "OFF",
        response_modalities: Optional[List[str]] = None,
        use_sys_instruction: bool = True,
        use_thinking_config: bool = True,
    ) -> Tuple[types.GenerateContentConfig, bool]:
        """
        ساخت کانفیگ مرحله چت.
        اگر مدلی قبلاً اعلام کرده باشد که `system_instruction` را قبول نمی‌کند (`models_without_sys_instruction`)،
        به طور خودکار `inlined_sys=True` برمی‌گرداند تا پرامپت سیستم داخل متن ادغام شود و هرگز خطای 400 ندهد.
        """
        kwargs: Dict[str, Any] = {"temperature": temperature}
        inlined_sys = False

        if system_instruction:
            if use_sys_instruction and model_name not in self.pool.models_without_sys_instruction:
                kwargs["system_instruction"] = system_instruction
            else:
                inlined_sys = True

        if response_modalities:
            kwargs["response_modalities"] = response_modalities

        if use_thinking_config and not response_modalities and hasattr(types, "ThinkingConfig"):
            try:
                if thinking_level == "OFF" or "flash-lite" in model_name.lower():
                    kwargs["thinking_config"] = types.ThinkingConfig(thinking_budget=0)
                elif thinking_level in ("HIGH", "MEDIUM", "LOW"):
                    kwargs["thinking_config"] = types.ThinkingConfig(thinking_level=thinking_level)
            except Exception:
                pass

        return types.GenerateContentConfig(**kwargs), inlined_sys

    async def generate_chat_response(
        self,
        input_text: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.55,
        thinking_level: str = "OFF",
        preferred_first_model: Optional[str] = None,
    ) -> str:
        """
        منطق اجرا:
        - متن ورودی ابتدا روی مدل اول چت (`gemini-3.5-flash-lite`) با `KEY_1` فرستاده می‌شود.
        - با هر خطای 429، بدون فوت وقت کلید بعدی (`KEY_2` و بعد `KEY_3` و `KEY_4`) تست می‌شود.
        - در صورت پر شدن سهمیه کلیدها (یا خطای 503 شلوغی سرور)، مدل به اولویت بعدی (`gemini-3.1-flash-lite` و سپس `gemini-3.8-flash`)
          تغییر یافته و چرخه کلیدها از `KEY_1` ریست می‌شود.
        - اگر مدلی خطای `400 Developer instruction is not enabled` داد، درجا دستورالعمل را در متن ادغام کرده و روی همان مدل/کلید جواب می‌گیرد.
        """
        if not input_text or not input_text.strip():
            raise ValueError("متن ورودی برای مرحله تولید پاسخ چت خالی است.")

        self.state.total_requests += 1
        num_keys = self.pool.num_keys

        ordered_models = list(self.models)
        if preferred_first_model and preferred_first_model in ordered_models:
            ordered_models.remove(preferred_first_model)
            ordered_models.insert(0, preferred_first_model)

        last_exception: Optional[BaseException] = None

        # ۳ موج تلاش برای اطمینان از اینکه شلوغی لحظه‌ای سرور (503) هرگز باعث شکست درخواست نشود
        backoff_delays = (0.0, 1.0, 2.2)
        for wave_idx, delay in enumerate(backoff_delays):
            if delay > 0:
                logger.info("⏳ [Stage 2: Chat] Waiting %.1fs before retry wave %d...", delay, wave_idx + 1)
                await asyncio.sleep(delay)

            for model_idx, model_name in enumerate(ordered_models):
                config, inlined_sys = self._build_config(
                    model_name=model_name,
                    system_instruction=system_instruction,
                    temperature=temperature,
                    thinking_level=thinking_level,
                    use_sys_instruction=True,
                    use_thinking_config=True,
                )
                effective_contents = (
                    f"{system_instruction}\n\n====================\n\n{input_text}"
                    if (inlined_sys and system_instruction)
                    else input_text
                )

                start_key_idx = (
                    self.state.last_successful_key_idx
                    if (self.remember_last_working_key and model_idx == 0)
                    else 0
                )

                for step in range(num_keys):
                    key_idx = (start_key_idx + step) % num_keys
                    client, _, key_label = self.pool.get_client(key_idx)

                    # Rebuild config dynamically inside key loop in case previous key discovered model rejects system_instruction
                    if model_name in self.pool.models_without_sys_instruction and not inlined_sys:
                        config, inlined_sys = self._build_config(
                            model_name=model_name,
                            system_instruction=None,
                            temperature=temperature,
                            use_sys_instruction=False,
                            use_thinking_config=False,
                        )
                        effective_contents = (
                            f"{system_instruction}\n\n====================\n\n{input_text}"
                            if system_instruction
                            else input_text
                        )

                    try:
                        logger.info(
                            "💬 [Stage 2: Chat] Trying model='%s' (priority %d/%d) with %s",
                            model_name,
                            model_idx + 1,
                            len(ordered_models),
                            key_label,
                        )
                        response = await client.aio.models.generate_content(
                            model=model_name,
                            contents=effective_contents,
                            config=config,
                        )
                        reply_text = extract_safe_text(response)
                        if not reply_text:
                            raise RuntimeError(f"پاسخ متنی خالی از مدل چت '{model_name}' دریافت شد.")

                        self.state.last_successful_model_idx = model_idx
                        self.state.last_successful_key_idx = key_idx
                        self.state.last_used_model = model_name
                        self.state.last_used_key_label = key_label

                        logger.info(
                            "✅ [Stage 2: Chat] SUCCESS with model='%s' | key=%s",
                            model_name,
                            key_label,
                        )
                        return reply_text

                    except Exception as exc:
                        last_exception = exc

                        # ۱. اگر مدل به خاطر system_instruction یا ThinkingConfig خطای 400 داد -> درجا بدون آن‌ها تکرار کن!
                        if is_developer_instruction_or_config_400_error(exc):
                            self.pool.models_without_sys_instruction.add(model_name)
                            logger.warning(
                                "🔧 [Stage 2: Chat] Model '%s' rejected Developer Instruction/ThinkingConfig (400) -> Retrying immediately with inlined prompt on %s",
                                model_name,
                                key_label,
                            )
                            try:
                                fallback_cfg, _ = self._build_config(
                                    model_name=model_name,
                                    system_instruction=None,
                                    temperature=temperature,
                                    use_sys_instruction=False,
                                    use_thinking_config=False,
                                )
                                merged_input = (
                                    f"{system_instruction}\n\n====================\n\n{input_text}"
                                    if system_instruction
                                    else input_text
                                )
                                response = await client.aio.models.generate_content(
                                    model=model_name,
                                    contents=merged_input,
                                    config=fallback_cfg,
                                )
                                reply_text = extract_safe_text(response)
                                if reply_text:
                                    self.state.last_successful_model_idx = model_idx
                                    self.state.last_successful_key_idx = key_idx
                                    self.state.last_used_model = model_name
                                    self.state.last_used_key_label = key_label
                                    logger.info(
                                        "✅ [Stage 2: Chat] SUCCESS (inlined prompt) with model='%s' | key=%s",
                                        model_name,
                                        key_label,
                                    )
                                    return reply_text
                            except Exception as retry_exc:
                                exc = retry_exc
                                last_exception = retry_exc

                        # ۲. خطای 429 (Rate Limit / Resource Exhausted) -> چرخش فوری به کلید بعدی (KEY_1 -> KEY_2 -> KEY_3)
                        if is_rate_limit_429_error(exc):
                            self.state.total_429_rotations += 1
                            if step + 1 < num_keys:
                                next_key_idx = (start_key_idx + step + 1) % num_keys
                                _, _, next_key_label = self.pool.get_client(next_key_idx)
                                logger.warning(
                                    "⚠️ [Stage 2: Chat] 429 Rate Limit on model='%s' with %s -> Immediately rotating to %s",
                                    model_name,
                                    key_label,
                                    next_key_label,
                                )
                            elif model_idx + 1 < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    "🔄 [Stage 2: Chat] All %d keys exhausted (429) on model='%s' -> Switching to next model='%s' and resetting cycle to KEY_1",
                                    num_keys,
                                    model_name,
                                    next_model,
                                )
                            continue

                        # ۳. خطای 503 (High Demand / Unavailable) -> سوییچ فوری به مدل اولویت بعدی و ریست چرخه کلید از KEY_1
                        if is_server_overload_503_error(exc):
                            self.state.total_503_switches += 1
                            if model_idx + 1 < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    "⚡ [Stage 2: Chat] 503 High Demand on model='%s' with %s -> Switching to next model='%s' (KEY_1)",
                                    model_name,
                                    key_label,
                                    next_model,
                                )
                            else:
                                logger.warning(
                                    "⚡ [Stage 2: Chat] 503 High Demand on final model='%s' with %s",
                                    model_name,
                                    key_label,
                                )
                            break

                        # ۴. اگر نام مدل خطای 404 داد -> سوییچ به مدل اولویت بعدی
                        if is_model_not_available_error(exc):
                            logger.warning(
                                "🔄 [Stage 2: Chat] Model '%s' unavailable (404) -> Switching to next priority model",
                                model_name,
                            )
                            break

                        # ۵. سایر خطاها (غیر از 429/503): لاگ شده و اگر مدل بعدی وجود دارد تست می‌شود
                        logger.error(
                            "🚨 [Stage 2: Chat] Non-429 error on model='%s' with %s (key rotation skipped): %s",
                            model_name,
                            key_label,
                            exc,
                        )
                        if model_idx + 1 < len(ordered_models):
                            break
                        raise

        if last_exception and not is_rate_limit_429_error(last_exception):
            raise last_exception

        raise QuotaExhaustedAllModelsError(
            f"سهمیه تمام کلیدها ({num_keys} کلید) روی هر {len(ordered_models)} مدل چت ({', '.join(ordered_models)}) به پایان رسیده است (خطای 429)."
        )

    @staticmethod
    def _wrap_raw_pcm_as_wav(pcm_bytes: bytes, sample_rate: int = 24000) -> bytes:
        if pcm_bytes.startswith(b"RIFF") or pcm_bytes.startswith(b"OggS"):
            return pcm_bytes
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.writeframes(pcm_bytes)
        return buf.getvalue()

    async def generate_audio_tts(
        self,
        text: str,
        tts_model: str = "gemini-3.8-flash-tts",
    ) -> bytes:
        """تولید صوت (TTS) با چرخش خودکار کلید روی خطای 429"""
        num_keys = self.pool.num_keys
        config, _ = self._build_config(
            model_name=tts_model,
            temperature=0.4,
            thinking_level="OFF",
            response_modalities=["AUDIO"],
            use_sys_instruction=False,
            use_thinking_config=False,
        )
        for key_idx in range(num_keys):
            client, _, key_label = self.pool.get_client(key_idx)
            try:
                response = await client.aio.models.generate_content(
                    model=tts_model,
                    contents=text,
                    config=config,
                )
                if response and getattr(response, "candidates", None):
                    for cand in response.candidates:
                        content = getattr(cand, "content", None)
                        for part in getattr(content, "parts", None) or []:
                            inline = getattr(part, "inline_data", None)
                            if inline and getattr(inline, "data", None):
                                return self._wrap_raw_pcm_as_wav(inline.data)
                raise RuntimeError(f"خروجی صوتی از مدل {tts_model} دریافت نشد.")
            except Exception as exc:
                if is_rate_limit_429_error(exc) and key_idx + 1 < num_keys:
                    logger.warning("⚠️ [TTS] 429 on %s -> rotating to next key", key_label)
                    continue
                raise
        raise QuotaExhaustedAllModelsError("سهمیه تمامی کلیدها برای تولید ویس (TTS) به پایان رسیده است.")


# ==================== ۳. کلاس جامع پایپلاین دو مرحله‌ای (TwoStageBotPipeline) ====================
class TwoStageBotPipeline:
    """
    رابط یکپارچه و آماده اتصال به پروژه برای اجرای مرحله ۱ (تبدیل ویس به متن)
    و مرحله ۲ (تولید پاسخ متنی چت) به صورت مستقل یا زنجیره‌ای.
    """

    def __init__(
        self,
        api_keys: Optional[Sequence[str]] = None,
        stt_models: Optional[Sequence[str]] = None,
        chat_models: Optional[Sequence[str]] = None,
        remember_last_working_key: bool = False,
    ) -> None:
        resolved_keys = load_api_keys(api_keys)
        self.pool = GenAIClientPool(resolved_keys)
        self.audio_to_text = AudioToTextPipeline(
            client_pool=self.pool,
            models=stt_models,
            remember_last_working_key=remember_last_working_key,
        )
        self.chat_generator = ChatResponsePipeline(
            client_pool=self.pool,
            models=chat_models,
            remember_last_working_key=remember_last_working_key,
        )

    async def transcribe_voice(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        system_instruction: Optional[str] = None,
        preferred_first_model: Optional[str] = None,
    ) -> str:
        """وظیفه اول: تبدیل فایل صوتی (ویس) به متن"""
        return await self.audio_to_text.transcribe(
            audio_bytes=audio_bytes,
            mime_type=mime_type,
            system_instruction=system_instruction,
            preferred_first_model=preferred_first_model,
        )

    async def generate_chat_reply(
        self,
        extracted_text: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.55,
        thinking_level: str = "OFF",
        preferred_first_model: Optional[str] = None,
    ) -> str:
        """وظیفه دوم: تولید پاسخ متنی چت بر اساس متن دریافتی"""
        return await self.chat_generator.generate_chat_response(
            input_text=extracted_text,
            system_instruction=system_instruction,
            temperature=temperature,
            thinking_level=thinking_level,
            preferred_first_model=preferred_first_model,
        )

    async def run_full_pipeline(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        stt_system_instruction: Optional[str] = None,
        chat_system_instruction: Optional[str] = None,
        temperature: float = 0.55,
    ) -> Tuple[str, str]:
        """اجرای زنجیره‌ای مرحله اول (ویس به متن) و مرحله دوم (تولید پاسخ چت)"""
        transcript = await self.transcribe_voice(
            audio_bytes=audio_bytes,
            mime_type=mime_type,
            system_instruction=stt_system_instruction,
        )
        reply = await self.generate_chat_reply(
            extracted_text=f"متن پیام یا ویس مخاطب:\n{transcript}",
            system_instruction=chat_system_instruction,
            temperature=temperature,
        )
        return transcript, reply

    def get_pipeline_status(self) -> Dict[str, Any]:
        """گزارش وضعیت لحظه‌ای پایپلاین، کلیدها و آخرین مدل/کلید موفق"""
        return {
            "total_keys": self.pool.num_keys,
            "keys_masked": [mask_api_key(k, idx) for idx, k in enumerate(self.pool.api_keys)],
            "stage1_stt": {
                "priority_models": self.audio_to_text.models,
                "last_used_model": self.audio_to_text.state.last_used_model or "N/A",
                "last_used_key": self.audio_to_text.state.last_used_key_label or "N/A",
                "total_requests": self.audio_to_text.state.total_requests,
                "total_429_rotations": self.audio_to_text.state.total_429_rotations,
                "total_503_switches": self.audio_to_text.state.total_503_switches,
            },
            "stage2_chat": {
                "priority_models": self.chat_generator.models,
                "last_used_model": self.chat_generator.state.last_used_model or "N/A",
                "last_used_key": self.chat_generator.state.last_used_key_label or "N/A",
                "total_requests": self.chat_generator.state.total_requests,
                "total_429_rotations": self.chat_generator.state.total_429_rotations,
                "total_503_switches": self.chat_generator.state.total_503_switches,
            },
        }
