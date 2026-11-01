#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ماژول دو مرحله‌ای (Two-Stage Pipeline) تبدیل ویس به متن (Speech-to-Text) و تولید پاسخ چت (Chat Response Generation)
با مدیریت اختصاصی خطای 429 (Resource Exhausted / Rate Limit) و چرخش هوشمند بین ۳ کلید مجزا [KEY_1, KEY_2, KEY_3].

ویژگی‌های معماری:
1. مرحله اول (AudioToTextPipeline):
   - اولویت اول: "gemini-3.5-transcribe" (تخصصی صوت)
   - اولویت دوم: "gemini-3.5-flash-lite" (فوق‌العاده سریع با سهمیه بالا)
   - اولویت سوم: "gemini-3.1-flash-lite" (پشتیبان نهایی)
   - چرخش کلیدها روی هر مدل: KEY_1 -> KEY_2 -> KEY_3 (بلافاصله و بدون توقف در صورت خطای 429)
   - ریست چرخه کلیدها به KEY_1 به محض سوییچ به مدل اولویت بعدی
   - ارسال خودکار پرامپت اختصاصی برای مدل‌های سری Flash Lite:
     "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."

2. مرحله دوم (ChatPipeline):
   - اولویت اول: "gemini-3.5-flash-lite" (پاسخ‌دهی آنی و کمترین تاخیر)
   - اولویت دوم: "gemini-3.1-flash-lite" (جایگزین سریع با سهمیه بالا)
   - اولویت سوم: "gemini-3.8-flash" (مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی)
   - چرخش کلیدها روی هر مدل: KEY_1 -> KEY_2 -> KEY_3 در صورت بروز 429 و ریست به KEY_1 در مدل بعدی

3. مدیریت خطا و وضعیت:
   - تفکیک دقیق خطای 429 از سایر خطاها (خطاهای شبکه، فرمت فایل، 400، 401، 403، 404 و غیره وارد چرخه تعویض کلید نمی‌شوند و فوراً لاگ و Raise می‌شوند).
   - ذخیره وضعیت آخرین کلید و مدل موفق (Last Successful State) + قابلیت شروع هوشمند از اولویت اول یا ادامه از آخرین وضعیت موفق.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

try:
    from google import genai
    from google.genai import types
except ImportError:
    genai = None  # type: ignore
    types = None  # type: ignore


logger = logging.getLogger("VoiceAndPasokhBot.Pipeline")

# ==================== مشخصات کلیدها و اولویت مدل‌ها ====================

AUDIO_PIPELINE_MODELS: Tuple[str, ...] = (
    "gemini-3.5-transcribe",   # اولویت اول: تخصصی صوت
    "gemini-3.5-flash-lite",   # اولویت دوم: فوق‌العاده سریع با سهمیه بالا
    "gemini-3.1-flash-lite",   # اولویت سوم: پشتیبان نهایی
)

CHAT_PIPELINE_MODELS: Tuple[str, ...] = (
    "gemini-3.5-flash-lite",   # اولویت اول: پاسخ‌دهی آنی و کمترین تاخیر
    "gemini-3.1-flash-lite",   # اولویت دوم: جایگزین سریع با سهمیه بالا
    "gemini-3.8-flash",        # اولویت سوم: مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی
)

# پرامپت الزامی برای مدل‌های سری Flash Lite در مرحله تبدیل ویس به متن
FLASH_LITE_TRANSCRIPTION_PROMPT: str = (
    "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
)

# پرامپت پیش‌فرض برای مدل تخصصی صوت (gemini-3.5-transcribe)
DEFAULT_TRANSCRIPTION_PROMPT: str = (
    "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
)


def load_three_api_keys(fallback_key: str = "") -> List[str]:
    """
    بارگذاری ۳ کلید مجزا [KEY_1, KEY_2, KEY_3] از متغیرهای محیطی:
    - ابتدا KEY_1, KEY_2, KEY_3 یا GEMINI_API_KEY_1, GEMINI_API_KEY_2, GEMINI_API_KEY_3 بررسی می‌شوند.
    - در صورت ارسال کلیدهای جداشده با ویرگول در GEMINI_API_KEYS یا GEMINI_API_KEY، به ترتیب تفکیک می‌شوند.
    - اگر فقط یک کلید تنظیم شده باشد، برای جلوگیری از خطای 400 کلید نامعتبر، اسلات‌های خالی با کلید اصلی پر می‌شوند.
    """
    raw_multi = (
        os.getenv("GEMINI_API_KEYS", "").strip()
        or os.getenv("GEMINI_API_KEY", "").strip()
    )
    split_keys: List[str] = []
    if "," in raw_multi or ";" in raw_multi:
        split_keys = [k.strip() for k in re.split(r"[,;\s]+", raw_multi) if k.strip()]

    key_1 = (
        os.getenv("KEY_1", "").strip()
        or os.getenv("GEMINI_API_KEY_1", "").strip()
        or (split_keys[0] if len(split_keys) >= 1 else "")
        or (raw_multi if not split_keys else "")
        or fallback_key.strip()
    )
    key_2 = (
        os.getenv("KEY_2", "").strip()
        or os.getenv("GEMINI_API_KEY_2", "").strip()
        or (split_keys[1] if len(split_keys) >= 2 else "")
        or key_1
    )
    key_3 = (
        os.getenv("KEY_3", "").strip()
        or os.getenv("GEMINI_API_KEY_3", "").strip()
        or (split_keys[2] if len(split_keys) >= 3 else "")
        or key_1
    )
    return [key_1, key_2, key_3]


# ==================== استثناها و تشخیص اختصاصی خطای 429 ====================


class AllModelsAndKeysExhaustedError(RuntimeError):
    """زمانی رخ می‌دهد که تمام کلیدهای [KEY_1, KEY_2, KEY_3] روی تمام مدل‌های اولویت خطای 429 بدهند."""

    def __init__(self, pipeline_name: str, attempts: int, last_error: Optional[BaseException] = None) -> None:
        self.pipeline_name = pipeline_name
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"[{pipeline_name}] سهمیه تمامی کلیدها و مدل‌ها پس از {attempts} تلاش به اتمام رسید (HTTP 429)."
        )


def is_http_429_error(exc: BaseException) -> bool:
    """
    بررسی دقیق اینکه آیا خطای رخ‌داده مربوط به محدودیت نرخ/سهمیه (HTTP 429 / Resource Exhausted) است یا خیر.
    خطاهای شبکه، فرمت فایل، کلید نامعتبر (400/401/403)، عدم وجود مدل (404) و غیره False برمی‌گردانند.
    """
    for attr in ("code", "status_code", "http_status", "status"):
        val = getattr(exc, attr, None)
        if val == 429 or str(val) == "429":
            return True
        if isinstance(val, str) and "RESOURCE_EXHAUSTED" in val.upper():
            return True

    msg = str(exc).upper()
    # اطمینان از عدم تداخل با کدهای خطای دیگر که به صورت تصادفی عدد 429 در آیدی پیام دارند
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


# ==================== ذخیره وضعیت آخرین کلید و مدل موفق ====================


@dataclass
class PipelineExecutionState:
    """نگهداری وضعیت آخرین اجرای موفق برای هر پایپ‌لاین"""

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


# ==================== پایه مدیریت کلاینت‌های کلیدهای سه‌گانه ====================


class BaseKeyModelPipeline:
    """
    کلاس پایه مدیریت چرخش کلیدها [KEY_1, KEY_2, KEY_3] و اولویت مدل‌ها
    با مدیریت Try/Except اختصاصی برای خطای 429.
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
        if not models:
            raise ValueError(f"[{pipeline_name}] لیست مدل‌ها نمی‌تواند خالی باشد.")
        cleaned_keys = [k.strip() for k in api_keys if k and k.strip()]
        if not cleaned_keys:
            raise ValueError(f"[{pipeline_name}] حداقل یک کلید API معتبر الزامی است.")

        # تضمین وجود دقیقاً ۳ کلید در آرایه [KEY_1, KEY_2, KEY_3]
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
                if genai is None:
                    raise RuntimeError("کتابخانه google-genai نصب نشده است.")
                self._clients_cache[api_key] = genai.Client(api_key=api_key)
        return self._clients_cache[api_key]

    def _get_ordered_models(self, preferred_first_model: Optional[str] = None) -> List[str]:
        """
        تعیین ترتیب بررسی مدل‌ها:
        - به صورت پیش‌فرض (start_from_first_priority=True) همیشه از اولویت اول شروع می‌شود.
        - اگر start_from_first_priority=False باشد و مدل موفقی از قبل ذخیره شده باشد، از آن مدل شروع می‌کند.
        """
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
            try:
                self._state_callback(self.state)
            except Exception as cb_err:
                logger.debug(f"[{self.pipeline_name}] خطا در ذخیره وضعیت: {cb_err}")


# ==================== ۱. مرحله تبدیل ویس به متن (Audio-to-Text Pipeline) ====================


class AudioToTextPipeline(BaseKeyModelPipeline):
    """
    مرحله اول: تبدیل فایل صوتی (ویس) به متن (Speech-to-Text)
    ترتیب اولویت مدل‌ها:
    1. "gemini-3.5-transcribe" (تخصصی صوت)
    2. "gemini-3.5-flash-lite" (فوق‌العاده سریع با سهمیه بالا)
    3. "gemini-3.1-flash-lite" (پشتیبان نهایی)
    """

    def __init__(
        self,
        api_keys: Sequence[str],
        models: Sequence[str] = AUDIO_PIPELINE_MODELS,
        start_from_first_priority: bool = True,
        client_factory: Optional[Callable[[str], Any]] = None,
        state_callback: Optional[Callable[[PipelineExecutionState], None]] = None,
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
        نکته الزامی: برای مدل‌های سری Flash Lite، فایل صوتی حتماً با این پرامپت ارسال می‌شود:
        "متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن."
        """
        if cls.is_flash_lite_model(model_name):
            return FLASH_LITE_TRANSCRIPTION_PROMPT
        return DEFAULT_TRANSCRIPTION_PROMPT

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

        if types is not None and hasattr(types, "Part"):
            audio_part = types.Part.from_bytes(data=audio_bytes, mime_type=mime_type)
        else:
            audio_part = {" inline_data": {"data": audio_bytes, "mime_type": mime_type}}

        contents = [audio_part, prompt_text]

        config: Any = None
        if types is not None and hasattr(types, "GenerateContentConfig"):
            cfg_kwargs: Dict[str, Any] = {"temperature": temperature}
            # برای مدل‌های سری Flash Lite پرامپت دقیق مستقیم داخل contents ارسال می‌شود؛
            # اگر system_instruction هم داده شده باشد و مدل پشتیبانی کند اضافه می‌گردد
            if system_instruction and not self.is_flash_lite_model(model_name):
                cfg_kwargs["system_instruction"] = system_instruction
            config = types.GenerateContentConfig(**cfg_kwargs)

        response = await client.aio.models.generate_content(
            model=model_name,
            contents=contents,
            config=config,
        )
        text = getattr(response, "text", None)
        if text and str(text).strip():
            return str(text).strip()
        raise RuntimeError(f"پاسخ متنی خالی از مدل صوتی '{model_name}' دریافت شد.")

    async def transcribe(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        system_instruction: Optional[str] = None,
        temperature: float = 0.2,
    ) -> str:
        """
        اجرای مرحله اول (تبدیل ویس به متن) با ساختار Fallback و مدیریت خطای 429:
        - ابتدا روی مدل اول (gemini-3.5-transcribe) با KEY_1 ارسال می‌شود.
        - در صورت خطای 429، بلافاصله و بدون توقف با KEY_2 و سپس KEY_3 تکرار می‌شود.
        - اگر هر ۳ کلید روی مدل اول 429 دادند، فوراً به مدل دوم (gemini-3.5-flash-lite) سوئیچ کرده
          و چرخه کلیدها مجدداً از KEY_1 آغاز می‌شود.
        - در صورت اتمام سهمیه مدل دوم، به سراغ مدل سوم (gemini-3.1-flash-lite) از KEY_1 می‌رود.
        - خطاهای غیر از 429 (شبکه، فرمت فایل و غیره) وارد چرخه تعویض کلید نشده و فوراً لاگ و Raise می‌شوند.
        """
        if not audio_bytes:
            raise ValueError("فایل صوتی ورودی خالی است (0 bytes).")
        if not mime_type or "/" not in mime_type:
            raise ValueError(f"فرمت فایل صوتی (mime_type='{mime_type}') نامعتبر است.")

        ordered_models = self._get_ordered_models()
        total_attempts = 0
        last_429_error: Optional[BaseException] = None

        for model_idx, model_name in enumerate(ordered_models):
            # در شروع مدل اول (در حالت پیش‌فرض) و در زمان سوییچ به مدل بعدی، چرخه کلیدها همیشه از KEY_1 (ایندکس 0) آغاز می‌شود
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
                        f"✅ [{self.pipeline_name}] تبدیل ویس به متن با موفقیت انجام شد | "
                        f"مدل موفق: '{model_name}' | کلید موفق: '{key_label}'"
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
                                f"⚠️ [{self.pipeline_name}][429 Rate Limit] سهمیه کلید '{key_label}' روی مدل "
                                f"'{model_name}' پر شد -> سوییچ فوری و بدون توقف به کلید '{next_key_label}'..."
                            )
                            continue
                        else:
                            if (model_idx + 1) < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    f"🔄 [{self.pipeline_name}][Model Fallback] هر ۳ کلید روی مدل '{model_name}' "
                                    f"خطای 429 دادند -> سوییچ فوری به مدل اولویت بعدی '{next_model}' و ریست چرخه از KEY_1!"
                                )
                            else:
                                logger.error(
                                    f"🚨 [{self.pipeline_name}] تمام کلیدها روی آخرین مدل پشتیبان '{model_name}' نیز خطای 429 دادند!"
                                )
                            break
                    else:
                        # الزام فنی ۲: خطاهای غیر از 429 نباید وارد چرخه تعویض کلید شوند و باید لاگ و Raise شوند
                        logger.error(
                            f"❌ [{self.pipeline_name}][Non-429 Error] بروز خطای غیر 429 در مدل '{model_name}' "
                            f"با کلید '{key_label}': {exc} -> توقف چرخه Fallback و Raise مستقیم خطا."
                        )
                        raise

        raise AllModelsAndKeysExhaustedError(
            pipeline_name=self.pipeline_name,
            attempts=total_attempts,
            last_error=last_429_error,
        )


# ==================== ۲. مرحله تولید پاسخ متنی چت (Chat Pipeline) ====================


class ChatPipeline(BaseKeyModelPipeline):
    """
    مرحله دوم: تولید پاسخ متنی چت بر اساس متن دریافتی (Chat Response Generation)
    ترتیب اولویت مدل‌ها:
    1. "gemini-3.5-flash-lite" (پاسخ‌دهی آنی و کمترین تاخیر)
    2. "gemini-3.1-flash-lite" (جایگزین سریع با سهمیه بالا)
    3. "gemini-3.8-flash" (مدل هوشمندتر و قوی‌تر برای پاسخ‌های تحلیلی/طولانی)
    """

    def __init__(
        self,
        api_keys: Sequence[str],
        models: Sequence[str] = CHAT_PIPELINE_MODELS,
        start_from_first_priority: bool = True,
        client_factory: Optional[Callable[[str], Any]] = None,
        state_callback: Optional[Callable[[PipelineExecutionState], None]] = None,
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
        config: Any = None
        if types is not None and hasattr(types, "GenerateContentConfig"):
            cfg_kwargs: Dict[str, Any] = {"temperature": temperature}
            if system_instruction:
                cfg_kwargs["system_instruction"] = system_instruction
            config = types.GenerateContentConfig(**cfg_kwargs)

        response = await client.aio.models.generate_content(
            model=model_name,
            contents=input_text,
            config=config,
        )
        text = getattr(response, "text", None)
        if text and str(text).strip():
            return str(text).strip()
        raise RuntimeError(f"پاسخ متنی خالی از مدل چت '{model_name}' دریافت شد.")

    async def generate_response(
        self,
        input_text: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.65,
        preferred_first_model: Optional[str] = None,
    ) -> str:
        """
        اجرای مرحله دوم (تولید پاسخ متنی چت) با ساختار Fallback و مدیریت خطای 429:
        - متن ورودی ابتدا روی مدل اول چت (gemini-3.5-flash-lite) با KEY_1 فرستاده می‌شود.
        - با هر خطای 429، بدون فوت وقت کلید بعدی (KEY_2 و بعد KEY_3) تست می‌شود.
        - در صورت پر شدن سهمیه کلیدها، مدل به اولویت بعدی (gemini-3.1-flash-lite و سپس gemini-3.8-flash)
          تغییر یافته و چرخه کلیدها از KEY_1 ریست می‌شود.
        - خطاهای غیر از 429 فوراً لاگ و Raise می‌شوند.
        """
        if not input_text or not input_text.strip():
            raise ValueError("متن ورودی برای تولید پاسخ چت نمی‌تواند خالی باشد.")

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
                        f"✅ [{self.pipeline_name}] پاسخ چت با موفقیت تولید شد | "
                        f"مدل موفق: '{model_name}' | کلید موفق: '{key_label}'"
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
                                f"⚠️ [{self.pipeline_name}][429 Rate Limit] سهمیه کلید '{key_label}' روی مدل چت "
                                f"'{model_name}' پر شد -> سوییچ آنی به کلید '{next_key_label}'..."
                            )
                            continue
                        else:
                            if (model_idx + 1) < len(ordered_models):
                                next_model = ordered_models[model_idx + 1]
                                logger.warning(
                                    f"🔄 [{self.pipeline_name}][Model Fallback] هر ۳ کلید روی مدل چت '{model_name}' "
                                    f"خطای 429 دادند -> سوییچ فوری به مدل اولویت بعدی '{next_model}' و ریست چرخه کلیدها از KEY_1!"
                                )
                            else:
                                logger.error(
                                    f"🚨 [{self.pipeline_name}] تمام کلیدها روی آخرین مدل چت '{model_name}' نیز خطای 429 دادند!"
                                )
                            break
                    else:
                        logger.error(
                            f"❌ [{self.pipeline_name}][Non-429 Error] بروز خطای غیر 429 در مدل چت '{model_name}' "
                            f"با کلید '{key_label}': {exc} -> توقف چرخه تعویض کلید و Raise مستقیم خطا."
                        )
                        raise

        raise AllModelsAndKeysExhaustedError(
            pipeline_name=self.pipeline_name,
            attempts=total_attempts,
            last_error=last_429_error,
        )


# ==================== هماهنگ‌کننده پایپ‌لاین دو مرحله‌ای (Two-Stage Bot Pipeline) ====================


class TwoStageBotPipeline:
    """
    ماژول یکپارچه دو مرحله‌ای (Audio-to-Text + Chat Response Generation)
    که خروجی مرحله اول را به عنوان ورودی به مرحله دوم می‌دهد و وضعیت هر دو مرحله را مدیریت می‌کند.
    """

    def __init__(
        self,
        api_keys: Sequence[str],
        audio_models: Sequence[str] = AUDIO_PIPELINE_MODELS,
        chat_models: Sequence[str] = CHAT_PIPELINE_MODELS,
        start_from_first_priority: bool = True,
        client_factory: Optional[Callable[[str], Any]] = None,
        state_callback: Optional[Callable[[PipelineExecutionState], None]] = None,
    ) -> None:
        self.audio_pipeline = AudioToTextPipeline(
            api_keys=api_keys,
            models=audio_models,
            start_from_first_priority=start_from_first_priority,
            client_factory=client_factory,
            state_callback=state_callback,
        )
        self.chat_pipeline = ChatPipeline(
            api_keys=api_keys,
            models=chat_models,
            start_from_first_priority=start_from_first_priority,
            client_factory=client_factory,
            state_callback=state_callback,
        )

    async def transcribe_audio(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        system_instruction: Optional[str] = None,
        temperature: float = 0.2,
    ) -> str:
        """وظیفه اول: تبدیل فایل صوتی (ویس) به متن"""
        return await self.audio_pipeline.transcribe(
            audio_bytes=audio_bytes,
            mime_type=mime_type,
            system_instruction=system_instruction,
            temperature=temperature,
        )

    async def generate_chat_response(
        self,
        text: str,
        system_instruction: Optional[str] = None,
        temperature: float = 0.65,
        preferred_first_model: Optional[str] = None,
    ) -> str:
        """وظیفه دوم: تولید پاسخ متنی چت بر اساس متن دریافتی"""
        return await self.chat_pipeline.generate_response(
            input_text=text,
            system_instruction=system_instruction,
            temperature=temperature,
            preferred_first_model=preferred_first_model,
        )

    async def process_voice_to_chat(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/ogg",
        audio_system_instruction: Optional[str] = None,
        chat_system_instruction: Optional[str] = None,
        chat_temperature: float = 0.65,
    ) -> Tuple[str, str]:
        """
        اجرای کامل پایپ‌لاین دو مرحله‌ای:
        ۱. استخراج متن از ویس با AudioToTextPipeline
        ۲. ارسال متن استخراج‌شده به ChatPipeline جهت تولید پاسخ
        خروجی: (transcript, chat_reply)
        """
        transcript = await self.transcribe_audio(
            audio_bytes=audio_bytes,
            mime_type=mime_type,
            system_instruction=audio_system_instruction,
        )
        chat_reply = await self.generate_chat_response(
            text=transcript,
            system_instruction=chat_system_instruction,
            temperature=chat_temperature,
        )
        return transcript, chat_reply

    def get_status_summary(self) -> Dict[str, Any]:
        return {
            "audio_pipeline": self.audio_pipeline.state.to_dict(),
            "chat_pipeline": self.chat_pipeline.state.to_dict(),
        }
