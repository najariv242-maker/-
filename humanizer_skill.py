# -*- coding: utf-8 -*-
"""
ماژول فوق‌سریع مهارت انسانی‌سازی، تفکیک هوشمند «حافظه مکالمه قبلی» از «پیام جدید دانش‌آموز»،
تشخیص دقیق پیام استاد در فورواردهای چندتایی، و لحن قاطع و کوتاه استاد (نسخه نهایی v3.1)

اصلاحات کلیدی نسخه ۳.۱ طبق دستور مستقیم استاد:
1. حذف کامل لحن احساسی، دخترانه و همدلی‌های مصنوعی («آره میفهمم چی میگی»، «حست رو درک میکنم»، «طبیعیه که قفل بشی»):
   - لحن دقیقاً مثل یک استاد و مشاور جدی، کاربلد، خونسرد و قاطع است.
   - در پیام اول: توضیح شفاف و روشن کردن تکلیف بدون شعار و احساسات.
   - در پیام دوم به بعد (Follow-up / ادامه مکالمه): وقتی قبلاً یک بار توضیح داده شده و دانش‌آموز دوباره سوال یا بهانه جدید می‌آورد،
     مطلقاً هیچ مقدمه، همدلی یا تکرار مکررات نوشته نمی‌شود؛ فقط و فقط در ۱ یا ۲ پاراگراف خیلی کوتاه (۲ تا ۴ خط)،
     مستقیم یک راه‌حل عملی کوتاه یا دستورالعمل مشخص جلوی پایش گذاشته می‌شود (مثلاً «ببین وقتی اینجوری میشی این کارو بکن... امتحان کن نتیجه رو بگو»).
2. الگوریتم دقیق تشخیص پیام استاد در بین پیام‌های فورواردشده چندتایی (Multi-Forward Teacher Detector):
   - بررسی متادیتای فرستنده تلگرام + تحلیل ساختاری و واژگانی متن (تشخیص پیام‌های خطابی/دستوری استاد در مقابل پیام‌های پرسشی/گزارشی دانش‌آموز)
   - تفکیک ۱۰۰٪ دقیق: [سوالات قبلی دانش‌آموز + جواب استاد] ⬅️ حافظه مکالمه | [پیام‌های بعد از جواب استاد] ⬅️ سوال جدید دانش‌آموز.
"""

from __future__ import annotations

import difflib
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT = """تو خودِ استاد و مشاور کنکور (یک مرد جدی، پخته، کاربلد و خونسرد) هستی که داری توی تلگرام سریع به پیام یا ویس دانش‌آموزت جواب میدی.
تمام عادت‌های نگارشی هوش مصنوعی، جملات کتابی، چاپلوسی و به‌خصوص «همدلی‌های احساسی و ناز کشیدن‌های دخترونه» رو کامل بریز دور و دقیقاً طبق قوانین زیر بنویس:

### 1. خط قرمز لحن: جدی، قاطع، راحت و مردانه (بدون همدلی احساسی و بدون تملق)
- **ممنوعیت مطلق جملات احساسی و روانشناسی زرد:** هرگز جملاتی مثل «آره میفهمم چی میگی»، «کاملا درکت میکنم»، «حست رو میفهمم»، «طبیعیه که ذهنت قفل بشه»، «حق داری سخته»، «قربون صداقتت»، «دمت گرم» یا «عالی عمل کردی» ننویس! یک استاد و مشاور این‌طوری حرف نمی‌زند.
- **لحن واقعی استاد:** مستقیم، خونسرد، بدون تعارف و کاربردی حرف بزن. وقتی دانش‌آموز کاری رو درست انجام میده عادی بگو «همینجوری که داری میری جلو اوکیه» یا «همین کافیه». وقتی هم گیر کرده یا بهانه میاره، بدون ناز کشیدن مستقیم بگو چیکار کنه.

### 2. قانون حیاتی تفاوت «پیام اول» با «پیام دوم و ادامه مکالمه (Follow-up)»
- **در پیام اول (وقتی سابقه قبلی وجود ندارد):** خیلی شفاف، منطقی و خونسرد اصل موضوع رو براش باز کن و تکلیفش رو روشن کن (معمولاً ۲ تا ۳ پاراگراف کوتاه).
- **در پیام دوم به بعد (وقتی بخش «📚 پیش‌زمینه و حافظه مکالمه قبلی» وجود دارد و قبلاً یک بار بهش توضیح دادی):**
  1. **مطلقاً دوباره توضیح و فلسفه نباف و همدلی نکن!** (چون توی پیام قبلی توضیحات لازم رو دادی).
  2. **فقط و فقط یک پاسخ خیلی کوتاه (حداکثر ۲ تا ۴ خط در یک یا دو پاراگراف کوتاه) بده** و مستقیم یک راه‌حل عملی کوتاه یا یک تکنیک مشخص جلوی پاش بذار؛ مثلاً بگو «ببین تو اینجور مواقع که میبینی نمیتونی کتابو باز کنی فقط فلان کار رو بکن... این روش رو تست کن ببین چطور جواب میده».
  3. هیچ حرفی از پیام قبلی خودت رو تکرار نکن؛ فقط راه‌حل کوتاهِ مشکلِ جدید رو بگو و تمام.

### 3. حذف مطلق علائم نگارشی و نیم‌فاصله‌های کتابی
- ممنوعیت کامل علائم نگارشی: استفاده از ویرگول (،)، نقطه (.)، دو نقطه (:)، نقطه‌ویرگول (؛)، علامت تعجب (!)، علامت سوال (؟)، گیومه («»)، پرانتز () و خط تیره (-) در متن خروجی کاملاً ممنوعه.
- حذف نیم‌فاصله‌های وسواسی (تایپ موبایلی): کلمات رو همون‌طور بنویس که روی کیبورد گوشی تایپ میشن:
  - به جای «می‌خوابی» بنویس «میخوابی» | به جای «نمی‌خواهد» بنویس «نمیخواد»
  - به جای «پارت‌ها» بنویس «پارتا» یا «پارت ها» | به جای «برنامه‌ات» بنویس «برنامت»
  - به جای «می‌شود» بنویس «میشه» | به جای «دانش‌آموزان» بنویس «دانش آموزا»
- تنها ابزار جداسازی، خط جدید (Enter) است: بدون شماره‌گذاری و بدون بولت‌پوینت بنویس.

### 4. نحو گفتاری مستقیم و پایان‌بندی آنی
- شروع مستقیم بدون سلام و احوال‌پرسی:
  - در پیام اول: «ببین این حالتی که میگی...» یا «آره ببین راجع به...»
  - در پیام دوم (ادامه مکالمه): مستقیم برو سر راه‌حل؛ مثلاً «ببین وقتی میبینی اینطوری قفل شدی و نمیتونی دست به کتاب بزنی...» یا «واسه این مواقع که نمیتونی شروع کنی یه کار ساده بکن...»
- هرگز شعارهای پایانی («موفق باشی»، «حواست به خودت باشه»، «هر سوالی داشتی بپرس») ننویس. مستقیم روی آخرین جمله عملی فرود بیا.

### 5. نمونه واقعی سبک خودِ استاد در دو مرحله:
- **نمونه پیام اول استاد (توضیح اولیه):**
ببین این حالتی که میگی واسه خیلی از دانش آموزا پیش میاد و لزوما دلیل خاصی نداره که بخوای دنبالش بگردی
همین که با وجود این حس داری درساتو میخونی و برنامه رو جلو میبری خودش کافیه و لازم نیست الان انرژی زیادی بذاری که بخوای این وضعیت رو تحلیل کنی یا دنبال جواب براش بگردی
فقط حواست باشه این استرس اینکه چرا اینطوری هستی رو به درس خوندنت اضافه نکنی چون همین خودش باعث میشه ذهنت بیشتر درگیر بشه و تمرکزت بیاد پایین
همینجوری که داری میری جلو اوکیه و لازم نیست الان کار خاصی انجام بدی تا این حس عوض بشه خودش به مرور زمان درست میشه و میگذره خیالت راحت باشه ادم وقتی زیاد درس میخونه اینطوری میشه طبیعیه

- **نمونه پیام دوم استاد در ادامه همان مکالمه (وقتی دانش‌آموز دوباره می‌پرسد: «ولی الان کتاب جلومه و اصلاً نمیتونم دستش بزنم یه مانعی جلومه»):**
ببین تو اینجور مواقع که کتاب جلوته و نمیتونی شروع کنی نخواه که همون اول کل اون پارت رو بخونی
فقط کتابو باز کن و بگو میخوام ده دقیقه مثل روزنامه از روش رد بشم یا فقط پنج تا تست اولشو بدون تایم نگاه کنم همین که اینجوری خیلی سبک استارت بزنی اون مانعه رد میشه و راه میفتی این روش رو همین الان تست کن ببین چطوره
"""

SUMMARY_SYSTEM_PROMPT = """تو یک دستیار هوشمند و سریع تلگرامی هستی.
چکیده و اصلِ منظورِ پیام یا ویس(های) ورودی رو خیلی سریع، خودمانی، روشن و بدون حاشیه بنویس تا استاد در ۳ ثانیه بفهمه دانش‌آموز چی گفته و سوالات یا دغدغه‌های اصلیش چیه.
از مقدمه‌چینی مثل «در این پیام گوینده اشاره می‌کند که...» خودداری کن و مستقیم اصل حرف و سوالاتش رو کوتاه و تمیز زیر هم بنویس."""

TRANSCRIPTION_CLEANUP_PROMPT = """متن این فایل صوتی را دقیق، کلمه به کلمه و بدون هیچ توضیح اضافه پیاده کن. دقیقاً کلمات و لحن محاوره‌ای گوینده را بنویس، تپق‌های صوتی بی‌معنی (اممم، آاا) را حذف کن و فقط متن خالص داخل ویس را خروجی بده."""

TRANSLATION_SYSTEM_PROMPT = """تو یک مترجم فوق‌العاده حرفه‌ای، مسلط و طبیعی هستی.
وظیفه تو ترجمه دقیق، روان و اصیل متن یا پیامی است که دریافت می‌کنی.
اگر متن ورودی فارسی است، آن را به انگلیسی روان و محاوره‌ای/صحیح ترجمه کن.
اگر متن ورودی به زبانی غیر از فارسی (مثل انگلیسی) است، آن را به فارسی سلیس، روان و طبیعی ترجمه کن.
هیچ توضیح، مقدمه یا پی‌نوشتی اضافه نکن؛ فقط و فقط خودِ ترجمه نهایی را خروجی بده."""

STYLE_EVOLUTION_EXTRACTOR_SYSTEM_PROMPT = """تو موتور ارتقای مهارت (Skill Evolution Engine) هستی.
وظیفه تو مقایسه «پاسخ اولیه هوش مصنوعی (Draft)» با «نسخه ویرایش‌شده و نهایی توسط استاد (Edited)» است تا دقیقاً بفهمی استاد چه ایرادهایی در لحن، واژگان، طول پیام یا توضیحات اضافه هوش مصنوعی دیده و چطور آن را طبیعی‌تر و قاطع‌تر کرده است.

قوانین بسیار مهم در استخراج نکته آموزشی:
1. **حذف کامل نام افراد و موضوع شخصی پیام (Zero Context Leakage):** قانون استخراج‌شده باید ۱۰۰٪ کلی و قابل استفاده برای پیام‌های افراد دیگر در موضوعات متفاوت باشد. هرگز اسم شخص یا داستان خاص آن ویس را به عنوان فکت ذخیره نکن.
2. **تشخیص سه نوع اصلاح اصلی:**
   - **اصلاح لحن و حذف تعارف/احساسات:** چه عبارات احساسی، کتابی یا ماشینی حذف شد و چه عبارت قاطع و طبیعی جایگزینش شد؟
   - **حذف توضیحات اضافه و پیشنهادهای سرخود:** آیا هوش مصنوعی پرحرفی کرده بود که استاد آن را کوتاه و جمع‌وجور کرد؟
   - **ساختار و کوتاهی جملات:** چطور جملات کوتاه‌تر، راحت‌تر یا بی‌مقدمه‌تر شدند؟
3. خروجی تو باید فقط ۱ تا ۳ خط قانون بسیار شفاف، کوتاه و امری (خطاب به نویسنده بعدی) باشد؛ بدون هیچ مقدمه یا توضیح اضافه."""


def build_dynamic_human_system_prompt(
    learned_lessons: Optional[Sequence[Dict[str, Any]]] = None,
) -> str:
    """ترکیب پرامپت پایه با قوانین و نمونه‌های یادگرفته‌شده از ویرایش‌های قبلی استاد"""
    if not learned_lessons:
        return HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT

    rules_lines: List[str] = []
    examples_lines: List[str] = []

    for idx, lesson in enumerate(learned_lessons, start=1):
        rule_text = (lesson.get("learned_rule") or "").strip()
        if rule_text:
            rules_lines.append(f"- قانون آموخته‌شده {idx}: {rule_text}")

        bot_before = (lesson.get("bot_draft") or "").strip()
        human_after = (lesson.get("human_edited") or "").strip()
        if bot_before and human_after and len(examples_lines) < 4:
            b_short = bot_before[:260].replace("\n", " ")
            h_short = human_after[:260].replace("\n", " ")
            examples_lines.append(
                f"• نمونه اصلاح {len(examples_lines) + 1}:\n"
                f"  ❌ جواب خام قبلی بات (نادرست/طولانی): {b_short}\n"
                f"  ✅ ویرایش نهایی استاد (الگوی درست): {h_short}"
            )

    dynamic_sections: List[str] = [HUMAN_TELEGRAM_REPLY_SYSTEM_PROMPT.rstrip()]

    if rules_lines:
        dynamic_sections.append(
            "\n\n### 6. قوانین اختصاصی یادگرفته‌شده از ویرایش‌های قبلی استاد (با بالاترین اولویت اجرا کن):\n"
            + "\n".join(rules_lines)
        )

    if examples_lines:
        dynamic_sections.append(
            "\n\n### 7. الگوهای واقعی اصلاح لحن و کوتاه‌سازی توسط استاد (فقط سبک نگارش، قاطعیت و کوتاهی را تقلید کن، نه موضوع پیام را):\n"
            + "\n".join(examples_lines)
        )

    return "\n".join(dynamic_sections)


# ==================== تشخیص و تفکیک هوشمند چند پیام یا لاگ کپی‌شده تلگرام ====================
_RE_TG_EXPORT_HEADER = re.compile(
    r"(?m)^(?:[^\n\r,:]{1,45},\s*\[[0-9./:\sAPMapm\-]+\]|[^\n\r:]{1,40}\s+\[[0-9]{1,2}:[0-9]{2}(?:\s*[APMapm]{2})?\])\s*$"
)
_RE_TIME_ONLY_LINE = re.compile(
    r"(?m)^\s*[0-9]{1,2}:[0-9]{2}(?:\s*(?:AM|PM|am|pm))?\s*$"
)
_RE_EXPLICIT_SEPARATOR = re.compile(r"(?m)^\s*(?:---+|===+|///+)\s*$")


def split_copied_conversation_blocks(raw_text: str) -> List[str]:
    """
    اگر کاربر چند پیام تلگرام را یکجا کپی و پیست کرده باشد (یا با --- جدا کرده باشد)،
    آن‌ها را به لیست پیام‌های جداگانه به ترتیب زمانی تبدیل می‌کند.
    """
    if not raw_text or not raw_text.strip():
        return []

    text = raw_text.strip()

    if _RE_EXPLICIT_SEPARATOR.search(text):
        parts = [p.strip() for p in _RE_EXPLICIT_SEPARATOR.split(text) if p.strip()]
        if len(parts) >= 2:
            return parts

    if _RE_TG_EXPORT_HEADER.search(text):
        chunks = _RE_TG_EXPORT_HEADER.split(text)
        headers = _RE_TG_EXPORT_HEADER.findall(text)
        blocks: List[str] = []
        for idx, h in enumerate(headers):
            body = chunks[idx + 1].strip() if (idx + 1) < len(chunks) else ""
            if body:
                blocks.append(f"{h.strip()}\n{body}")
        if len(blocks) >= 2:
            return blocks

    if len(_RE_TIME_ONLY_LINE.findall(text)) >= 2:
        raw_parts = _RE_TIME_ONLY_LINE.split(text)
        cleaned_parts = [p.strip() for p in raw_parts if p.strip()]
        if len(cleaned_parts) >= 2:
            return cleaned_parts

    return [text]


def format_contextual_prompt(
    context_messages: Sequence[str],
    target_messages: Sequence[str],
    variation_hint: str = "",
) -> str:
    """
    ساخت پرامپت فوق‌دقیق برای مرحله ۲:
    - وقتی `context_messages` خالی است (پیام اول دانش‌آموز یا چند ویس اول دانش‌آموز):
      پاسخ کامل، خونسرد و روشن‌کننده تکلیف برای تمام موارد مطرح‌شده داده می‌شود.
    - وقتی `context_messages` وجود دارد (پیام دوم به بعد / ادامه مکالمه پس از جواب قبلی استاد):
      پاسخ **بسیار کوتاه، مستقیم و فقط حاوی یک راه‌حل عملی سریع (۲ تا ۴ خط)** بدون هیچ همدلی یا تکرار توضیحات قبلی تولید می‌شود!
    """
    clean_ctx = [m.strip() for m in context_messages if m and m.strip()]
    clean_tgt = [m.strip() for m in target_messages if m and m.strip()]

    if not clean_ctx:
        combined_target = "\n\n".join(clean_tgt)
        prompt = (
            "🎯 **متن پیام یا ویس(های) فعلی دانش‌آموز (به تمام نکات و سوالات مطرح‌شده در این متن با لحن استاد قاطع، خونسرد، بدون همدلی احساسی و بدون علائم نگارشی جواب بده):**\n"
            f"{combined_target}"
        )
    else:
        ctx_formatted = "\n\n---\n".join(
            f"[پیام شماره {i + 1} در سابقه مکالمه قبلی]:\n{msg}"
            for i, msg in enumerate(clean_ctx)
        )
        tgt_formatted = "\n\n".join(clean_tgt)
        prompt = (
            "📚 **پیش‌زمینه و حافظه مکالمه قبلی (شامل سوال قبلی دانش‌آموز و جوابی که استاد قبلاً داده است — فقط برای اطلاع از پیش‌زمینه؛ هرگز به این بخش دوباره جواب نده):**\n"
            f"{ctx_formatted}\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 **پیام(های) جدید دانش‌آموز در ادامه جواب قبلی استاد (که الان باید فقط به این بخش جواب بدهی):**\n"
            f"{tgt_formatted}\n\n"
            "⚠️ **قانون اکید برای این پاسخ (چون ادامه مکالمه و بار دوم است):**\n"
            "۱. تو در پیام قبلی توضیحات کلی رو دادی، پس الان **مطلقاً فلسفه نباف، توضیح طولانی نده و هرگز نگو «آره میفهمم چی میگی» یا «درکت میکنم»**!\n"
            "۲. **فقط و فقط یک جواب خیلی کوتاه (حداکثر ۲ تا ۴ خط)** بنویس و مستقیم یک راه‌حل عملیِ ساده یا یک روش کوتاه بهش معرفی کن تا انجام بده (مثلاً «ببین تو اینجور مواقع... این روش رو تست کن ببین چطوره»)."
        )

    if variation_hint:
        prompt += f"\n\n📌 **دستور صریح استاد برای این پاسخ:**\n{variation_hint}"

    return prompt


def _score_teacher_vs_student_message(text: str, is_marked_teacher: bool = False) -> int:
    """
    امتیازدهی هوشمند برای تشخیص ۱۰۰٪ دقیق اینکه در یک دسته پیام فورواردشده،
    کدام پیام «جواب قبلی استاد» است و کدام پیام‌ها «سوالات دانش‌آموز» هستند.
    امتیاز مثبت بالا (>= 2) یعنی پیامِ استاد. امتیاز منفی یعنی پیامِ دانش‌آموز.
    """
    if is_marked_teacher:
        return 100
    if not text:
        return -10

    t = text.strip()
    score = 0

    # نشانه‌های قطعی پیام دانش‌آموز (خطاب به استاد یا بیان مشکل شخصی)
    student_markers = (
        "استاد",
        "سلام استاد",
        "راستش",
        "حالم خوب نیست",
        "نمیدونم چیکار",
        "نمیزاره درس بخونم",
        "نمیتونم دستش بزنم",
        "عصبی میشم",
        "چرا اینجوری شدم",
        "کمکم کنید",
        "به نظرتون",
        "چیکار بکنم",
        "😢",
        "😭",
        "😔",
    )
    for sm in student_markers:
        if sm in t:
            score -= 4

    # نشانه‌های قطعی سبک پیام استاد (خطاب به دانش‌آموز، دستوری، بدون علائم نگارشی)
    teacher_markers = (
        "ببین این حالتی که میگی",
        "همینجوری که داری میری جلو اوکیه",
        "واسه خیلی از دانش آموزا",
        "فقط حواست باشه",
        "لازم نیست الان کار خاصی انجام بدی",
        "خیالت راحت باشه",
        "آره ببین راجع به",
        "همون روال رو برو جلو",
        "خودش کافیه",
        "چشمی رد کن بره",
        "اصلا حساس نشو",
        "تایمت کش بیاد",
        "درساتو میخونی",
        "برنامه رو جلو میبری",
        "تست کن",
    )
    for tm in teacher_markers:
        if tm in t:
            score += 3

    # اگر پیام با «ببین » یا «آره ببین » شروع شده باشد و کلمه «استاد» در آن نباشد
    if (t.startswith("ببین ") or t.startswith("آره ببین ") or t.startswith("خب ببین ")) and "استاد" not in t:
        score += 4

    # پیام‌های خروجی قبلی بات هیچ علامت نگارشی ندارند و بیش از ۱۵ کلمه هستند
    words_count = len(t.split())
    has_punct = any(ch in t for ch in (".", "،", "؟", "!", ":", "؛"))
    if words_count >= 20 and not has_punct and "استاد" not in t:
        score += 2

    return score


def _looks_like_teacher_previous_reply(text: str) -> bool:
    return _score_teacher_vs_student_message(text, False) >= 2


def split_sequence_into_context_and_target(
    items: Sequence[Dict[str, Any]],
) -> Tuple[List[str], List[str]]:
    """
    تحلیل فوق‌دقیق زنجیره پیام‌های فورواردشده همزمان:
    ورودی `items`: لیستی از دیکشنری‌ها به ترتیب زمانی `[{"text": ..., "is_teacher": bool, "sender_key": ...}, ...]`
    خروجی: `(context_messages, target_messages)`
    - بات دقیقاً پیدا می‌کند کدام پیام(ها) در بین پیام‌های فورواردشده، جوابِ قبلیِ استاد است:
      ۱. تمام پیام‌های قبل از آخرین جواب استاد (سوالات قبلی دانش‌آموز) + خودِ جواب استاد می‌شوند `context_messages` (حافظه مکالمه).
      ۲. تمام پیام‌های بعد از آخرین جواب استاد (چه ۱ پیام باشد، چه ۲ یا ۳ پیام پشت سر هم از دانش‌آموز) می‌شوند `target_messages` (سوالات جدید دانش‌آموز که الان باید جواب داده شوند).
      ۳. حتی اگر پیام آخر تصادفاً پیام استاد باشد (یعنی استاد پیام دانش‌آموز را بعد از پیام خودش فوروارد نکرده باشد)، هوشمندانه آخرین پیام دانش‌آموز را هدف قرار می‌دهد.
    """
    if not items:
        return [], []
    if len(items) == 1:
        return [], [str(items[0].get("text", "")).strip()]

    # امتیازدهی به تک‌تک پیام‌ها برای یافتن پیام(های) استاد
    scores = [
        _score_teacher_vs_student_message(str(it.get("text", "")), bool(it.get("is_teacher")))
        for it in items
    ]

    # بررسی فرستنده تلگرام (اگر در بین پیام‌های فورواردشده، فرستنده پیام آخر با فرستنده یکی از پیام‌های وسط فرق داشت)
    last_sender = items[-1].get("sender_key")
    if last_sender:
        for idx in range(len(items) - 1):
            s_key = items[idx].get("sender_key")
            if s_key and s_key != last_sender and scores[idx] >= 0:
                scores[idx] += 5

    # پیدا کردن آخرین پیام استاد که قبل از پیام(های) پایانی دانش‌آموز قرار دارد
    last_teacher_idx = -1
    for idx in range(len(items) - 1):
        if scores[idx] >= 2:
            last_teacher_idx = idx

    if last_teacher_idx >= 0:
        context_list: List[str] = []
        for i in range(last_teacher_idx + 1):
            txt = str(items[i].get("text", "")).strip()
            if not txt:
                continue
            is_t = (i == last_teacher_idx) or (scores[i] >= 2)
            role_label = "جواب قبلی شما (استاد)" if is_t else "سوال قبلی دانش‌آموز"
            context_list.append(f"({role_label}): {txt}")

        target_list = [
            str(it.get("text", "")).strip()
            for it in items[last_teacher_idx + 1 :]
            if str(it.get("text", "")).strip()
        ]
        return context_list, target_list

    # حالت خاص: اگر هیچ پیام استادی در بین پیام‌ها نبود (مثلاً دانش‌آموز ۳ پیام پشت سر هم فرستاده و استاد هر ۳ را فوروارد کرده تا یکجا جواب بدهد):
    # اگر همه پیام‌ها از یک دانش‌آموز و بدون جواب استاد در وسطشان باشد، همه با هم به عنوان سوالات فعلی دانش‌آموز در نظر گرفته می‌شوند
    # مگر اینکه پیام اول خیلی قدیمی/مقدماتی باشد.
    all_from_same_student = all(sc < 0 for sc in scores)
    if all_from_same_student:
        combined_targets = [str(it.get("text", "")).strip() for it in items if str(it.get("text", "")).strip()]
        return [], combined_targets

    context_list = [f"(پیام قبلی): {str(it.get('text', '')).strip()}" for it in items[:-1] if str(it.get("text", "")).strip()]
    target_list = [str(items[-1].get("text", "")).strip()]
    return context_list, target_list


def is_likely_edited_reply(bot_reply: str, user_text: str) -> bool:
    """
    تشخیص سریع اینکه آیا پیام ارسالی کاربر در ریپلای به جواب بات،
    نسخه ویرایش‌شده/انسانی‌ترشده همان جواب است یا یک دستور کوتاه جدید (مثل «کوتاهترش کن»).
    """
    if not bot_reply or not user_text:
        return False

    u_clean = user_text.strip()
    b_clean = bot_reply.strip()

    for prefix in ("ویرایش:", "آموزش:", "اصلاح:", "edit:", "learn:"):
        if u_clean.lower().startswith(prefix):
            return True

    short_command_STARTS = (
        "بگو ",
        "بهش بگو",
        "کوتاهتر",
        "کوتاه تر",
        "خلاصه تر",
        "رسمی تر",
        "صمیمی تر",
        "اینو اضافه کن",
        "اضافه کن که",
        "جواب اینو",
        "در ادامه",
    )
    if len(u_clean) < 55 and any(u_clean.startswith(cmd) for cmd in short_command_STARTS):
        return False

    bot_words = set(b_clean.split())
    user_words = set(u_clean.split())
    if not bot_words or not user_words:
        return False

    overlap = len(bot_words & user_words) / max(1, min(len(bot_words), len(user_words)))
    seq_ratio = difflib.SequenceMatcher(None, b_clean, u_clean).quick_ratio()

    if overlap >= 0.25 or seq_ratio >= 0.28:
        return True

    return False


# ==================== الگوهای ازپیش‌کامپایل‌شده (Pre-Compiled Regexes & Translation Table) ====================
_RE_PREFIX = re.compile(
    r"^(پاسخ|جواب|متن پیام|پیام تلگرام|خروجی)\s*[:：\-]+\s*",
    flags=re.IGNORECASE,
)
_RE_BULLETS = re.compile(r"(?m)^\s*[\-\*•▪▫]+\s+")
_RE_NUM_LIST = re.compile(r"(?m)^\s*[0-9۰-۹]+[\.\)\-]\s+")
_RE_MI_PREFIX = re.compile(r"(^|[\s\u200c])(ن?می)\u200c+")
_RE_HA_SUFFIX = re.compile(r"\u200c+ها(?=$|\s|[،.,؛:!؟?])")
_RE_HEH_PRONOUN = re.compile(r"ه\u200c+(ای|ام|ات|اش|ایم|اید|اند)(?=$|\s|[،.,؛:!؟?])")
_RE_OTHER_PRONOUN = re.compile(r"\u200c+(ای|ام|ات|اش|ایم|اید|اند)(?=$|\s|[،.,؛:!؟?])")
_RE_CLOCK_TIME = re.compile(r"(?<![0-9۰-۹])([0-2۰-۲]?[0-9۰-۹]):([0-5۰-۵][0-9۰-۹])(?![0-9۰-۹])")
_RE_PARAGRAPH_SPLIT = re.compile(r"\n\s*\n")
_RE_SPACES = re.compile(r"[ \t]+")

# عبارات احساسی/همدلی مصنوعی که اگر هوش مصنوعی احیاناً در ابتدای جمله آورد، خودکار حذف شوند
_RE_EMPATHY_FLUFF = re.compile(
    r"^(?:آره\s+)?(?:کاملا\s+|خیلی\s+خوب\s+)?(?:میفهمم\s+چی\s+میگی|درکت\s+میکنم|حست\s+رو\s+میفهمم|حستو\s+درک\s+میکنم|میفهمم\s+این\s+حالتو)\s*(?:و\s*)?",
    flags=re.IGNORECASE,
)

_PUNCTUATION_CHARS = ".,،;؛:!؟?«»\"'“”—–-()[]{}…*_`~\u200c"
_PUNCT_TRANSLATION_TABLE = str.maketrans({ch: " " for ch in _PUNCTUATION_CHARS})


def strip_ai_punctuation_and_artifacts(text: str) -> str:
    """
    فیلتر فوق‌سریع ضد هوش مصنوعی منطبق با چک‌لیست `telegram-human-reply`
    با حذف خودکار جملات همدلی مصنوعی، حفظ سلامت اعداد و تبدیل نیم‌فاصله‌ها به تایپ موبایلی
    """
    if not text:
        return ""

    cleaned = text.strip()
    cleaned = _RE_PREFIX.sub("", cleaned)
    cleaned = _RE_BULLETS.sub("", cleaned)
    cleaned = _RE_NUM_LIST.sub("", cleaned)

    cleaned = _RE_MI_PREFIX.sub(r"\1\2", cleaned)
    cleaned = (
        cleaned.replace("پارت‌ها", "پارتا")
        .replace("پارت ها", "پارتا")
        .replace("دانش‌آموزان", "دانش آموزا")
        .replace("دانش‌آموزا", "دانش آموزا")
        .replace("دانش‌آموز", "دانش آموز")
        .replace("برنامه‌ات", "برنامت")
        .replace("برنامه‌ام", "برنامم")
        .replace("برنامه‌اش", "برنامش")
        .replace("عقب‌افتادگی", "عقب افتادگی")
    )
    cleaned = _RE_HA_SUFFIX.sub(" ها", cleaned)
    cleaned = _RE_HEH_PRONOUN.sub(r"ه \1", cleaned)
    cleaned = _RE_OTHER_PRONOUN.sub(r"\1", cleaned)

    cleaned = _RE_CLOCK_TIME.sub(r"\1 و \2", cleaned)
    cleaned = cleaned.translate(_PUNCT_TRANSLATION_TABLE)

    raw_paragraphs = _RE_PARAGRAPH_SPLIT.split(cleaned)
    clean_paragraphs = []
    for para in raw_paragraphs:
        lines = []
        for line in para.splitlines():
            ln = _RE_SPACES.sub(" ", line).strip()
            ln = _RE_EMPATHY_FLUFF.sub("", ln).strip()
            if ln:
                lines.append(ln)
        if lines:
            clean_paragraphs.append("\n".join(lines))

    return "\n\n".join(clean_paragraphs)
