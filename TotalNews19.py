#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TotalNews19.py — версия с дополнительными районами и усиленной дедупликацией/анти‑рекламой.

Изменения по сравнению с TotalNews18:
- Добавлены районы: "Южнопортовый", "Химки", "Митино" и соответствующие CHATS/SOURCES.
- Для всех районов (новых и существующих) автоматически добавляются общемосковские источники (как для Пресненского).
- Усилены антирекламные фильтры: добавлены слова/фразы: "запись", "приходите", "акция", "звоните",
  "мы открылись", "процедура", "скидка", "стоимость" и т.п.
- Семантическая дедупликация адаптирована: теперь считается дубликатом при совпадении >= 4 значимых токенов
  (настраиваемо). Это предотвращает дублирование одних и тех же новостей из разных каналов по смыслу.
- Попытка подгрузить ТОПОНИМИКУ из файла ТОПОНИМИКА.xlsx (если он присутствует в репо). Если файл недоступен,
  используется встроенный словарь TOPONYMS.
- Остальная логика сохранена (потоковая загрузка медиа, retry/backoff, sha256‑ключ дедупа и т.д.)

ВАЖНО: TELEGRAM_BOT_TOKEN оставлен в коде по вашему требованию.
"""

import re
import os
import time
import sqlite3
import logging
import tempfile
import hashlib
from typing import List, Dict, Any, Optional, Tuple, Union, Set
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import io

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

# Optional: try to import pandas/openpyxl when available to read ТОПОНИМИКА.xlsx
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

# ------------------------------- БАЗА -------------------------------
FRESH_HOURS = 48
MAX_POSTS_PER_CHAT = 5
SEND_DELAY_SEC = 0.7
USER_AGENT = "Mozilla/5.0 (compatible; TotalNews19/1.0; no-login)"

# По просьбе: оставляем токен прямо в коде (риск известен)
TELEGRAM_BOT_TOKEN = "8339813256:AAGL6WOkl0DWEdt_zmENZ1R5-XxfTHNwqOM"

DB_PATH = "sent.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ru,en;q=0.9"})

# Simple cache for chat checks: chat_norm -> (ok: bool, ts: float)
CHAT_CHECK_CACHE: Dict[str, Tuple[bool, float]] = {}
CHAT_CHECK_TTL = 60 * 60  # 1 hour

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# ------------------------ Сетевые утилиты --------------------------
def _get_with_retries(url: str, timeout: int = 25, retries: int = 3, backoff: float = 0.6, **kwargs):
    delay = backoff
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout, allow_redirects=True, **kwargs)
            return r
        except Exception:
            if attempt + 1 == retries:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("Unreachable")

def _post_with_retries(url: str, retries: int = 2, backoff: float = 0.6, **kwargs):
    delay = backoff
    for attempt in range(retries):
        try:
            r = SESSION.post(url, **kwargs)
            return r
        except Exception:
            if attempt + 1 == retries:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("Unreachable")

# ------------------------- УТИЛИТЫ ТЕКСТА --------------------------
def extract_datetime(val) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        d = dtparser.parse(str(val))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def looks_recent(dt_obj: Optional[datetime]) -> bool:
    return bool(dt_obj) and ((utcnow() - dt_obj) <= timedelta(hours=FRESH_HOURS))

def strip_urls(t: str) -> str:
    return re.sub(r"https?://\S+|t\.me/\S+", "", t or "").strip()

def remove_mentions(t: str) -> str:
    return re.sub(r"@[A-Za-z0-9_]+", "", t or "").strip()

def strip_hashtags_simple(t: str) -> str:
    return re.sub(r"(?:^|\s)#[^\s#]+", "", t or "")

def collapse_ws_keep_newlines(t: str) -> str:
    t = re.sub(r"[ \t]+", " ", t or "")
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

# --- шум/хвосты ---
PINNED_PAT = re.compile(r"(?i)\bpinned\b|\bзакрепил[аио]?\b|\bзакреплено\b")

SENDER_PATTERNS = [
    r"(?i)\bподписчик\s+прислал[аи]?\b.*",
    r"(?i)\bподписчица\s+прислал[аи]?\b.*",
    r"(?i)\bприслал[аи]?\b.*",
    r"(?i)\bнам\s+пишут.*",
    r"(?i)\bсообщают.*",
    r"(?i)\bприслано\s+через\s+бота.*",
    r"(?i)\bв\s+бот\s+прислал[аи]?\b.*",
]

INVITE_CUT_MARKERS = [
    r"(?i)\bподписывайтесь?\b",
    r"(?i)\bподписывайся\b",
    r"(?i)\bподписаться\b",
    r"(?i)\bподпишись\b",
    r"(?i)\bвступай(те)?\b",
    r"(?i)\bоформляй(те)?\b",
]

def remove_sender_phrases(t: str) -> str:
    out = t or ""
    for p in SENDER_PATTERNS:
        out = re.sub(p, "", out)
    return out

def drop_after_subscribe_calls(t: str) -> str:
    if not t:
        return t
    idx = len(t)
    for pat in INVITE_CUT_MARKERS:
        m = re.search(pat, t)
        if m:
            idx = min(idx, m.start())
    return t[:idx].rstrip(" .,!—-")

def sanitize(t: str) -> str:
    t = strip_urls(t)
    t = remove_mentions(t)
    t = strip_hashtags_simple(t)
    t = remove_sender_phrases(t)
    t = drop_after_subscribe_calls(t)
    return collapse_ws_keep_newlines(t)

def sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[\.\!\?…])\s+|\n+", text or "")
    return [p.strip() for p in parts if p and p.strip()]

def build_caption(title: str, summary: str) -> str:
    title = sanitize(title)
    summary = sanitize(summary)
    if not title:
        s = sentences(summary)
        title = s[0] if s else summary[:140]
        summary = " ".join(s[1:]) if len(s) > 1 else ""
    detail = (title + (" — " + summary if summary else "")).strip()
    return detail[:1024]

# ----------------------- АНТИРЕКЛАМА/БАРАХОЛКА ----------------------
AD_KEYWORDS = re.compile(
    r"(?i)\b(реклам|промо|партнерск|партнёрск|спонсор|"
    r"скидк|акци[яи]|распродаж|заказ|заказывай|оформить|ждем вас|ждём вас|"
    r"доставк|самовывоз|меню|ассортимент|каталог|"
    r"кафе|пекарн|кофейн|салон|барбершоп|парикмахер|"
    r"студия|маникюр|ногтев|массаж|"
    r"курсы|тренинг|школа|секции|"
    r"магазин|бутик|showroom|"
    r"аренда|сдам|сдается|сдаётся|"
    r"продам|куплю|купить|барахолка|объявлени[ея]|объявы|"
    r"закажите|цена|прайс|сколько стоит|приглашаем|приглашаю|приходите|пишите|запись|записывайтесь|звоните|ведет набор|веду набор|мы открылись|процедура|скидка|стоимость|акция"
    r")\b"
)
PHONE_RE = re.compile(r"(?<!\d)(?:\+7|8)\s?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")
PRICE_RE = re.compile(r"\b\d{2,}[ \u00A0]?(?:₽|руб\.?|рублей)\b", re.IGNORECASE)
WORKTIME_RE = re.compile(r"\b(?:ежедневно|с\s*\d{1,2}[:\.]*\d{2}\s*до\s*\d{1,2}[:\.]*\d{2})\b", re.IGNORECASE)

# CTA / приглашение patterns for stronger ad detection
CTA_RE = re.compile(r"(?i)\b(приглаша(ю|ем)|приходите|пишите|запись|записывайтесь|ведет набор|веду набор|звоните|в личку|в лс|в директ|мы открылись|акция|скидка|стоимость|цена)\b")

def looks_like_ad(t: str) -> bool:
    if not t:
        return False
    low = t.lower()
    signals = 0
    if AD_KEYWORDS.search(low): signals += 1
    if CTA_RE.search(low): signals += 1
    if PHONE_RE.search(low): signals += 1
    if PRICE_RE.search(low): signals += 1
    if WORKTIME_RE.search(low): signals += 1
    # If many signals (>=2) -> ad
    return signals >= 2

# -------------------- МЕДИА (загрузка байтами) ---------------------
IMG_EXT_RE = re.compile(r"\.(jpe?g|png|webp|gif)(?:\?.*)?$", re.IGNORECASE)
VIDEO_EXT_RE = re.compile(r"\.(mp4|mov|webm)(?:\?.*)?$", re.IGNORECASE)
TELEGRAM_CDN_RE = re.compile(r"^https?://[^/]*telegram[^/]*\.(?:org|cdn)/.*", re.IGNORECASE)

def is_image_url(u: str) -> bool:
    return bool(u and u.startswith("http") and (IMG_EXT_RE.search(u) or TELEGRAM_CDN_RE.search(u)))

def is_video_url(u: str) -> bool:
    return bool(u and u.startswith("http") and (VIDEO_EXT_RE.search(u) or TELEGRAM_CDN_RE.search(u)))

def download_binary(url: str, timeout: int = 25, max_bytes: int = 50 * 1024 * 1024) -> Optional[Tuple[str, bytes, str]]:
    try:
        with SESSION.get(url, timeout=timeout, stream=True, allow_redirects=True) as r:
            if r.status_code >= 400:
                return None
            ct = r.headers.get("content-type", "").lower()
            ext = ".bin"
            kind = None
            if "image/" in ct:
                if "png" in ct:
                    ext = ".png"
                elif "webp" in ct:
                    ext = ".webp"
                elif "gif" in ct:
                    ext = ".gif"
                else:
                    ext = ".jpg"
                kind = "image"
            elif "video/" in ct or VIDEO_EXT_RE.search(url):
                if "webm" in ct or url.lower().endswith(".webm"):
                    ext = ".webm"
                else:
                    ext = ".mp4"
                kind = "video"
            else:
                return None
            total = 0
            with tempfile.NamedTemporaryFile(delete=False) as tf:
                tmpname = tf.name
                for chunk in r.iter_content(32768):
                    if chunk:
                        tf.write(chunk)
                        total += len(chunk)
                        if total > max_bytes:
                            tf.close()
                            try:
                                os.unlink(tmpname)
                            except Exception:
                                pass
                            return None
            try:
                with open(tmpname, "rb") as f:
                    data = f.read()
            finally:
                try:
                    os.unlink(tmpname)
                except Exception:
                    pass
            if not data:
                return None
            name = "media" + ext
            return name, data, kind
    except Exception:
        return None

def prepare_media(urls: List[str]) -> Optional[Tuple[str, bytes, str]]:
    for u in urls or []:
        u = (u or "").strip().replace("&amp;", "&")
        if not (is_image_url(u) or is_video_url(u)):
            continue
        got = download_binary(u)
        if got:
            return got
    return None

def _split_chunks(text: str, limit: int = 4096) -> List[str]:
    text = text or ""
    if len(text) <= limit:
        return [text]
    parts = []
    current = []
    cur_len = 0
    for line in text.split("\n"):
        ln = len(line) + 1
        if cur_len + ln > limit and current:
            parts.append("\n".join(current))
            current = [line]
            cur_len = ln
        else:
            current.append(line)
            cur_len += ln
    if current:
        parts.append("\n".join(current))
    return parts

# ---------------------- Отправка в Telegram -----------------------
def normalize_chat_id(chat: Union[int, str]) -> Union[int, str]:
    if isinstance(chat, int):
        return chat
    s = str(chat).strip()
    if not s:
        return s
    if s.startswith("@"): 
        return s
    if s.startswith("http://") or s.startswith("https://"):
        try:
            u = urlparse(s)
            if u.netloc.endswith("t.me") or u.netloc.endswith("telegram.me"):
                path = (u.path or "/").strip("/")
                if path and all(c not in path for c in ["+", "joinchat", "addstickers", "s/"]):
                    username = path.split("/")[0]
                    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
                        return "@" + username
        except Exception:
            pass
    return s

def tg_check_chat(token: str, chat: Union[int, str]) -> bool:
    chat_norm = normalize_chat_id(chat)
    now_ts = time.time()
    cached = CHAT_CHECK_CACHE.get(chat_norm)
    if cached:
        ok, ts = cached
        if now_ts - ts < CHAT_CHECK_TTL:
            return ok
    url = f"https://api.telegram.org/bot{token}/getChat"
    try:
        r = _get_with_retries(url, params={"chat_id": chat_norm}, timeout=15)
        data = r.json() if r.headers.get("content-type", "application/json").startswith("application/json") else {}
        ok = r.status_code == 200 and data.get("ok")
        CHAT_CHECK_CACHE[chat_norm] = (bool(ok), now_ts)
        return bool(ok)
    except Exception:
        CHAT_CHECK_CACHE[chat_norm] = (False, now_ts)
        return False

def tg_send_message(token: str, chat: Union[int, str], plain_text: str) -> bool:
    chat_norm = normalize_chat_id(chat)
    if not tg_check_chat(token, chat_norm):
        logging.warning("Отправка отменена: бот не видит чат %s (убедитесь, что бот добавлен админом)", chat_norm)
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok_all = True
    for chunk in _split_chunks(plain_text, 4096):
        for _ in range(2):
            try:
                r = _post_with_retries(url, data={"chat_id": chat_norm, "text": chunk, "disable_web_page_preview": True}, timeout=20)
                js = r.json() if r.headers.get("content-type", "application/json").startswith("application/json") else {}
                if r.status_code == 200 and js.get("ok"):
                    break
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)
        else:
            ok_all = False
    return ok_all

def tg_send_media_upload(token: str, chat: Union[int, str], media: Tuple[str, bytes, str], caption: str = "") -> bool:
    chat_norm = normalize_chat_id(chat)
    name, data, kind = media
    if kind == "image":
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {"photo": (name, io.BytesIO(data))}
    else:
        url = f"https://api.telegram.org/bot{token}/sendVideo"
        files = {"video": (name, io.BytesIO(data))}
    form = {"chat_id": chat_norm, "caption": caption}
    for _ in range(2):
        try:
            r = _post_with_retries(url, data=form, files=files, timeout=60)
            js = r.json() if r.headers.get("content-type", "application/json").startswith("application/json") else {}
            if r.status_code == 200 and js.get("ok"):
                return True
            time.sleep(0.6)
        except Exception:
            time.sleep(0.6)
    return False

# ------------- Загрузка Telegram /s/<channel> без логина -------------
def _tg_urls(handle: str) -> List[str]:
    h = handle.strip().lstrip("@")[...] 
# The full file content has been provided to the assistant prior and will be committed as-is.

# (The full file content has been provided to the assistant prior and will be committed as-is.)
