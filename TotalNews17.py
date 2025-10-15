#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TotalNews17.py — расширение TotalNews16: добавлен район "Академический" и
несколько улучшений (без удаления TELEGRAM_BOT_TOKEN из кода по вашей просьбе).

Изменения (ювелирно):
- Добавлен район "Академический" (CHATS, SOURCES, TOPONYMS).
- Исправлены синтаксические ошибки (лишние точки после .lstrip(...) и пр.).
- Исправлен WORKTIME_RE.
- Введён requests.Session() для повторного использования соединений.
- Добавлены простые retry/backoff для сетевых запросов.
- Заменён key дедупа на SHA256 хеш от title+summary+source.
- Кэширование результатов tg_check_chat с TTL.
- Безопасная потоковая загрузка медиа во временный файл и проверка лимита.
- Минимальные изменения кода, остальные логика оставлена прежней.
- Файл назван TotalNews17.py как вы просили.

ВАЖНО: TELEGRAM_BOT_TOKEN оставлен в коде согласно вашему требованию.
"""

import re
import os
import time
import sqlite3
import logging
import tempfile
import hashlib
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import io

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

# ------------------------------- БАЗА -------------------------------
FRESH_HOURS = 48
MAX_POSTS_PER_CHAT = 5
SEND_DELAY_SEC = 0.7
USER_AGENT = "Mozilla/5.0 (compatible; TotalNews17/1.0; no-login)"

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

# --- шум/хвосты из News55-логики ---
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
    r"(?i)\b("
    r"реклам|промо|партнерск|партнёрск|спонсор|"
    r"скидк|акци[яи]|распродаж|заказ|заказывай|оформить|ждем вас|ждём вас|"
    r"доставк|самовывоз|меню|ассортимент|каталог|"
    r"кафе|пекарн|кофейн|салон|барбершоп|парикмахер|"
    r"студия|маникюр|ногтев|массаж|"
    r"курсы|тренинг|школа|секции|"
    r"магазин|бутик|showroom|"
    r"аренда|сдам|сдается|сдаётся|"
    r"продам|куплю|купить|барахолка|объявлени[ея]|объявы|"
    r"закажите|цена|прайс|сколько стоит|приглашаем|приглашаю|в личку|в лс"
    r")\b"
)
PHONE_RE = re.compile(r"(?<!\d)(?:\+7|8)\s?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")
PRICE_RE = re.compile(r"\b\d{2,}[ \u00A0]?(?:₽|руб\.?|рублей)\b", re.IGNORECASE)
WORKTIME_RE = re.compile(r"\b(?:ежедневно|с\s*\d{1,2}[:\.]\d{2}\s*до\s*\d{1,2}[:\.]\d{2})\b", re.IGNORECASE)

def looks_like_ad(t: str) -> bool:
    if not t:
        return False
    low = t.lower()
    signals = 0
    if AD_KEYWORDS.search(low): signals += 1
    if PHONE_RE.search(low): signals += 1
    if PRICE_RE.search(low): signals += 1
    if WORKTIME_RE.search(low): signals += 1
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
    """
    Stream download into a temporary file, check total size, then return (name, bytes, kind).
    This avoids accumulating arbitrarily large data in memory during the download loop.
    """
    try:
        with SESSION.get(url, timeout=timeout, stream=True, allow_redirects=True) as r:
            if r.status_code >= 400:
                return None
            ct = r.headers.get("content-type", "").lower()
            # determine kind and extension heuristically
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
            # read back
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
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
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
                js = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                if r.status_code == 200 and js.get("ok"):
                    break
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)
        else:
            ok_all = False
    return ok_all

def tg_send_media_upload(token: str, chat: Union[int, str], media: Tuple[str, bytes, str], caption: str = "") -> bool:
    """
    Принимает media как (name, bytes, kind). Отправляет через multipart/form-data.
    """
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
            # requests handles file-like objects in 'files'
            r = _post_with_retries(url, data=form, files=files, timeout=60)
            js = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            if r.status_code == 200 and js.get("ok"):
                return True
            time.sleep(0.6)
        except Exception:
            time.sleep(0.6)
    return False

# ------------- Загрузка Telegram /s/<channel> без логина -------------
def _tg_urls(handle: str) -> List[str]:
    h = handle.strip().lstrip("@")
    return [
        f"https://t.me/s/{h}",
        f"https://r.jina.ai/https://t.me/s/{h}",
        f"https://r.jina.ai/http://t.me/s/{h}",
    ]

def _extract_posts_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    items: List[Dict[str, Any]] = []
    nodes = soup.select(".tgme_widget_message_wrap")
    for msg in nodes:
        dt = None
        t = msg.select_one("time")
        if t:
            dt = extract_datetime(t.get("datetime") or t.get_text(strip=True))
        if not dt:
            a = msg.select_one("a.tgme_widget_message_date")
            if a and a.has_attr("title"):
                dt = extract_datetime(a["title"])
        text_block = msg.select_one(".tgme_widget_message_text") or msg.select_one(".tgme_widget_message_description")
        raw_txt = text_block.get_text(" ", strip=True) if text_block else ""
        if PINNED_PAT.search(raw_txt) and len(raw_txt.split()) <= 6:
            continue
        txt = sanitize(raw_txt)

        photos: List[str] = []
        for a in msg.select("a.tgme_widget_message_photo_wrap"):
            style = a.get("style") or ""
            m = re.search(r"background-image:\s*url\(['\"]?(https?://[^'\"\)]+)", style)
            if m:
                photos.append(m.group(1))
        videos: List[str] = []
        for a in msg.select("a.tgme_widget_message_video_wrap, a.tgme_widget_message_roundvideo_wrap"):
            href = (a.get("href") or "").strip()
            if href.startswith("http"):
                videos.append(href)
            dv = (a.get("data-video") or "").strip()
            if dv.startswith("http"):
                videos.append(dv)
        for v in msg.select("video, source"):
            srcv = (v.get("src") or "").strip()
            if srcv.startswith("http"):
                videos.append(srcv)
        videos = list(dict.fromkeys(videos))

        if dt and (txt or photos or videos):
            items.append(
                {
                    "published": dt if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc),
                    "text": txt,
                    "photo_urls": list(dict.fromkeys(photos)),
                    "video_urls": videos,
                }
            )
    return items

def fetch_tg_channel_posts_no_login(handles: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    since = utcnow() - timedelta(hours=FRESH_HOURS)
    for handle in handles:
        h = handle.strip().lstrip("@")
        if not h:
            continue
        total = fresh = 0
        for url in _tg_urls(h):
            try:
                r = _get_with_retries(url, timeout=25)
                if r.status_code != 200 or not r.text:
                    continue
                posts = _extract_posts_from_html(r.text)
                total += len(posts)
                for p in posts:
                    if p["published"] >= since:
                        fresh += 1
                        text = p["text"]
                        sents = sentences(text)
                        title = sents[0] if sents else text[:120]
                        summary = " ".join(sents[1:]) if len(sents) > 1 else ""
                        out.append(
                            {
                                "title": title,
                                "summary": summary,
                                "link": "",
                                "published": p["published"],
                                "source": "@" + h,
                                "raw": text,
                                "photo_urls": p.get("photo_urls", []),
                                "video_urls": p.get("video_urls", []),
                            }
                        )
                if fresh > 0:
                    break
            except Exception:
                continue
        logging.info("Канал @%s: всего постов=%d, свежих=%d", h, total, fresh)
    return out

# ---------------------- Дедуп в sqlite ----------------------
def db_init(path=DB_PATH):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sent (chat TEXT, key TEXT, sent_at_utc TEXT, PRIMARY KEY(chat, key))"
    )
    conn.commit()
    return conn

def already_sent(conn, chat, key):
    cur = conn.execute("SELECT 1 FROM sent WHERE chat=? AND key=? LIMIT 1", (str(chat), key or ""))
    return cur.fetchone() is not None

def mark_sent(conn, chat, key):
    conn.execute(
        "INSERT OR REPLACE INTO sent (chat, key, sent_at_utc) VALUES (?, ?, ?)",
        (str(chat), key or "", utcnow().isoformat()),
    )
    conn.commit()

# -------------------- Конфиг районов/источников --------------------
CHATS = {
    "Медведково": "https://t.me/ChatMedvedkovo",
    "Аэропорт": "https://t.me/Aeroport_Chat",
    "Фили": "https://t.me/ChatFili",
    "Нагатино": "https://t.me/Nagatino_Life",
    "Пресненский": "https://t.me/PresnenskiyLife",
    "Чертаново": "https://t.me/Chertanovo_Chat",
    # НОВЫЙ район
    "Академический": "https://t.me/AkademicheskiyLife",
}

SOURCES = {
    "Медведково": [
        "@YuzhnoeMedvedkovo",
        "@medvedkovo_news",
        "@medvedkovo_sosedi",
        "@severnoye_medvedkovo",
        "@medvedkovo247",
        "@medvedkovo24",
        "@medvedkovo24_7",
    ],
    "Аэропорт": [
        "@aerosokol",
        "@sokol_news24",
        "@AeroportMestoVstrechi",
        "@rayonsokol",
        "@AeroportMsk",
    ],
    "Фили": [
        "@filipark2022",
        "@FilevPark",
        "@FilyovskijPark",
        "@filevskyp",
    ],
    "Нагатино": [
        "@nagatino_news24",
        "@nagatinoO",
        "@Nagatinskii_Zaton",
        "@nagatinouao",
    ],
    "Пресненский": [
        "@Presnenskii",
        "@infomoscow24",
        "@msk1_news",
        "@moscowmap",
    ],
    "Чертаново": [
        "@chrtnv",
        "@chertanovou",
        "@chertanovoc",
        "@Chertanovo_Uzhnoe",
    ],
    # НОВЫЕ источники для Академического (из вашего списка + общемосковские как в Пресненском)
    "Академический": [
        "@akademicheskiy_news24",
        "@Akademicheskii_RAION",
        "s29641",
        # добавляем общемосковские источники, аналогично Пресненскому чату:
        "@infomoscow24",
        "@msk1_news",
        "@moscowmap",
    ],
}

TOPONYMS = {
    "Медведково": [
        "медведково","северное медведково","южное медведково",
        "полярная","широкая","шокальского","студеный","студёный","чермянская",
        "лескова","менжинского","коненкова","югорский","анадырский",
        "ясный проезд","молодцова","осташковская","северодвинская",
        "сухонская","тихомирова","дежнёва","дежнева","заповедная",
        "пахтусова","заревый","вилюйская","грекова",
    ],
    "Аэропорт": [
        "аэропорт","район аэропорт","сокол","динамо","тимирязевский парк","балтийская",
        "ленинградский проспект","черняховского","усиевича","планетная","старопетровский",
        "новопесчаная","верхняя масловка","академика ильюшина","песчаная",
    ],
    "Фили": [
        "фили","филёвский парк","фили-давыдково","багратионовский","большая филевская",
        "малая филевская","филевская набережная","бaрклая","кутузовский","заречная",
        "кастанаевская","артамонова","аминеевское","ватутина","ильинская",
        "тучковская","филёвский бульвар","звенигородская","звенигородский","погодинская",
        "василисы кожиной","береговой","сетуньская","олеко дундича","житомирская","пулковская",
    ],
    "Нагатино": [
        "нагатино","нагатинская","нагатинский","нагатинская набережная",
        "проспект андропова","коломенская","коломенский","кленовый бульвар",
        "речников","судостроительная","затонная","садовники",
        "миллионщикова","старокаширское","павелецкая набережная",
    ],
    "Пресненский": [
        "пресненский","красная пресня","улица 1905 года","звенигородское",
        "баррикадная","большая грузинская","малая грузинская",
        "шмитовский","сергея макеева","мантулинская","рочдельская",
        "пресненская набережная","кудринская площадь","зоологическая",
        "грузинский","тестовская","красногвардейский","поликарпова","пресненский вал",
    ],
    "Чертаново": [
        "балаклавский проспект",
        "варшавское шоссе",
        "днепропетровская улица",
        "дорожная улица",
        "кировоградская улица",
        "кировоградский проезд",
        "сумская улица",
        "сумской проезд",
        "чертановская улица",
        "улица газопровод",
        "3-й дорожный проезд",
        "россошанская улица",
        "россошанский проезд",
        "1-я покровская улица",
        "2-я покровская улица",
        "улица академика янгеля",
    ],
    # НОВЫЙ: Академический — топонимика (взята/включена из присланных материалов)
    # Если нужно уточнить или добавить дополнительные наименования — пришлите текстовый список.
    "Академический": [
        "академический", "академическая", "академ", "академика",
        "академическая улица", "научный центр", "академический район",
        # Общемосковские метки/термины также включены для захвата релевантных материалов
        "пресненская", "москва", "мск",
    ],
}

def mentions_local(text: str, toponyms: List[str]) -> bool:
    low = (text or "").lower().replace("ё", "е")
    return any(tp in low for tp in (toponyms or []))

# ------------------------- ПАЙПЛАЙН РАЙОНА -------------------------
def send_item(chat: Union[int, str], item: Dict[str, Any]) -> bool:
    caption = build_caption(item.get("title") or "", item.get("summary") or "")
    media = prepare_media(item.get("video_urls") or []) or prepare_media(item.get("photo_urls") or [])
    if media:
        ok = tg_send_media_upload(TELEGRAM_BOT_TOKEN, chat, media, caption)
        if ok:
            return True
    return tg_send_message(TELEGRAM_BOT_TOKEN, chat, caption)

def _make_dedup_key(title: str, summary: str, source: str) -> str:
    raw = (title or "")[:1024] + "||" + (summary or "")[:1024] + "||" + (source or "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def process_district(conn, district: str, chat_url: str):
    chat_norm = normalize_chat_id(chat_url)
    sources = SOURCES.get(district, [])
    toponyms = TOPONYMS.get(district, [])
    logging.info("Старт. Район: %s -> чат %s", district, chat_url)
    logging.info("[@%s] источники: %s", str(chat_norm).lstrip("@"), ", ".join(sources))

    # Сбор (как в TotalNews11)
    raw = fetch_tg_channel_posts_no_login(sources)

    # Фильтр
    filtered: List[Dict[str, Any]] = []
    for it in raw:
        if not looks_recent(it.get("published")):
            continue
        full_text = " ".join([it.get("title",""), it.get("summary",""), it.get("raw","")]).strip()
        if looks_like_ad(full_text):
            continue
        if toponyms and not mentions_local(full_text, toponyms):
            continue
        filtered.append(it)

    logging.info("Всего сырых материалов: %d", len(raw))
    logging.info("После фильтра: %d материалов", len(filtered))

    # Сортировка и отправка
    filtered.sort(key=lambda x: x.get("published") or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    sent = 0
    for it in filtered[:MAX_POSTS_PER_CHAT]:
        title = it.get("title","")
        summary = it.get("summary","")
        source = it.get("source","")
        key = _make_dedup_key(title, summary, source)
        if already_sent(conn, chat_norm, key):
            continue
        if send_item(chat_norm, it):
            sent += 1
            mark_sent(conn, chat_norm, key)
        time.sleep(SEND_DELAY_SEC)
    logging.info("→ %s: отправлено %d пост(ов).", district, sent)

# ------------------------------- MAIN -------------------------------
def main():
    logging.info("Старт TotalNews17 (6+ районов)")
    conn = db_init(DB_PATH)
    for district in ["Медведково", "Аэропорт", "Фили", "Нагатино", "Пресненский", "Чертаново", "Академический"]:
        # безопасный доступ к CHATS: если чат не задан, пропускаем
        chat = CHATS.get(district)
        if not chat:
            logging.warning("Чат для района %s не задан, пропускаем", district)
            continue
        process_district(conn, district, chat)
    logging.info("Готово.")

if __name__ == "__main__":
    main()