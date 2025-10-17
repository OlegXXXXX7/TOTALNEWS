#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TotalNews23.py — TotalNews19-derived runtime with:
- user-provided chats/sources included,
- stricter filtering (ads, greetings, non-local markers, politics),
- editorial cleanup (remove "прислал/прислала/от подписчика" and trailing channel signatures,
  includes "Зеленоград live" among signatures),
- end-of-sentence trimming,
- media download & send logic preserved (not changed).

Changes requested by user (implemented here):
- Do NOT change media sending logic.
- Remove common Moscow sources from all source lists (keep only local sources).
- Add "Зеленоград live" to phrases trimmed as signatures.
- Fix issue where news were not sent to all chats: semantic deduplication now checks per-target-chat
  (so sending a post to chat A does not block a similar post from being sent to chat B).
"""
from __future__ import annotations
import re
import os
import sys
import time
import sqlite3
import logging
import tempfile
import hashlib
import io
from typing import List, Dict, Any, Optional, Tuple, Union, Set
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

# Optional: try to import pandas/openpyxl when available to read ТОПОНИМИКА.xlsx
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

# ------------------------------- CONFIG -------------------------------
FRESH_HOURS = 48
MAX_POSTS_PER_CHAT = 5
SEND_DELAY_SEC = 0.7
USER_AGENT = "Mozilla/5.0 (compatible; TotalNews23/1.0; no-login)"

# TELEGRAM TOKEN (left inline per user's request)
TELEGRAM_BOT_TOKEN = "8339813256:AAGL6WOkl0DWEdt_zmENZ1R5-XxfTHNwqOM"

DB_PATH = "sent.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("TotalNews23")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ru,en;q=0.9"})

CHAT_CHECK_CACHE: Dict[str, Tuple[bool, float]] = {}
CHAT_CHECK_TTL = 60 * 60  # 1 hour

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# ------------------------ Network utilities --------------------------
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


# ------------------------- Text utilities --------------------------
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


PINNED_PAT = re.compile(r"(?i)\bpinned\b|\bзакрепил[аио]?\b|\bзакреплено\b")


def sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[\.\!\?…])\s+|\n+", text or "")
    return [p.strip() for p in parts if p and p.strip()]


def build_caption(title: str, summary: str) -> str:
    title = sanitize(title)
    summary = sanitize(summary)
    if not title:
        s = sentences(summary)
        title = s[0] if s else summary[:120]
        summary = " ".join(s[1:]) if len(s) > 1 else ""
    detail = (title + (" — " + summary if summary else "")).strip()
    return detail[:1024]


# ----------------------- FILTER/TRIGGERS ----------------------
AD_KEYWORDS_PARTS = [
    "реклам", "промо", "партнерск", "партнёрск", "спонсор",
    "скидк", "дисконт", "акци", "распродаж", "заказ", "заказывай", "оформить",
    "ждем вас", "ждём вас", "доставк", "самовывоз", "меню", "ассортимент", "каталог",
    "кафе", "пекарн", "кофейн", "салон", "барбершоп", "парикмахер",
    "студия", "маникюр", "ногтев", "массаж",
    "курсы", "тренинг", "школа", "секции",
    "магазин", "бутик", "showroom",
    "аренда", "сдам", "сдается", "сдаётся",
    "продам", "куплю", "купить", "барахолка",
    "объявлени", "объявы", "закажите", "цена", "прайс", "сколько стоит",
    "получите в подарок", "подписаться", "приветств", "ведётс?я набор", "ведут набор",
    "пишите", "введут штраф", "введут штрафы", "обучение", "бесплатное обучение",
    "мы находимся", "контакт", "контакты", "хорошего рабочего дня",
    "ищу сотрудник", "ищу сотрудницу", "услуги оказывает", "старт продаж",
    "россиянам", "россиян", "в россии", "сдам квартиру", "вакансия",
    "розыгрыш", "подписанным", "получите в подарок",
    "доброе утро", "добрый день", "добрый вечер", "доброй ночи", "доброе утречко"
]

escaped_parts = []
for p in AD_KEYWORDS_PARTS:
    if re.search(r"[\\\[\]\?\(\)\{\}\^\$\.\|\+\*]", p):
        escaped_parts.append(p)
    else:
        escaped_parts.append(re.escape(p))

AD_KEYWORDS = re.compile(r"\b(?:" + r"|".join(escaped_parts) + r")\b", flags=re.IGNORECASE)

PHONE_RE = re.compile(r"(?<!\d)(?:\+7|8)\s?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")
PRICE_RE = re.compile(r"\b\d{2,}[ \u00A0]?(?:₽|руб\.?|рублей)\b", re.IGNORECASE)
WORKTIME_RE = re.compile(r"\b(?:ежедневно|с\s*\d{1,2}[:\.]*\d{2}\s*до\s*\d{1,2}[:\.]*\d{2})\b", re.IGNORECASE)

CTA_PATTERNS = ["подписыв", "подписаться", "подпишись", "вступай", "приходите", "звоните", "запись", "пишите"]
CTA_RE = re.compile("|".join(re.escape(p) for p in CTA_PATTERNS), flags=re.IGNORECASE)

POLITICAL_WORDS = [
    "мэр", "губернатор", "депутат", "единая россия", "госдума", "путин", "партия",
    "политик", "оппозиция", "выбор", "голосован", "правительство", "министр",
    "кандидат", "парламент", "сенат", "президент", "избират", "кампания", "санкц", "война",
    "депутаты", "голосования", "оппозиционный", "сенатор",
    "митинг", "протест", "законопроект", "референдум", "голос", "голосование",
    "конституция", "премьер", "кремль", "администрация"
]
POLITICAL_RE = re.compile(r"\b(?:" + r"|".join(re.escape(w) for w in POLITICAL_WORDS) + r")\b", flags=re.IGNORECASE)


def looks_like_ad_or_unwanted(t: str) -> bool:
    if not t:
        return False
    low = t.lower()
    if POLITICAL_RE.search(low):
        return True
    signals = 0
    if AD_KEYWORDS.search(low):
        signals += 1
    if CTA_RE.search(low):
        signals += 1
    if PHONE_RE.search(low):
        signals += 1
    if PRICE_RE.search(low):
        signals += 1
    if WORKTIME_RE.search(low):
        signals += 1
    if re.search(r"\b(россия|россиянам|в россии)\b", low):
        signals += 1
    return signals >= 2


# -------------------- EDITORIAL: remove sender phrases and channel signatures --------------------
SENDER_PATTERNS = [
    r"(?i)\bприслать\s+новост", r"(?i)\bприслала\s+подписчица\b", r"(?i)\bприслал\s+подписчик\b",
    r"(?i)\bот\s+подписчик", r"(?i)\bот\s+подписчица", r"(?i)\bподписчик\s+прислал\b", r"(?i)\bподписчица\s+прислала\b",
    r"(?i)\bприслан(?:о|а)(?:\s+через\s+бота)?\b", r"(?i)\bнам\s+пишут\b", r"(?i)\bсообщают\b",
    r"(?i)\bприсла(?:ла|л)\b", r"(?i)\bот\s+подписчика\b", r"(?i)\bприслала\s+новость\b"
]

INVITE_CUT_MARKERS = [
    "подписывайтесь?", "подписывайт", "подписаться", "подпишись", "вступай",
    "приглаша", "запись", "записывайт", "пишите", "контакт", "контакты"
]


def remove_sender_phrases(t: str) -> str:
    if not t:
        return t
    out = t
    for p in SENDER_PATTERNS:
        out = re.sub(p, "", out)
    return out


def drop_after_subscribe_calls(t: str) -> str:
    if not t:
        return t
    idx = len(t)
    for pat in INVITE_CUT_MARKERS:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            idx = min(idx, m.start())
    return t[:idx].rstrip(" .,!—-")


_SIGNATURE_TOKENS: List[str] = []


def _build_signature_tokens_from_sources(sources_map: Dict[str, List[str]], chats_map: Dict[str, str]) -> List[str]:
    tokens = set()
    for chat_name in chats_map.keys():
        if not chat_name:
            continue
        t = str(chat_name).lower().replace("_", " ").strip()
        tokens.add(t)
    for _, vals in sources_map.items():
        for v in vals:
            vv = (v or "").strip()
            if vv.startswith("@"):
                vv = vv[1:]
            vv = vv.replace("_", " ").lower().strip()
            tokens.add(vv)
            tokens.add(vv.replace(" ", ""))
    extras = ["перово-новогиреево-москва", "химки онлайн", "москва", "новости района", "онлайн", "чат", "зеленоград live"]
    for e in extras:
        tokens.add(e.lower())
    tokens = {t for t in tokens if t and len(t) >= 3}
    return sorted(tokens, key=lambda x: -len(x))


def _looks_like_signature_line(line: str) -> bool:
    if not line or len(line.strip()) < 3:
        return False
    ln = line.strip()
    lnl = re.sub(r"[^\w\s\-]", " ", ln).lower()
    for tok in _SIGNATURE_TOKENS:
        if tok in lnl:
            return True
    words = ln.split()
    if len(words) <= 5 and any(re.search(r"[А-ЯЁA-Z]", w) for w in words):
        return True
    return False


def remove_trailing_signatures(text: str) -> str:
    if not text:
        return text
    lines = text.rstrip().splitlines()
    max_check = min(6, len(lines))
    drop = 0
    for i in range(1, max_check + 1):
        line = lines[-i].strip()
        if _looks_like_signature_line(line):
            drop += 1
        else:
            if line == "":
                drop += 1
                continue
            break
    if drop:
        return "\n".join(lines[:len(lines)-drop]).rstrip()
    return text


def sanitize(t: str) -> str:
    t = strip_urls(t)
    t = remove_mentions(t)
    t = strip_hashtags_simple(t)
    t = remove_sender_phrases(t)
    t = drop_after_subscribe_calls(t)
    t = remove_trailing_signatures(t)
    return collapse_ws_keep_newlines(t)


# ---------------------- End-of-sentence trimming ----------------------
_SENTENCE_END_RE = re.compile(r'(?s)(.*[\.!\?…])')


def trim_to_last_sentence(text: str) -> str:
    if not text:
        return text
    if len(text) < 120:
        return text
    m = _SENTENCE_END_RE.search(text)
    if m:
        trimmed = m.group(1).strip()
        return trimmed if trimmed else text
    return text


# -------------------- MEDIA (download) --------------------
IMG_EXT_RE = re.compile(r"\.(jpe?g|png|webp|gif)(?:\?.*)?$", re.IGNORECASE)
VIDEO_EXT_RE = re.compile(r"\.(mp4|mov|webm)(?:\?.*)?$", re.IGNORECASE)
TELEGRAM_CDN_RE = re.compile(r"^https?://[^/]*telegram[^/]*\.(?:org|cdn|me|telesco.pe)/.*", re.IGNORECASE)


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
                if IMG_EXT_RE.search(url):
                    ext = IMG_EXT_RE.search(url).group(0)
                    kind = "image"
                elif VIDEO_EXT_RE.search(url):
                    ext = VIDEO_EXT_RE.search(url).group(0)
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
        logger.exception("download_binary failed for %s", url)
        return None


def prepare_media(urls: List[str]) -> Optional[Tuple[str, bytes, str]]:
    for u in urls or []:
        u = (u or "").strip().replace("&amp;", "&")
        if not u:
            continue
        if not (is_image_url(u) or is_video_url(u)):
            continue
        got = download_binary(u)
        if got:
            return got
    return None


# ---------------------- Telegram helpers -----------------------
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
    # media sending intentionally unchanged
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
            js = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            if r.status_code == 200 and js.get("ok"):
                return True
            time.sleep(0.6)
        except Exception:
            time.sleep(0.6)
    return False


# ---------------------- HTML parsing helpers (proven) ----------------------
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
    nodes = soup.select(".tgme_widget_message_wrap, .tgme_widget_message")
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
        for a in msg.select("a.tgme_widget_message_photo_wrap, a.tgme_widget_message_photo"):
            style = a.get("style") or ""
            m = re.search(r"background-image:\s*url\(['\"]?(https?://[^'\"\)]+)", style)
            if m:
                photos.append(m.group(1))
                continue
            img = a.find("img")
            if img:
                href = img.get("src") or img.get("data-src")
                if href:
                    photos.append(href)
                    continue
            href = a.get("href") or a.get("data-src")
            if href and href.startswith("http"):
                photos.append(href)

        videos: List[str] = []
        for a in msg.select("a.tgme_widget_message_video_wrap, a.tgme_widget_message_roundvideo_wrap, a.tgme_widget_message_video"):
            href = (a.get("href") or "").strip()
            if href.startswith("http"):
                videos.append(href)
            dv = (a.get("data-video") or "").strip()
            if dv.startswith("http"):
                videos.append(dv)
            style = a.get("style") or ""
            hv = re.search(r"url\((['\"]?)(.*?)\1\)", style)
            if hv:
                videos.append(hv.group(2))

        for v in msg.select("video, source"):
            srcv = (v.get("src") or "").strip()
            if srcv.startswith("http"):
                videos.append(srcv)

        photos = list(dict.fromkeys([p for p in photos if p]))
        videos = list(dict.fromkeys([v for v in videos if v]))

        photos = [u for u in photos if not re.search(r"/i/|avatar|logo|thumb|cover|header|icon", u, flags=re.IGNORECASE)]
        videos = [u for u in videos if not re.search(r"/i/|avatar|logo|thumb|cover|header|icon", u, flags=re.IGNORECASE)]

        if dt and (txt or photos or videos):
            items.append(
                {
                    "published": dt if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc),
                    "text": txt,
                    "photo_urls": photos,
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
        logger.info("Channel @%s: total posts=%d, fresh=%d", h, total, fresh)
    return out


# ---------------------- Dedup DB ----------------------
def db_init(path=DB_PATH):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sent (chat TEXT, key TEXT, sent_at_utc TEXT, PRIMARY KEY(chat, key))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sent_texts (chat TEXT, text_hash TEXT, excerpt TEXT, sent_at_utc TEXT, PRIMARY KEY(chat, text_hash))"
    )
    conn.commit()
    return conn


def already_sent(conn, chat, key):
    cur = conn.execute("SELECT 1 FROM sent WHERE chat=? AND key=? LIMIT 1", (str(chat), key or ""))
    return cur.fetchone() is not None


def already_sent_text_hash(conn, chat, text_hash):
    cur = conn.execute("SELECT 1 FROM sent_texts WHERE chat=? AND text_hash=? LIMIT 1", (str(chat), text_hash or ""))
    return cur.fetchone() is not None


def mark_sent(conn, chat, key, excerpt: Optional[str] = None):
    conn.execute(
        "INSERT OR REPLACE INTO sent (chat, key, sent_at_utc) VALUES (?, ?, ?)",
        (str(chat), key or "", utcnow().isoformat()),
    )
    if excerpt is not None:
        text_hash = hashlib.sha256(excerpt.encode("utf-8")).hexdigest()
        conn.execute(
            "INSERT OR REPLACE INTO sent_texts (chat, text_hash, excerpt, sent_at_utc) VALUES (?, ?, ?, ?)",
            (str(chat), text_hash, excerpt, utcnow().isoformat()),
        )
    conn.commit()


def semantic_already_sent(conn, chat, token_set: Set[str], min_common: int = 4) -> bool:
    """
    Check whether a similar excerpt was already sent to THIS chat (not globally).
    This change ensures posting to other chats is not blocked by items already sent to a different chat.
    """
    try:
        rows = []
        cur = conn.execute("SELECT excerpt FROM sent_texts WHERE chat=?", (str(chat),))
        rows.extend([r[0] for r in cur.fetchall() if r and r[0]])
        for ex in rows:
            ex_tokens = set(ex.split())
            if not ex_tokens:
                continue
            if len(token_set & ex_tokens) >= min_common:
                return True
    except Exception:
        return False
    return False


# -------------------- Config: existing + user-provided chats/sources --------------------
COMMON_CITYWIDE_SOURCES = [
    "@infomoscow24",
    "@msk1_news",
    "@moscowmap",
]

CHATS = {
    "Медведково": "https://t.me/ChatMedvedkovo",
    "Аэропорт": "https://t.me/Aeroport_Chat",
    "Фили": "https://t.me/ChatFili",
    "Нагатино": "https://t.me/Nagatino_Life",
    "Пресненский": "https://t.me/PresnenskiyLife",
    "Чертаново": "https://t.me/Chertanovo_Chat",
    "Академический": "https://t.me/AkademicheskiyLife",
    "Царицыно": "https://t.me/TsarytsinoChat",
    "Люблино": "https://t.me/Chat_Lublino",
    "Перово": "https://t.me/ChatPerovo",
    "Южнопортовый": "https://t.me/UzhnoportChat",
    "Химки": "https://t.me/HimkiChat",
    "Митино": "https://t.me/MitinoChat",
    # user-provided chat targets
    "PutilkovoLifeChat": "https://t.me/PutilkovoLifeChat",
    "Zelenograd_LifeChat": "https://t.me/Zelenograd_LifeChat",
    "Chat_OdintsovoCity": "https://t.me/Chat_OdintsovoCity",
    "PatrikiOfficial": "https://t.me/PatrikiOfficial",
    "PokrovskoeStreshnevoChat": "https://t.me/PokrovskoeStreshnevoChat",
    "ChatKommunarka": "https://t.me/ChatKommunarka",
    "ChatProspektVernadskogo": "https://t.me/ChatProspektVernadskogo",
    "ChatMetrogorodok": "https://t.me/ChatMetrogorodok",
    "ChatKonkovo": "https://t.me/ChatKonkovo",
    "ChatTushino": "https://t.me/ChatTushino",
}

SOURCES: Dict[str, List[str]] = {
    "Медведково": ["@YuzhnoeMedvedkovo","@medvedkovo_news","@medvedkovo_sosedi","@severnoye_medvedkovo","@medvedkovo247","@medvedkovo24","@medvedkovo24_7"],
    "Аэропорт": ["@aerosokol","@sokol_news24","@AeroportMestoVstrechi","@rayonsokol","@AeroportMsk"],
    "Фили": ["@filipark2022","@FilevPark","@FilyovskijPark","@filevskyp"],
    "Нагатино": ["@nagatino_news24","@nagatinoO","@Nagatinskii_Zaton","@nagatinouao"],
    "Пресненский": ["@Presnenskii"],
    "Чертаново": ["@chrtnv","@chertanovou","@chertanovoc","@Chertanovo_Uzhnoe"],
    "Академический": ["@akademicheskiy_news24","@Akademicheskii_RAION","s29641"],
    "Царицыно": ["@birulevo_tsar_online","@tsaritsyno_museum","@caricino_news","@Tsaritcyno","@upravatsar"],
    "Люблино": ["@teleg_Lublino_UVAO","@kuzminki_lublino","@kuzminc"],
    "Перово": ["@novogireevo_online","@perovo_veshnyaki","@perovo_msk","@perovo_news","@novogireevo"],
    "Южнопортовый": ["@uportovy","@Uzhnoportovyi","@tekstilshiky"],
    "Химки": ["@khimki_mo","@himki_tut","@tipkhimki"],
    "Митино": ["@lp_mitino","@mitino_org","@mitino_news24"],
    # user-provided mappings (only local sources, common Moscow sources intentionally NOT appended)
    "PutilkovoLifeChat": ["@life_putilkovo", "@putilkovo_info"],
    "Zelenograd_LifeChat": ["@zelenogradru", "@zelenograd", "@chp_Zelenograd", "@zelenogradj"],
    "Chat_OdintsovoCity": ["@odi_city", "@odintsovo_channel", "@odincovo_mosoblast", "@odintsovo_town", "@odintsovo_sluh"],
    "PatrikiOfficial": ["@Presnenskii", "@sosedi_presnensky"],
    "PokrovskoeStreshnevoChat": ["@pokrovskoe_news", "@PokrovskoeStreshnevotop"],
    "ChatKommunarka": ["@kmnrka"],
    "ChatProspektVernadskogo": ["@teleg_ProspektVernadskogo", "@prospectver"],
    "ChatMetrogorodok": ["@metrogorodok_vao", "@metrog", "@sosedi_metrogorodok", "@teleg_Metrogorodok"],
    "ChatKonkovo": ["@teleg_Konkovo", "@konkovo_mos", "@Obruchevskii"],
    "ChatTushino": ["@tushino_dom", "@tyshino_news", "@SevernoeTushinotop", "@chptushino", "@chp_szao", "@tushino5"],
}

# Build signature tokens now that CHATS and SOURCES are known
_SIGNATURE_TOKENS = _build_signature_tokens_from_sources(SOURCES, CHATS)


# ---------------------- ТОПОНИМИКА --------------------
def load_toponyms_from_xlsx(xlsx_path: str) -> Dict[str, List[str]]:
    top = {}
    try:
        if pd is None:
            return {}
        if not os.path.exists(xlsx_path):
            return {}
        df = pd.read_excel(xlsx_path, sheet_name=0, dtype=str)
        for _, row in df.iterrows():
            vals = [str(x).strip() for x in row.tolist() if not pd.isna(x)]
            if not vals:
                continue
            district = vals[0]
            rest = ",".join(vals[1:]) if len(vals) > 1 else ""
            if not rest and len(vals) == 1:
                continue
            toks = re.split(r"[,\n;]+", rest)
            toks = [t.strip().lower() for t in toks if t and t.strip()]
            if toks:
                top[district] = toks
        return top
    except Exception:
        return {}


TOPONYMS: Dict[str, List[str]] = {
    "Медведково": ["медведково","северное медведково","южное медведково"],
    "Аэропорт": ["аэропорт","сокол","динамо"],
    "Фили": ["фили","филёвский парк","фили-давыдково"],
    "Нагатино": ["нагатино","нагатинская","нагатинский"],
    "Пресненский": ["пресненский","пресненская набережная","улица 1905 года"],
    "Чертаново": ["чертаново","балаклавский проспект"],
    "Академический": ["академический","академическая","академика"],
    "Царицыно": ["царицыно","царитцын","царск","царитцyno"],
    "Люблино": ["люблино","кузьминки","кузьминская"],
    "Перово": ["перово","новогиреево","вешняки"],
    "Южнопортовый": ["южнопортовый","южный порт","южно порт"],
    "Химки": ["химки","химкинский","город химки"],
    "Митино": ["митино","улица митинская","митинская"],
}

xlsx_path_candidates = [
    os.path.join(os.path.dirname(__file__), "ТОПОНИМИКА.xlsx") if "__file__" in globals() else "ТОПОНИМИКА.xlsx",
    "ТОПОНИМИКА.xlsx",
]
for p in xlsx_path_candidates:
    loaded = load_toponyms_from_xlsx(p)
    if loaded:
        for k, v in loaded.items():
            if isinstance(v, list) and v:
                TOPONYMS[k] = [t.lower() for t in v]
        logging.info("Loaded ТОПОНИМИКА from %s", p)
        break


def mentions_local(text: str, toponyms: List[str]) -> bool:
    low = (text or "").lower().replace("ё", "е")
    return any(tp in low for tp in (toponyms or []))


# ------------------ Similarity helpers -------------------
STOPWORDS_RU = {
    "и","в","во","не","на","с","по","что","как","это","для","о","об","за","от","до","из",
    "со","а","но","или","у","же","бы","его","ее","она","он","они","нам","вам","их","ему",
    "этого","так","откуда","когда","где","ту","та","те","того","тот","эти","также","ещё",
    "через","при","после","перед","над","под","при","про","без","чтобы"
}


def normalize_for_similarity(text: str) -> List[str]:
    if not text:
        return []
    t = text.lower()
    t = t.replace("ё", "е")
    t = re.sub(r"https?://\S+|t\.me/\S+", " ", t)
    t = re.sub(r"[^а-яa-z0-9\s]", " ", t)
    tokens = [w.strip() for w in t.split() if w.strip()]
    tokens = [w for w in tokens if len(w) >= 3 and w not in STOPWORDS_RU]
    return tokens


def tokens_to_excerpt(tokens: List[str], max_words: int = 80) -> str:
    return " ".join(tokens[:max_words])


# ------------------------- Pipeline & processing -------------------------
def send_item(chat: Union[int, str], item: Dict[str, Any]) -> bool:
    caption = build_caption(item.get("title") or "", item.get("summary") or "")
    # preserve media sending logic, prefer photos then videos
    media = None
    if item.get("photo_urls"):
        media = prepare_media(item.get("photo_urls"))
    if not media and item.get("video_urls"):
        media = prepare_media(item.get("video_urls"))
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
    logging.info("Start district: %s -> chat %s", district, chat_url)
    logging.info("[@%s] sources: %s", str(chat_norm).lstrip("@"), ", ".join(sources))

    raw = fetch_tg_channel_posts_no_login(sources)

    for it in raw:
        full_text = " ".join([it.get("title",""), it.get("summary",""), it.get("raw","")]).strip()
        tokens = normalize_for_similarity(full_text)
        it["tokens"] = set(tokens)
        it["excerpt"] = tokens_to_excerpt(tokens)

    filtered: List[Dict[str, Any]] = []
    for it in raw:
        if not looks_recent(it.get("published")):
            continue
        full_text = " ".join([it.get("title",""), it.get("summary",""), it.get("raw","")]).strip()
        if looks_like_ad_or_unwanted(full_text):
            continue
        if toponyms and not mentions_local(full_text, toponyms):
            continue
        filtered.append(it)

    logging.info("Raw items: %d", len(raw))
    logging.info("After filter: %d", len(filtered))

    filtered.sort(key=lambda x: x.get("published") or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    sent = 0
    sent_token_sets: List[Set[str]] = []
    for it in filtered[:MAX_POSTS_PER_CHAT * 4]:
        title = it.get("title","")
        summary = it.get("summary","")
        source = it.get("source","")
        key = _make_dedup_key(title, summary, source)
        tokens = it.get("tokens", set())

        if already_sent(conn, chat_norm, key):
            continue

        if tokens and semantic_already_sent(conn, chat_norm, tokens, min_common=4):
            logging.info("Skipped (semantic dup in DB for this chat): %s", title)
            continue

        is_dup = False
        for sset in sent_token_sets:
            if len(tokens & sset) >= 4:
                is_dup = True
                break
        if is_dup:
            logging.info("Skipped (semantic dup in session): %s", title)
            continue

        if send_item(chat_norm, it):
            sent += 1
            excerpt = " ".join(sorted(tokens))
            mark_sent(conn, chat_norm, key, excerpt=excerpt)
            sent_token_sets.append(set(tokens))
        time.sleep(SEND_DELAY_SEC)
        if sent >= MAX_POSTS_PER_CHAT:
            break
    logging.info("→ %s: sent %d posts.", district, sent)


def main_once():
    logger.info("Start TotalNews23 run")
    conn = db_init(DB_PATH)
    run_list = [
        "Медведково","Аэропорт","Фили","Нагатино",
        "Пресненский","Чертаново","Академический",
        "Царицыно","Люблино","Перово",
        "Южнопортовый","Химки","Митино",
        "PutilkovoLifeChat","Zelenograd_LifeChat","Chat_OdintsovoCity","PatrikiOfficial","PokrovskoeStreshnevoChat",
        "ChatKommunarka","ChatProspektVernadskogo","ChatMetrogorodok","ChatKonkovo","ChatTushino"
    ]
    for district in run_list:
        chat = CHATS.get(district)
        if not chat:
            logging.warning("No chat for %s, skipping", district)
            continue
        try:
            process_district(conn, district, chat)
        except Exception:
            logger.exception("Error processing district %s", district)
    conn.close()
    logger.info("Done.")


def main_loop(poll_interval: int = 300):
    try:
        while True:
            try:
                main_once()
            except Exception:
                logger.exception("main_once failed")
            logger.info("Sleeping %s seconds before next pass", poll_interval)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Interrupted, exiting")


if __name__ == "__main__":
    arg = (sys.argv[1].lower() if len(sys.argv) > 1 else "loop")
    if arg in ("once", "--once", "run", "run_once"):
        print("Running single pass (run_once)...")
        main_once()
    else:
        print("Starting main_loop (continuous polling). Press Ctrl+C to stop.")
        main_loop()