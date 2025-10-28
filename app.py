# app.py — v4 "near-real-time" tuned to beat the NSE website by seconds

# Changes vs your working file:

# - Independent loops per source (no batching)

# - Faster intervals (NSE 1.5s, SEBI 2s, BSE 2s)

# - Cache buster + no-cache headers on ALL requests

# - Snappy timeouts, warm NSE cookies

# - Price-sensitive filters kept (Option B)

# - Optional DEBUG logs via env DEBUG=1

import os, time, asyncio, aiohttp, feedparser, sqlite3

from datetime import datetime, timezone

from urllib.parse import urlparse, urlencode, urlunparse, parse_qsl

from tenacity import retry, stop_after_attempt, wait_exponential

from dotenv import load_dotenv

from telegram import Bot

from telegram.constants import ParseMode

from dateutil import parser as dtparser

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN","").strip()

CHAT_IDS = [cid.strip() for cid in os.getenv("CHAT_IDS","").split(",") if cid.strip()]

# Per-source intervals (seconds)

NSE_INTERVAL  = float(os.getenv("NSE_INTERVAL",  "1.5"))

SEBI_INTERVAL = float(os.getenv("SEBI_INTERVAL", "2"))

BSE_INTERVAL  = float(os.getenv("BSE_INTERVAL",  "2"))

# Price-sensitive only (Option B). You can extend via KEYWORDS env.

DEFAULT_KEYWORDS = (

    "bonus,split,dividend,buyback,preferential,rights,rights issue,fundraising,qip,"

    "order win,contract,award,loi,letter of intent,merger,demerger,acquisition,amalgamation,"

    "board approves,approval,approves,capacity,expansion,stake sale,fpo,ncd,allotment,issue price"

)

KEYWORDS    = [k.strip().lower() for k in os.getenv("KEYWORDS", DEFAULT_KEYWORDS).split(",") if k.strip()]

WATCHLIST   = [w.strip().lower() for w in os.getenv("WATCHLIST","").split(",") if w.strip()]

ONLY_MATCHING = os.getenv("ONLY_MATCHING","1")=="1"   # keep 1 for best “useful noise vs speed”

SEND_PDF    = os.getenv("SEND_PDF","1")=="1"

MAX_PDF_MB  = float(os.getenv("MAX_PDF_MB","40"))

DB_PATH     = os.getenv("DB_PATH","seen.db")

DEBUG       = os.getenv("DEBUG","0")=="1"

ENABLE_SEBI = os.getenv("ENABLE_SEBI","1")=="1"

ENABLE_NSE  = os.getenv("ENABLE_NSE","1")=="1"

ENABLE_BSE  = os.getenv("ENABLE_BSE","1")=="1"

SEBI_RSS = os.getenv("SEBI_RSS","https://www.sebi.gov.in/sebiweb/rss/corporate-filing.xml")

BSE_RSS  = os.getenv("BSE_RSS","https://www.bseindia.com/xml-data/corpfiling/CorpFiling.xml")

NSE_URL  = os.getenv("NSE_URL","https://www.nseindia.com/api/corporate-announcements?index=equities")

if not BOT_TOKEN: raise SystemExit("ERROR: BOT_TOKEN not set")

if not CHAT_IDS:  raise SystemExit("ERROR: CHAT_IDS empty")

bot = Bot(BOT_TOKEN)

# ---------- Storage ----------

def db_init():

    conn = sqlite3.connect(DB_PATH); c = conn.cursor()

    c.execute("CREATE TABLE IF NOT EXISTS seen (source TEXT, item_id TEXT, PRIMARY KEY(source,item_id))")

    c.execute("CREATE TABLE IF NOT EXISTS sent_pdf (url TEXT PRIMARY KEY)")

    conn.commit(); conn.close()

def is_seen(source,item_id):

    conn = sqlite3.connect(DB_PATH); c = conn.cursor()

    c.execute("SELECT 1 FROM seen WHERE source=? AND item_id=? LIMIT 1",(source,item_id))

    r = c.fetchone(); conn.close(); return r is not None

def seen_add(source,item_id):

    conn = sqlite3.connect(DB_PATH); c = conn.cursor()

    c.execute("INSERT OR IGNORE INTO seen (source,item_id) VALUES (?,?)",(source,item_id))

    conn.commit(); conn.close()

def pdf_was_sent(url):

    conn = sqlite3.connect(DB_PATH); c = conn.cursor()

    c.execute("SELECT 1 FROM sent_pdf WHERE url=? LIMIT 1",(url,))

    r = c.fetchone(); conn.close(); return r is not None

def pdf_mark_sent(url):

    conn = sqlite3.connect(DB_PATH); c = conn.cursor()

    c.execute("INSERT OR IGNORE INTO sent_pdf (url) VALUES (?)",(url,))

    conn.commit(); conn.close()

# ---------- Helpers ----------

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"

USER_HEADERS = {"User-Agent": UA, "Accept":"application/json, text/plain, */*", "Accept-Language":"en-US,en;q=0.9",

                "Connection":"keep-alive", "Referer":"https://www.nseindia.com/"}

def with_cache_buster(url: str) -> str:

    try:

        parts = list(urlparse(url))

        qs = dict(parse_qsl(parts[4]))

        qs["_"] = str(int(time.time() * 1000))  # ms timestamp

        parts[4] = urlencode(qs)

        return urlunparse(parts)

    except Exception:

        return url

def looks_like_pdf_url(url):

    return bool(url) and (url.lower().endswith(".pdf") or "pdf" in url.lower())

def match_filters(text: str) -> bool:

    t = (text or "").lower()

    kw_ok = any(k in t for k in KEYWORDS) if KEYWORDS else True

    wl_ok = any(w in t for w in WATCHLIST) if WATCHLIST else True

    return kw_ok and (wl_ok if WATCHLIST else True)

def fmt_time(dt): return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

async def notify_text(msg):

    for cid in CHAT_IDS:

        try: await bot.send_message(chat_id=cid, text=msg, parse_mode=ParseMode.HTML, disable_web_page_preview=False)

        except Exception as e: 

            if DEBUG: print("Send text error:", e)

async def notify_pdf(caption, filename, content):

    for cid in CHAT_IDS:

        try: await bot.send_document(chat_id=cid, document=content, filename=filename, caption=caption, parse_mode=ParseMode.HTML)

        except Exception as e:

            if DEBUG: print("Send pdf error:", e)

# ---------- Fetchers ----------

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=0.5, max=10))

async def fetch_rss(session, url):

    headers = {"User-Agent": UA, "Cache-Control":"no-cache, no-store", "Pragma":"no-cache"}

    async with session.get(with_cache_buster(url), headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:

        resp.raise_for_status()

        content = await resp.read()

        feed = feedparser.parse(content)

        items = []

        for e in feed.entries:

            uid = getattr(e,"id","") or getattr(e,"link","") or getattr(e,"title","")

            title = getattr(e,"title","") or ""

            link = getattr(e,"link","") or ""

            summary = getattr(e,"summary","") or ""

            published = None

            for key in ("published_parsed","updated_parsed"):

                v = getattr(e, key, None)

                if v:

                    published = datetime(*v[:6], tzinfo=timezone.utc); break

            items.append({"id":uid,"title":title,"summary":summary,"link":link,"published":published,"company":None})

        if DEBUG: print(f"[{datetime.now()}] RSS {url} -> {len(items)}")

        return items

fetch_sebi = lambda s: fetch_rss(s, SEBI_RSS)

fetch_bse  = lambda s: fetch_rss(s, BSE_RSS)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=0.5, max=10))

async def fetch_nse(session):

    async with session.get("https://www.nseindia.com/", headers=USER_HEADERS, timeout=aiohttp.ClientTimeout(total=5)) as resp:

        await resp.read()  # warm cookies

    hdrs = dict(USER_HEADERS); hdrs.update({"Cache-Control":"no-cache, no-store", "Pragma":"no-cache"})

    async with session.get(with_cache_buster(NSE_URL), headers=hdrs, timeout=aiohttp.ClientTimeout(total=8)) as resp:

        resp.raise_for_status()

        data = await resp.json(content_type=None)

        records = data.get("data", data) if isinstance(data, dict) else data

        items = []

        if isinstance(records, list):

            for r in records:

                try:

                    uid = str(r.get("id") or r.get("slno") or r.get("ATTACHMENTNAME") or (r.get("sm_desc","")+r.get("symbol","")))

                    title = r.get("sm_desc") or r.get("HEADLINE") or r.get("desc") or ""

                    comp  = r.get("company") or r.get("companyName") or r.get("symbol") or ""

                    link  = r.get("attchmnt") or r.get("attachment") or r.get("pdfUrl") or r.get("more") or ""

                    if link and link.startswith("/"): link = "https://www.nseindia.com"+link

                    dt = r.get("dt") or r.get("attachmentDt") or r.get("dissemDT") or ""

                    published = dtparser.parse(dt).astimezone(timezone.utc) if dt else None

                    items.append({"id":uid or title,"title":title or f"NSE Announcement: {comp}","summary":comp,"link":link,"published":published,"company":comp})

                except: 

                    pass

        if DEBUG: print(f"[{datetime.now()}] NSE -> {len(items)}")

        return items

async def fetch_pdf_bytes(session, url):

    try:

        async with session.get(with_cache_buster(url), headers=USER_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:

            resp.raise_for_status()

            content = await resp.read()

            if len(content)/(1024*1024) > MAX_PDF_MB: return None

            ctype = resp.headers.get("Content-Type","").lower()

            if "pdf" in ctype or looks_like_pdf_url(url): return content

            return None

    except Exception as e:

        if DEBUG: print("PDF fetch error:", e)

        return None

# ---------- Processing ----------

async def handle_item(session, source, it):

    uid = it.get("id","")

    if not uid or is_seen(source, uid): return

    title = it.get("title",""); summary = it.get("summary",""); company = it.get("company",""); link = it.get("link","")

    text = " ".join([title, summary, company])

    should = match_filters(text) if ONLY_MATCHING else True

    if should:

        msg = f"📣 <b>{source.upper()} Filing</b>\n"

        if company: msg += f"🏢 <b>{company}</b>\n"

        msg += f"📝 <b>{title}</b>\n"

        if link: msg += f"🔗 <a href=\"{link}\">Open filing</a>\n"

        pub = it.get("published")

        if pub: msg += f"⏱ {fmt_time(pub)}\n"

        await notify_text(msg)

        if SEND_PDF and link and looks_like_pdf_url(link) and not pdf_was_sent(link):

            pdf = await fetch_pdf_bytes(session, link)

            if pdf:

                fname = (company or source).replace(" ","_")[:40]+".pdf"

                await notify_pdf(f"📄 <b>Attachment</b>\n{title}", fname, pdf)

                pdf_mark_sent(link)

    seen_add(source, uid)

# ---------- Loops per source (independent) ----------

async def loop_sebi(session):

    if not ENABLE_SEBI: return

    while True:

        try:

            items = await fetch_sebi(session)

            for it in reversed(items): await handle_item(session, "SEBI", it)

        except Exception as e:

            if DEBUG: print("SEBI loop error:", e)

        await asyncio.sleep(SEBI_INTERVAL)

async def loop_bse(session):

    if not ENABLE_BSE: return

    while True:

        try:

            items = await fetch_bse(session)

            for it in reversed(items): await handle_item(session, "BSE", it)

        except Exception as e:

            if DEBUG: print("BSE loop error:", e)

        await asyncio.sleep(BSE_INTERVAL)

async def loop_nse(session):

    if not ENABLE_NSE: return

    while True:

        try:

            items = await fetch_nse(session)

            for it in reversed(items): await handle_item(session, "NSE", it)

        except Exception as e:

            if DEBUG: print("NSE loop error:", e)

        await asyncio.sleep(NSE_INTERVAL)

# ---------- Main ----------

async def main_async():

    db_init()

    start_msg = "🟢 <b>Instant India Filings Bot (PDF)</b> started. Sources: "

    start_msg += ", ".join(s for s,on in [("SEBI",ENABLE_SEBI),("NSE",ENABLE_NSE),("BSE",ENABLE_BSE)] if on)

    start_msg += ", PDFs ON" if SEND_PDF else ", PDFs OFF"

    await notify_text(start_msg)

    timeout   = aiohttp.ClientTimeout(total=10, connect=3, sock_read=5)

    connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=90)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        tasks = [loop for loop in (

            loop_sebi(session),

            loop_nse(session),

            loop_bse(session),

        ) if loop is not None]

        await asyncio.gather(*tasks)

def main(): asyncio.run(main_async())

if __name__ == "__main__": main()
 
