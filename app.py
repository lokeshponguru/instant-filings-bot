
import os, time, asyncio, aiohttp, feedparser, sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode, urlunparse, parse_qsl
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from dateutil import parser as dtparser
# ───────────────────────── Config ─────────────────────────
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_IDS  = [cid.strip() for cid in os.getenv("CHAT_IDS", "").split(",") if cid.strip()]
# You asked for fastest results ⇒ poll every 3s.
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))
# PRICE-SENSITIVE ONLY (default). You can tweak keywords later in Railway.
DEFAULT_KEYWORDS = (
   "bonus,split,dividend,buyback,preferential,rights,rights issue,fundraising,qip,"
   "order win,contract,award,loi,letter of intent,merger,demerger,acquisition,amalgamation,"
   "board approves,approval,approves,capacity,expansion,stake sale,fpo,ncd,allotment,issue price"
)
KEYWORDS = [k.strip().lower() for k in os.getenv("KEYWORDS", DEFAULT_KEYWORDS).split(",") if k.strip()]
WATCHLIST = [w.strip().lower() for w in os.getenv("WATCHLIST", "").split(",") if w.strip()]
ONLY_MATCHING = os.getenv("ONLY_MATCHING", "1") == "1"   # keep 1 for price-sensitive only
ENABLE_SEBI = os.getenv("ENABLE_SEBI", "1") == "1"
ENABLE_NSE  = os.getenv("ENABLE_NSE",  "1") == "1"
ENABLE_BSE  = os.getenv("ENABLE_BSE",  "1") == "1"
SEBI_RSS = os.getenv("SEBI_RSS", "https://www.sebi.gov.in/sebiweb/rss/corporate-filing.xml")
BSE_RSS  = os.getenv("BSE_RSS",  "https://www.bseindia.com/xml-data/corpfiling/CorpFiling.xml")
NSE_URL  = os.getenv("NSE_URL",  "https://www.nseindia.com/api/corporate-announcements?index=equities")
SEND_PDF    = os.getenv("SEND_PDF", "1") == "1"
MAX_PDF_MB  = float(os.getenv("MAX_PDF_MB", "40"))
DB_PATH     = os.getenv("DB_PATH", "seen.db")
if not BOT_TOKEN: raise SystemExit("ERROR: BOT_TOKEN not set")
if not CHAT_IDS:  raise SystemExit("ERROR: CHAT_IDS empty")
bot = Bot(BOT_TOKEN)
# ───────────────────────── Storage ─────────────────────────
def db_init():
   conn = sqlite3.connect(DB_PATH); c = conn.cursor()
   c.execute("CREATE TABLE IF NOT EXISTS seen (source TEXT, item_id TEXT, PRIMARY KEY(source,item_id))")
   c.execute("CREATE TABLE IF NOT EXISTS sent_pdf (url TEXT PRIMARY KEY)")
   conn.commit(); conn.close()
def is_seen(source, item_id):
   conn = sqlite3.connect(DB_PATH); c = conn.cursor()
   c.execute("SELECT 1 FROM seen WHERE source=? AND item_id=? LIMIT 1", (source, item_id))
   r = c.fetchone(); conn.close(); return r is not None
def seen_add(source, item_id):
   conn = sqlite3.connect(DB_PATH); c = conn.cursor()
   c.execute("INSERT OR IGNORE INTO seen (source,item_id) VALUES (?,?)", (source, item_id))
   conn.commit(); conn.close()
def pdf_was_sent(url):
   conn = sqlite3.connect(DB_PATH); c = conn.cursor()
   c.execute("SELECT 1 FROM sent_pdf WHERE url=? LIMIT 1", (url,))
   r = c.fetchone(); conn.close(); return r is not None
def pdf_mark_sent(url):
   conn = sqlite3.connect(DB_PATH); c = conn.cursor()
   c.execute("INSERT OR IGNORE INTO sent_pdf (url) VALUES (?)", (url,))
   conn.commit(); conn.close()
# ───────────────────────── Helpers ─────────────────────────
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
USER_HEADERS = {
   "User-Agent": UA,
   "Accept": "application/json, text/plain, */*",
   "Accept-Language": "en-US,en;q=0.9",
   "Connection": "keep-alive",
   "Referer": "https://www.nseindia.com/",
}
def with_cache_buster(url: str) -> str:
   """Append a millisecond timestamp to bypass CDN/browser caches (critical for beating NSE website)."""
   try:
       parts = list(urlparse(url))
       qs = dict(parse_qsl(parts[4]))
       qs["_"] = str(int(time.time() * 1000))   # ms timestamp
       parts[4] = urlencode(qs)
       return urlunparse(parts)
   except Exception:
       return url
def looks_like_pdf_url(url: str) -> bool:
   if not url: return False
   s = url.lower()
   return s.endswith(".pdf") or "pdf" in s
def match_filters(text: str) -> bool:
   t = (text or "").lower()
   kw_ok = any(k in t for k in KEYWORDS) if KEYWORDS else True
   wl_ok = any(w in t for w in WATCHLIST) if WATCHLIST else True
   return kw_ok and (wl_ok if WATCHLIST else True)
def fmt_time(dt: datetime) -> str:
   return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
async def notify_text(msg: str):
   for cid in CHAT_IDS:
       try:
           await bot.send_message(chat_id=cid, text=msg, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
       except Exception as e:
           print("Send text error:", e)
async def notify_pdf(caption: str, filename: str, content: bytes):
   for cid in CHAT_IDS:
       try:
           await bot.send_document(chat_id=cid, document=content, filename=filename, caption=caption, parse_mode=ParseMode.HTML)
       except Exception as e:
           print("Send pdf error:", e)
# ───────────────────────── Fetchers (cache-busted) ─────────────────────────
@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
async def fetch_rss(session: aiohttp.ClientSession, url: str):
   headers = {"User-Agent": UA, "Cache-Control": "no-cache, no-store", "Pragma": "no-cache"}
   async with session.get(with_cache_buster(url), headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
       resp.raise_for_status()
       content = await resp.read()
       feed = feedparser.parse(content)
       items = []
       for e in feed.entries:
           uid   = getattr(e, "id", "") or getattr(e, "link", "") or getattr(e, "title", "")
           title = getattr(e, "title", "") or ""
           link  = getattr(e, "link", "") or ""
           summ  = getattr(e, "summary", "") or ""
           published = None
           for key in ("published_parsed", "updated_parsed"):
               v = getattr(e, key, None)
               if v:
                   published = datetime(*v[:6], tzinfo=timezone.utc); break
           items.append({"id": uid, "title": title, "summary": summ, "link": link, "published": published, "company": None})
       return items
fetch_sebi = lambda s: fetch_rss(s, SEBI_RSS)
fetch_bse  = lambda s: fetch_rss(s, BSE_RSS)
@retry(stop=stop_after_attempt(6), wait=wait_exponential(min=1, max=40))
async def fetch_nse(session: aiohttp.ClientSession):
   # Warm cookies to avoid 403s
   async with session.get("https://www.nseindia.com/", headers=USER_HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as resp:
       await resp.read()
   hdrs = dict(USER_HEADERS); hdrs.update({"Cache-Control": "no-cache, no-store", "Pragma": "no-cache"})
   async with session.get(with_cache_buster(NSE_URL), headers=hdrs, timeout=aiohttp.ClientTimeout(total=12)) as resp:
       resp.raise_for_status()
       data = await resp.json(content_type=None)
       records = data.get("data", data) if isinstance(data, dict) else data
       items = []
       if isinstance(records, list):
           for r in records:
               try:
                   uid   = str(r.get("id") or r.get("slno") or r.get("ATTACHMENTNAME") or (r.get("sm_desc","")+r.get("symbol","")))
                   title = r.get("sm_desc") or r.get("HEADLINE") or r.get("desc") or ""
                   comp  = r.get("company") or r.get("companyName") or r.get("symbol") or ""
                   link  = r.get("attchmnt") or r.get("attachment") or r.get("pdfUrl") or r.get("more") or ""
                   if link and link.startswith("/"): link = "https://www.nseindia.com" + link
                   dt    = r.get("dt") or r.get("attachmentDt") or r.get("dissemDT") or ""
                   published = dtparser.parse(dt).astimezone(timezone.utc) if dt else None
                   items.append({"id": uid or title, "title": title or f"NSE Announcement: {comp}", "summary": comp, "link": link, "published": published, "company": comp})
               except Exception:
                   pass
       return items
async def fetch_pdf_bytes(session: aiohttp.ClientSession, url: str) -> bytes | None:
   try:
       async with session.get(with_cache_buster(url), headers=USER_HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
           resp.raise_for_status()
           content = await resp.read()
           if len(content)/(1024*1024) > MAX_PDF_MB: return None
           ctype = resp.headers.get("Content-Type", "").lower()
           if "pdf" in ctype or looks_like_pdf_url(url): return content
           return None
   except Exception as e:
       print("PDF fetch error:", e); return None
# ───────────────────────── Processing ─────────────────────────
async def handle_item(session, source: str, it: dict):
   uid = it.get("id") or ""
   if not uid or is_seen(source, uid): return
   title   = it.get("title", "") or ""
   summary = it.get("summary", "") or ""
   company = it.get("company", "") or ""
   link    = it.get("link", "") or ""
   text    = " ".join([title, summary, company])
   should_alert = match_filters(text) if ONLY_MATCHING else True
   if should_alert:
       msg = f"📣 <b>{source.upper()} Filing</b>\n"
       if company: msg += f"🏢 <b>{company}</b>\n"
       msg += f"📝 <b>{title}</b>\n"
       if link:   msg += f"🔗 <a href=\"{link}\">Open filing</a>\n"
       pub = it.get("published")
       if pub:    msg += f"⏱ {fmt_time(pub)}\n"
       await notify_text(msg)
       # Try to push PDF immediately
       if SEND_PDF and link and looks_like_pdf_url(link) and not pdf_was_sent(link):
           pdf = await fetch_pdf_bytes(session, link)
           if pdf:
               fname = (company or source).replace(" ", "_")[:40] + ".pdf"
               await notify_pdf(f"📄 <b>Attachment</b>\n{title}", fname, pdf)
               pdf_mark_sent(link)
   seen_add(source, uid)
# ───────────────────────── Main loop ─────────────────────────
async def poll_loop():
   db_init()
   start_msg = "🟢 <b>Instant India Filings Bot (PDF)</b> started. Sources: "
   start_msg += ", ".join(s for s, on in [("SEBI", ENABLE_SEBI), ("NSE", ENABLE_NSE), ("BSE", ENABLE_BSE)] if on)
   start_msg += ", PDFs ON" if SEND_PDF else ", PDFs OFF"
   await notify_text(start_msg)
   # Snappy networking for low latency
   timeout   = aiohttp.ClientTimeout(total=15, connect=5, sock_read=7)
   connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=60)
   async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
       while True:
           try:
               tasks = []
               if ENABLE_SEBI: tasks.append(fetch_sebi(session))
               if ENABLE_NSE:  tasks.append(fetch_nse(session))
               if ENABLE_BSE:  tasks.append(fetch_bse(session))
               results = await asyncio.gather(*tasks, return_exceptions=True)
               idx = 0
               if ENABLE_SEBI:
                   sebires = results[idx]; idx += 1
                   if not isinstance(sebires, Exception):
                       for it in reversed(sebires): await handle_item(session, "SEBI", it)
               if ENABLE_NSE:
                   nseres = results[idx]; idx += 1
                   if not isinstance(nseres, Exception):
                       for it in reversed(nseres): await handle_item(session, "NSE", it)
               if ENABLE_BSE:
                   bseres = results[idx]; idx += 1
                   if not isinstance(bseres, Exception):
                       for it in reversed(bseres): await handle_item(session, "BSE", it)
           except Exception as e:
               print("Loop error:", e)
           await asyncio.sleep(POLL_INTERVAL_SEC)
def main(): asyncio.run(poll_loop())
if __name__ == "__main__": main()
