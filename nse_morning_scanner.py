#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NSE Morning Scanner
===================
Runs daily at 8am via Windows Task Scheduler.

DATA SOURCES (verified working as of 2026-03):
  - NSE derivatives bhav copy    → live F&O stock list
  - NSE corporate announcements  → all other data (1000+ rows/day)
    Sliced into:
      • "Spurt in Volume"         → bulk/unusual-activity flag
      • Regulation 7/8/29 text   → SAST / insider-trading disclosures
      • desc keywords             → results, dividends, buybacks, capex

NSE's bulk-deal and block-deal historical APIs (plus all BSE APIs)
are blocked by Akamai bot protection and cannot be called from Python
requests or headless browsers. The corporate announcements endpoint
is the one reliable door into NSE data.
"""

import os
import sys
import re
import json
import time
import logging
import smtplib
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import pandas as pd

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LOG_DIR    = os.path.join(BASE_DIR, "scanner_logs")
CONFIG_F   = os.path.join(BASE_DIR, "scanner_config.json")
TODAY_SLUG = datetime.today().strftime("%Y%m%d")

os.makedirs(LOG_DIR, exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"scanner_{TODAY_SLUG}.log"),
                            encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("nse_scanner")


# ─── Config ───────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "gmail_user":         "your_email@gmail.com",
    "gmail_app_password": "xxxx xxxx xxxx xxxx",
    "recipients":         ["recipient@example.com"],
    "thresholds": {
        "capex_to_mcap_pct":          50.0,
        "spurt_repeat_days":           7,
        "announcement_lookback_days":  3,
    },
}


def load_config():
    if not os.path.exists(CONFIG_F):
        with open(CONFIG_F, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        log.error("Config created at %s — fill in credentials then re-run.", CONFIG_F)
        sys.exit(1)
    with open(CONFIG_F) as f:
        cfg = json.load(f)
    if cfg.get("gmail_user") == "your_email@gmail.com":
        log.error("Edit scanner_config.json with real Gmail credentials.")
        sys.exit(1)
    for k, v in DEFAULT_CONFIG["thresholds"].items():
        cfg.setdefault("thresholds", {}).setdefault(k, v)
    return cfg


# ─── NSE Session ──────────────────────────────────────────────────────────────
BASE_HEADERS = {
    "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    # Deliberately omit 'br' — requests has no native Brotli decompressor.
    # Without 'br' advertised, NSE falls back to gzip which requests handles.
    "Accept-Encoding": "gzip, deflate",
    "Referer":         "https://www.nseindia.com",
    "Connection":      "keep-alive",
}


WARMUP_URLS = [
    "https://www.nseindia.com/market-data/live-equity-market",
    "https://www.nseindia.com/api/allIndices",
]


def get_nse_session():
    """
    Warm up the NSE session without hitting the homepage
    (which can return 403 after repeated requests from the same IP).
    The API endpoints only need the nsit cookie, which is set by the
    live-equity-market page.
    """
    s = requests.Session()
    s.headers.update(BASE_HEADERS)
    for url in WARMUP_URLS:
        try:
            r = s.get(url, timeout=15)
            log.debug("Warmup %s → %s", url.split("/")[-1], r.status_code)
            time.sleep(1.2)
        except Exception as exc:
            log.warning("Warmup partial failure (%s): %s", url, exc)
    log.info("NSE session ready (cookies: %s)", list(s.cookies.keys()))
    return s


def nse_get(session, url, label="request"):
    """GET with retry on transient errors."""
    for attempt in range(3):
        try:
            r = session.get(url, timeout=25)
            return r
        except requests.RequestException as exc:
            log.warning("%s attempt %d failed: %s", label, attempt + 1, exc)
            time.sleep(2)
    return None


# ─── Helper: dates ────────────────────────────────────────────────────────────
def last_trading_day():
    d = datetime.today()
    if d.hour < 9:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def fmt_nse(dt):
    return dt.strftime("%d-%m-%Y")


# ─── Step 1: F&O Stock List ───────────────────────────────────────────────────
FO_CACHE_F = os.path.join(BASE_DIR, "fo_stocks_cache.json")


def _save_fo_cache(fo_stocks, as_of):
    try:
        with open(FO_CACHE_F, "w") as f:
            json.dump({"as_of": as_of, "symbols": sorted(fo_stocks)}, f)
        log.info("F&O cache saved (%d stocks, %s)", len(fo_stocks), as_of)
    except Exception as exc:
        log.warning("Could not save F&O cache: %s", exc)


def _load_fo_cache():
    try:
        with open(FO_CACHE_F) as f:
            data = json.load(f)
        symbols = set(data.get("symbols", []))
        as_of   = data.get("as_of", "?")
        if symbols:
            log.info("F&O cache loaded: %d stocks (cached as of %s)", len(symbols), as_of)
            return symbols
    except Exception:
        pass
    return set()


def get_fo_stocks(session):
    """
    Fetches the live F&O stock universe.

    Priority:
      1. NSE equity-stockIndices API  (Securities in F&O index) — fastest, most reliable
      2. NSE derivatives bhav copy ZIP — fallback
      3. Disk cache — last resort
    """
    # ── 1. Primary: Securities in F&O index API ────────────────────────────────
    url = ("https://www.nseindia.com/api/equity-stockIndices"
           "?index=SECURITIES%20IN%20F%26O")
    r = nse_get(session, url, label="F&O index API")
    if r and r.status_code == 200:
        try:
            data = r.json().get("data", [])
            fos  = set(
                str(x.get("symbol", "")).strip().upper()
                for x in data
                if x.get("symbol")
            )
            if fos:
                log.info("F&O universe: %d stocks (via Securities-in-F&O index)", len(fos))
                _save_fo_cache(fos, datetime.today().strftime("%d%b%Y").upper())
                return fos
        except Exception as exc:
            log.warning("F&O index API parse error: %s", exc)

    # ── 2. Fallback: Derivatives bhav copy ────────────────────────────────────
    log.warning("F&O index API failed — trying derivatives bhav copy.")
    for i in range(7):
        d = datetime.today() - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        date_str = d.strftime("%d%b%Y").upper()
        year     = d.strftime("%Y")
        month    = d.strftime("%b").upper()
        url2 = (f"https://www.nseindia.com/content/historical/DERIVATIVES/"
                f"{year}/{month}/fo{date_str}bhav.csv.zip")
        try:
            r2 = session.get(url2, timeout=25)
            if r2.status_code == 200 and r2.content[:2] == b"PK":
                z   = ZipFile(BytesIO(r2.content))
                df  = pd.read_csv(z.open(z.namelist()[0]))
                fos = set(
                    df[df["INSTRUMENT"] == "FUTSTK"]["SYMBOL"]
                    .str.strip().str.upper().unique()
                )
                log.info("F&O universe: %d stocks (bhav copy %s)", len(fos), date_str)
                _save_fo_cache(fos, date_str)
                return fos
        except Exception as exc:
            log.warning("Bhav copy attempt %d: %s", i, exc)
        time.sleep(1)

    # ── 3. Last resort: disk cache ────────────────────────────────────────────
    cached = _load_fo_cache()
    if cached:
        log.warning("Using cached F&O list (%d stocks).", len(cached))
        return cached

    log.error("No F&O stock list available.")
    return set()


# ─── Step 2: All Announcements (single call, classify locally) ────────────────

# ── 2a: Classification rules ──────────────────────────────────────────────────

# Matches in attchmntText (lowercased) → SAST / insider disclosure
SAST_TEXT_KWS = [
    "regulation 29",           # SAST Reg 29 (substantial acquisition)
    "substantial acquisition", # explicit mention
    "sebi (sast)",
    "regulation 7(",           # insider trading Reg 7
    "regulation 8(",           # insider trading Reg 8
    "regulation 13(",          # insider trading Reg 13
    "sebi (prohibition of insider trading)",
    "insider trading regulations",
]

# Promoter/insider context keywords (any of these in text = promoter activity)
PROMOTER_KWS = [
    "promoter", "promoter group", "person acting in concert",
    "key managerial", "director", "chairman", "managing director",
    "whole time director", "pac",
]

# NSE desc field values for volume-spurt announcements
VOLUME_SPURT_DESC = "spurt in volume"

# Corporate action categories (checked against desc + attchmntText)
CORP_ACTION_MAP = {
    "results":       ["financial results", "quarterly results", "annual results",
                      "unaudited results", "audited results", "q1", "q2", "q3", "q4"],
    "board_meeting": ["board meeting", "outcome of board meeting", "board of directors"],
    "dividend":      ["dividend", "interim dividend", "final dividend", "special dividend"],
    "buyback":       ["buyback", "buy-back", "buy back", "share repurchase",
                      "buyback of shares", "repurchase of shares"],
    "capex":         ["capex", "capital expenditure", "expansion plan", "new plant",
                      "greenfield", "brownfield", "capacity addition", "new facility",
                      "setting up of", "commissioning"],
}


def _classify_row(row):
    """
    Returns one of: 'volume_spurt' | 'sast' | category-key | None
    Checks desc field + attchmntText.
    """
    desc = str(row.get("desc", "") or "").lower().strip()
    text = str(row.get("attchmnttext", "") or "").lower()
    combined = desc + " " + text

    # Volume spurt (NSE's own flag)
    if desc == VOLUME_SPURT_DESC:
        return "volume_spurt"

    # SAST / insider trading disclosure
    if any(kw in combined for kw in SAST_TEXT_KWS):
        return "sast"

    # Corporate actions — check desc first (fast), then full text
    for cat, kws in CORP_ACTION_MAP.items():
        if any(kw in combined for kw in kws):
            return cat

    return None


def get_all_announcements(session, days_back=5):
    """
    Single NSE corporate announcements call covering the last `days_back` days.
    Returns a classified DataFrame with a 'category' column.
    """
    today  = datetime.today()
    from_d = fmt_nse(today - timedelta(days=days_back))
    to_d   = fmt_nse(today)

    url = (f"https://www.nseindia.com/api/corporate-announcements"
           f"?index=equities&from_date={from_d}&to_date={to_d}")
    r = nse_get(session, url, label="corporate announcements")
    if r is None or r.status_code != 200:
        log.error("Corporate announcements fetch failed (status %s).",
                  r.status_code if r else "no response")
        return pd.DataFrame()

    try:
        rows = r.json()
        rows = rows if isinstance(rows, list) else rows.get("data", [])
    except Exception as exc:
        log.error("Corporate announcements JSON parse: %s", exc)
        return pd.DataFrame()

    log.info("Corporate announcements: %d total rows", len(rows))
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Normalise column names to snake_case
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    df["category"] = df.apply(_classify_row, axis=1)

    counts = df["category"].value_counts().to_dict()
    log.info("Classified: %s", counts)
    return df


# ─── Step 3: Slice into sub-datasets ─────────────────────────────────────────
def slice_announcements(df):
    """
    Returns four DataFrames from the master announcements table:
      volume_spurts  — NSE-flagged unusual volume events
      sast           — SAST / insider-trading disclosures
      corp_actions   — results, dividends, buybacks, board meetings, capex
    """
    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    volume_spurts = df[df["category"] == "volume_spurt"].copy()
    sast          = df[df["category"] == "sast"].copy()
    corp_actions  = df[df["category"].isin(CORP_ACTION_MAP.keys())].copy()

    return volume_spurts, sast, corp_actions


# ─── Step 4: Analysis ─────────────────────────────────────────────────────────
def _flt(val):
    try:
        return float(str(val).replace(",", "").replace("%", "").strip())
    except Exception:
        return 0.0


def analyze(volume_spurts, sast_df, corp_df, fo_stocks, cfg):
    """
    Returns
    -------
    fo_section : dict   — rows for email Section 1 (F&O universe)
    flags      : list   — dicts for email Section 2 (100x flags)
    """
    thresh = cfg["thresholds"]

    fo_section = {
        "volume_spurts":  [],
        "sast":           [],
        "corp_actions":   [],
    }
    flags = []

    # ── A. Volume Spurts ──────────────────────────────────────────────────────
    if not volume_spurts.empty:
        spurt_by_sym = defaultdict(list)

        for _, row in volume_spurts.iterrows():
            sym  = str(row.get("symbol", "")).strip().upper()
            name = str(row.get("sm_name", "")).strip()
            dt   = str(row.get("an_dt", ""))
            txt  = str(row.get("attchmnttext", ""))[:200]

            if sym in fo_stocks:
                fo_section["volume_spurts"].append(row.to_dict())

            spurt_by_sym[sym].append(dt)

        # Flag: repeated volume spurt on same stock in window
        for sym, dates in spurt_by_sym.items():
            if len(dates) >= 2:
                flags.append({
                    "flag":   "REPEATED_VOLUME_SPURT",
                    "symbol": sym,
                    "detail": (f"NSE flagged {len(dates)} 'Spurt in Volume' alerts "
                               f"on this stock in {thresh['spurt_repeat_days']} days"),
                    "dates":  dates,
                })

    # ── B. SAST / Insider Disclosures ─────────────────────────────────────────
    if not sast_df.empty:
        for _, row in sast_df.iterrows():
            sym  = str(row.get("symbol", "")).strip().upper()
            text = str(row.get("attchmnttext", "") or "").lower()

            if sym in fo_stocks:
                fo_section["sast"].append(row.to_dict())

            # Detect promoter / insider activity from text
            is_promoter = any(kw in text for kw in PROMOTER_KWS)
            is_reg29    = "regulation 29" in text or "substantial acquisition" in text
            is_insider  = any(kw in text for kw in
                              ["regulation 7(", "regulation 8(", "insider trading"])

            # Try to extract a percentage from the text
            pct_match = re.search(
                r"(\d+\.?\d*)\s*%\s*(?:of|shares|equity|voting)", text
            )
            pct_str = f"{pct_match.group(1)}%" if pct_match else "disclosed"

            if is_promoter and is_reg29:
                flags.append({
                    "flag":   "PROMOTER_SAST",
                    "symbol": sym,
                    "detail": (f"Promoter/PAC filed Reg-29 SAST disclosure "
                               f"(stake: {pct_str})"),
                    "data":   row.to_dict(),
                })
            elif is_insider:
                flags.append({
                    "flag":   "INSIDER_TRADING_DISCLOSURE",
                    "symbol": sym,
                    "detail": (f"Insider trading disclosure filed "
                               f"(stake: {pct_str})"),
                    "data":   row.to_dict(),
                })
            elif is_reg29:
                flags.append({
                    "flag":   "SAST_DISCLOSURE",
                    "symbol": sym,
                    "detail": (f"SAST Reg-29 substantial acquisition disclosure "
                               f"(stake: {pct_str})"),
                    "data":   row.to_dict(),
                })

    # ── C. Corporate Actions ──────────────────────────────────────────────────
    if not corp_df.empty:
        for _, row in corp_df.iterrows():
            sym = str(row.get("symbol", "")).strip().upper()

            if sym in fo_stocks:
                fo_section["corp_actions"].append(row.to_dict())

            cat  = row.get("category", "")
            text = str(row.get("attchmnttext", "") or "").lower()

            # Flag: large capex — try to extract ₹ amount and compare to MCap
            if cat == "capex":
                amounts = re.findall(
                    r"(?:rs\.?|inr|₹)\s*([\d,]+)\s*(?:cr(?:ore)?s?)",
                    text
                )
                if amounts:
                    try:
                        capex_cr = _flt(amounts[0])
                        flags.append({
                            "flag":   "LARGE_CAPEX",
                            "symbol": sym,
                            "detail": (f"Capex announcement: ₹{capex_cr:,.0f} Cr "
                                       "(verify vs market cap)"),
                            "data":   row.to_dict(),
                        })
                    except Exception:
                        pass

            # Flag: buyback announcement
            if cat == "buyback":
                flags.append({
                    "flag":   "BUYBACK",
                    "symbol": sym,
                    "detail": "Buyback / share repurchase announced",
                    "data":   row.to_dict(),
                })

    log.info(
        "Analysis: F&O volume_spurts=%d  sast=%d  corp=%d  | flags=%d",
        len(fo_section["volume_spurts"]),
        len(fo_section["sast"]),
        len(fo_section["corp_actions"]),
        len(flags),
    )
    return fo_section, flags


# ─── Step 5: HTML Email ───────────────────────────────────────────────────────
CSS = """\
<style>
  body{font-family:'Segoe UI',Arial,sans-serif;font-size:13px;color:#222;
       background:#f0f2f5;margin:0;padding:16px}
  .wrap{max-width:960px;margin:0 auto;background:#fff;border-radius:10px;
        overflow:hidden;box-shadow:0 3px 12px rgba(0,0,0,.12)}
  .hdr{background:linear-gradient(135deg,#1a237e,#283593);color:#fff;padding:22px 28px}
  .hdr h1{margin:0;font-size:20px;font-weight:700}
  .hdr p{margin:5px 0 0;opacity:.75;font-size:12px}
  .sec{padding:22px 28px;border-bottom:1px solid #e8eaf0}
  .sec h2{margin:0 0 6px;font-size:15px;color:#1a237e;border-left:4px solid #3949ab;padding-left:10px}
  .sec h3{margin:16px 0 6px;font-size:12px;color:#555;text-transform:uppercase;letter-spacing:.6px}
  .badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600}
  .b-fo{background:#e3f2fd;color:#0d47a1}
  .b-sast{background:#fce4ec;color:#880e4f}
  .b-vol{background:#fff8e1;color:#e65100}
  .b-corp{background:#e8f5e9;color:#1b5e20}
  .flag-card{background:#fffde7;border-left:4px solid #f9a825;border-radius:6px;padding:12px 16px;margin:8px 0}
  .ft{font-size:11px;font-weight:700;text-transform:uppercase;color:#e65100}
  .fs{font-size:15px;font-weight:700;color:#1a237e;margin:3px 0}
  .fd{font-size:12px;color:#555}
  table.dt{border-collapse:collapse;width:100%;font-size:11px;margin:8px 0}
  table.dt th{background:#e8eaf6;color:#1a237e;text-align:left;padding:7px 9px;font-weight:600;white-space:nowrap}
  table.dt td{padding:6px 9px;border-bottom:1px solid #f0f0f0;vertical-align:top;word-break:break-word;max-width:320px}
  table.dt tr:nth-child(even) td{background:#f9fafb}
  .empty{color:#aaa;font-style:italic;font-size:12px;padding:8px 0}
  .ok{padding:12px 16px;background:#e8f5e9;border-radius:6px;color:#2e7d32;font-size:12px}
  .ftr{padding:14px 28px;background:#f5f5f5;font-size:11px;color:#999}
  .note{font-size:11px;color:#888;margin:4px 0 10px;font-style:italic}
</style>"""

FLAG_META = {
    "REPEATED_VOLUME_SPURT":      ("Repeated Volume Spurt",      "#e91e63"),
    "PROMOTER_SAST":              ("Promoter SAST Filing",        "#43a047"),
    "INSIDER_TRADING_DISCLOSURE": ("Insider Trading Disclosure",  "#7b1fa2"),
    "SAST_DISCLOSURE":            ("SAST Disclosure",             "#0277bd"),
    "LARGE_CAPEX":                ("Large Capex Announced",       "#ef6c00"),
    "BUYBACK":                    ("Buyback Announced",           "#00796b"),
}

SPURT_COLS   = ["symbol", "sm_name", "an_dt", "attchmnttext"]
SAST_COLS    = ["symbol", "sm_name", "desc", "an_dt", "attchmnttext"]
CORP_COLS    = ["symbol", "sm_name", "category", "desc", "an_dt", "attchmnttext"]


def _trim_text(df, col="attchmnttext", maxlen=180):
    """Truncate a long-text column for table display."""
    if col in df.columns:
        df = df.copy()
        df[col] = df[col].astype(str).str[:maxlen]
    return df


def _to_table(rows, prefer_cols=None):
    if not rows:
        return '<div class="empty">No data for this category today.</div>'
    df = pd.DataFrame(rows)
    if prefer_cols:
        keep = [c for c in prefer_cols if c in df.columns]
        df   = df[keep] if keep else df.iloc[:, :8]
    else:
        df = df.iloc[:, :8]
    df = _trim_text(df)
    df = df.head(50)
    return df.to_html(index=False, border=0, classes="dt", na_rep="—")


def build_html_email(fo_section, flags, date_str):
    now = datetime.now().strftime("%d %b %Y  %H:%M")

    def sub(label, rows, badge_cls, cols):
        return (f'<h3><span class="badge {badge_cls}">{label} ({len(rows)})</span></h3>'
                + _to_table(rows, cols))

    s1 = f"""
    <div class="sec">
      <h2>&#9312; F&amp;O Universe — Today's Signals</h2>
      <p class="note">Filtered to F&amp;O-eligible stocks only. Source: NSE corporate announcements.</p>
      {sub("Volume Spurt Alerts (NSE-flagged)", fo_section["volume_spurts"], "b-vol", SPURT_COLS)}
      {sub("SAST / Insider Disclosures",         fo_section["sast"],          "b-sast", SAST_COLS)}
      {sub("Corporate Actions",                  fo_section["corp_actions"],  "b-corp", CORP_COLS)}
    </div>"""

    if flags:
        cards = ""
        for f in flags:
            label, color = FLAG_META.get(f["flag"], (f["flag"], "#555"))
            sym   = f.get("symbol", "—")
            det   = f.get("detail", "")
            dates = f.get("dates", [])
            extra = ""
            if dates:
                extra = f'<div class="fd" style="margin-top:4px;font-size:11px;color:#888">{" | ".join(dates)}</div>'
            cards += f"""
            <div class="flag-card" style="border-left-color:{color}">
              <div class="ft">{label}</div>
              <div class="fs">{sym}</div>
              <div class="fd">{det}</div>
              {extra}
            </div>"""
        s2_body = cards
    else:
        s2_body = '<div class="ok">&#10004; No 100x flags triggered today.</div>'

    s2 = f"""
    <div class="sec">
      <h2>&#9313; 100x Opportunity Flags — All NSE Listed Stocks</h2>
      <p class="note">
        Triggers: Repeated NSE volume-spurt alerts &nbsp;|&nbsp;
        Promoter / PAC Reg-29 SAST filing &nbsp;|&nbsp;
        Insider trading disclosure &nbsp;|&nbsp;
        Capex announcement &nbsp;|&nbsp;
        Buyback announcement
      </p>
      {s2_body}
    </div>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">{CSS}</head>
<body>
<div class="wrap">
  <div class="hdr">
    <h1>NSE Morning Scanner</h1>
    <p>Generated: {now} &nbsp;|&nbsp; Data period: {date_str}</p>
  </div>
  {s1}
  {s2}
  <div class="ftr">
    Auto-generated &bull; Source: NSE corporate announcements API &bull;
    Not investment advice &bull; Verify before acting.
  </div>
</div>
</body></html>"""


# ─── Step 6: Send Email ───────────────────────────────────────────────────────
def send_email(html_body, cfg, subject):
    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["gmail_user"]
    msg["To"]      = ", ".join(cfg["recipients"])
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(cfg["gmail_user"], cfg["gmail_app_password"])
            smtp.sendmail(cfg["gmail_user"], cfg["recipients"], msg.as_string())
        log.info("Email sent to: %s", cfg["recipients"])
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("Gmail auth failed — check gmail_app_password in scanner_config.json.")
    except Exception as exc:
        log.error("Email send failed: %s", exc)
    return False


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("NSE Morning Scanner  (%s)", datetime.now().strftime("%d %b %Y %H:%M"))

    cfg     = load_config()
    session = get_nse_session()

    # 1. Live F&O stock list
    log.info("--- Step 1: F&O universe")
    fo_stocks = get_fo_stocks(session)

    # Re-warm session before announcements — the F&O fetch loop can degrade cookies
    log.info("Re-warming session before announcements fetch.")
    try:
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
        time.sleep(1.5)
    except Exception:
        pass

    # 2. All announcements (single API call)
    days_back = cfg["thresholds"].get("announcement_lookback_days", 3)
    log.info("--- Step 2: Corporate announcements (last %d days)", days_back)
    all_ann = get_all_announcements(session, days_back=days_back)

    # 3. Slice into categories
    volume_spurts, sast_df, corp_df = slice_announcements(all_ann)
    log.info("Sliced: volume_spurts=%d  sast=%d  corp_actions=%d",
             len(volume_spurts), len(sast_df), len(corp_df))

    # 4. Analyze
    log.info("--- Step 3: Analysis")
    fo_section, flags = analyze(volume_spurts, sast_df, corp_df, fo_stocks, cfg)

    # 5. Build email
    ltd      = last_trading_day()
    date_str = ltd.strftime("%d %b %Y  (%A)")
    n_flags  = len(flags)
    n_spurt  = len(fo_section["volume_spurts"])
    subject  = (f"NSE Scanner {ltd.strftime('%d %b')} — "
                f"{n_flags} flag(s) | {n_spurt} F&O volume spurts")
    html = build_html_email(fo_section, flags, date_str)

    # 6. Save preview
    preview = os.path.join(LOG_DIR, f"preview_{TODAY_SLUG}.html")
    with open(preview, "w", encoding="utf-8") as fh:
        fh.write(html)
    log.info("Preview saved: %s", preview)

    # 7. Send
    log.info("--- Step 4: Sending email")
    send_email(html, cfg, subject)

    log.info("Done.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
