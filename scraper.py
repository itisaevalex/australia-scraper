"""
scraper.py — ASX Announcements Scraper

Production scraper for ASX (Australian Securities Exchange) announcements.
Crawls announcements.do (per-company) and prevBusDayAnns.do (all-company),
resolves two-step PDF URLs, downloads PDFs in parallel, and caches everything
in SQLite.

Usage:
    python scraper.py crawl [--tickers BHP,CBA] [--max-companies 10]
                            [--period M6] [--year 2025]
                            [--download] [--workers 5] [--all-day]
    python scraper.py monitor [--interval 300] [--download]
    python scraper.py export [--output filings.json]
                             [--ticker BHP] [--downloaded-only]
    python scraper.py stats
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

BASE_HEADERS: dict[str, str] = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

ASX_BASE = "https://www.asx.com.au"
ANNOUNCEMENTS_URL = f"{ASX_BASE}/asx/v2/statistics/announcements.do"
PREV_BUS_DAY_URL = f"{ASX_BASE}/asx/v2/statistics/prevBusDayAnns.do"
DISPLAY_ANNOUNCEMENT_URL = f"{ASX_BASE}/asx/v2/statistics/displayAnnouncement.do"
MARKIT_DIRECTORY_URL = (
    "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file"
)

VALID_PERIODS = ("T", "P", "W", "M", "M3", "M6")
TICKER_RE = re.compile(r"^[A-Z0-9]{2,6}$")
IDS_ID_RE = re.compile(r"^[A-Za-z0-9]{1,64}$")
DB_PATH = Path("filings_cache.db")
DOCUMENTS_DIR = Path("documents")
FETCH_DELAY = 0.3  # seconds between page fetches

try:
    import lxml  # noqa: F401
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("asx_scraper")

# ---------------------------------------------------------------------------
# Data models (frozen dataclasses for immutability)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Announcement:
    ids_id: str
    asx_code: str
    date: str
    time: str | None
    headline: str
    pdf_url: str | None
    file_size: str | None
    num_pages: int | None
    price_sensitive: bool


@dataclass(frozen=True)
class CrawlResult:
    crawl_type: str
    ticker: str | None
    period: str | None
    announcements_found: int
    announcements_new: int
    started_at: str
    completed_at: str
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS announcements (
    ids_id           TEXT PRIMARY KEY,
    asx_code         TEXT NOT NULL,
    date             TEXT NOT NULL,
    time             TEXT,
    headline         TEXT NOT NULL,
    pdf_url          TEXT,
    direct_pdf_url   TEXT,
    file_size        TEXT,
    num_pages        INTEGER,
    price_sensitive  BOOLEAN DEFAULT FALSE,
    downloaded       BOOLEAN DEFAULT FALSE,
    download_path    TEXT,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crawl_log (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_type           TEXT NOT NULL,
    ticker               TEXT,
    period               TEXT,
    announcements_found  INTEGER,
    announcements_new    INTEGER,
    started_at           TEXT NOT NULL,
    completed_at         TEXT
);
"""


def get_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite cache and ensure the schema exists."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def upsert_announcement(conn: sqlite3.Connection, ann: Announcement) -> bool:
    """Insert announcement if not already present. Returns True when new."""
    conn.execute(
        """
        INSERT OR IGNORE INTO announcements
            (ids_id, asx_code, date, time, headline, pdf_url, file_size,
             num_pages, price_sensitive)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ann.ids_id,
            ann.asx_code,
            ann.date,
            ann.time,
            ann.headline,
            ann.pdf_url,
            ann.file_size,
            ann.num_pages,
            ann.price_sensitive,
        ),
    )
    inserted = conn.total_changes > 0
    conn.commit()
    return inserted


def mark_downloaded(
    conn: sqlite3.Connection, ids_id: str, direct_url: str, path: str
) -> None:
    """Update announcement row after a successful PDF download."""
    conn.execute(
        """
        UPDATE announcements
        SET downloaded = TRUE, direct_pdf_url = ?, download_path = ?
        WHERE ids_id = ?
        """,
        (direct_url, path, ids_id),
    )
    conn.commit()


def log_crawl(conn: sqlite3.Connection, result: CrawlResult) -> None:
    """Write a crawl_log entry."""
    conn.execute(
        """
        INSERT INTO crawl_log
            (crawl_type, ticker, period, announcements_found,
             announcements_new, started_at, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.crawl_type,
            result.ticker,
            result.period,
            result.announcements_found,
            result.announcements_new,
            result.started_at,
            result.completed_at,
        ),
    )
    conn.commit()


def fetch_undownloaded(conn: sqlite3.Connection, ticker: str | None = None) -> list[dict]:
    """Return rows that have a pdf_url but have not been downloaded yet."""
    if ticker:
        cur = conn.execute(
            "SELECT * FROM announcements WHERE downloaded = FALSE AND pdf_url IS NOT NULL "
            "AND asx_code = ?",
            (ticker,),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM announcements WHERE downloaded = FALSE AND pdf_url IS NOT NULL"
        )
    return [dict(row) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def make_session() -> requests.Session:
    """Return a fresh requests.Session pre-configured with browser headers."""
    sess = requests.Session()
    sess.headers.update(BASE_HEADERS)
    return sess


def safe_get(
    session: requests.Session,
    url: str,
    params: dict[str, str] | None = None,
    retries: int = 3,
    timeout: int = 30,
) -> requests.Response | None:
    """GET with retry logic. Returns None on unrecoverable failure."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            log.warning("HTTP %s on %s (attempt %d/%d)", exc.response.status_code, url, attempt, retries)
            if exc.response.status_code in (403, 404, 410):
                return None
        except requests.RequestException as exc:
            log.warning("Request error on %s (attempt %d/%d): %s", url, attempt, retries, exc)
        if attempt < retries:
            time.sleep(attempt * 1.5)
    return None


# ---------------------------------------------------------------------------
# HTML parsers
# ---------------------------------------------------------------------------


def _clean(text: str | None) -> str:
    """Strip and collapse whitespace."""
    if text is None:
        return ""
    return " ".join(text.split())


def _extract_ids_id(href: str) -> str | None:
    """Extract the idsId query parameter from a displayAnnouncement URL."""
    parsed = urlparse(href)
    params = parse_qs(parsed.query)
    ids = params.get("idsId")
    return ids[0] if ids else None


def _is_price_sensitive(td: Any) -> bool:
    """Return True if the pricesens cell contains the icon image."""
    if td is None:
        return False
    if td.find("img", class_="pricesens"):
        return True
    return td.find("img", alt=re.compile(r"asterix|price", re.I)) is not None


def _parse_headline_td(td: Any) -> dict[str, Any]:
    """Parse the headline <td>, returning headline, pdf_url, ids_id, file_size, num_pages."""
    result: dict[str, Any] = {
        "headline": None,
        "pdf_url": None,
        "ids_id": None,
        "file_size": None,
        "num_pages": None,
    }
    anchor = td.find("a", href=re.compile(r"displayAnnouncement\.do"))
    if anchor is None:
        return result

    href = anchor.get("href", "")
    result["pdf_url"] = (ASX_BASE + href) if href.startswith("/") else href
    result["ids_id"] = _extract_ids_id(href)

    for node in anchor.children:
        raw = str(node) if not hasattr(node, "get_text") else ""
        text = _clean(raw or getattr(node, "string", None) or "")
        if text:
            result["headline"] = text
            break

    page_span = anchor.find("span", class_="page")
    if page_span:
        match = re.search(r"(\d+)", page_span.get_text())
        result["num_pages"] = int(match.group(1)) if match else None

    size_span = anchor.find("span", class_="filesize")
    if size_span:
        result["file_size"] = _clean(size_span.get_text())

    return result


def parse_announcements_do(html: str) -> tuple[list[Announcement], list[str]]:
    """
    Parse /asx/v2/statistics/announcements.do HTML.

    Column order: Date/Time | Price sens. | Headline
    ASX code comes from the page <h2> header.
    """
    soup = BeautifulSoup(html, BS_PARSER)
    announcements: list[Announcement] = []
    errors: list[str] = []

    asx_code: str | None = None
    for h2 in soup.find_all("h2"):
        match = re.search(r"\(([A-Z0-9]{2,6})\)", h2.get_text())
        if match:
            asx_code = match.group(1)
            break

    ann_data = soup.find("announcement_data")
    if ann_data is None:
        errors.append("announcements.do: <announcement_data> tag not found")
        return announcements, errors

    table = ann_data.find("table")
    if table is None:
        errors.append("announcements.do: no <table> inside <announcement_data>")
        return announcements, errors

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    for i, row in enumerate(rows):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        try:
            date_raw = _clean(tds[0].find(string=True, recursive=False))
            time_span = tds[0].find("span", class_="dates-time")
            time_val = _clean(time_span.get_text()) if time_span else None
            price_sens = _is_price_sensitive(tds[1])
            hl = _parse_headline_td(tds[2])

            if not hl["ids_id"] or not hl["headline"]:
                continue

            announcements.append(
                Announcement(
                    ids_id=hl["ids_id"],
                    asx_code=asx_code or "",
                    date=date_raw,
                    time=time_val,
                    headline=hl["headline"],
                    pdf_url=hl["pdf_url"],
                    file_size=hl["file_size"],
                    num_pages=hl["num_pages"],
                    price_sensitive=price_sens,
                )
            )
        except Exception as exc:
            errors.append(f"announcements.do row {i}: {exc}")

    return announcements, errors


def parse_prev_bus_day_anns(html: str) -> tuple[list[Announcement], list[str]]:
    """
    Parse /asx/v2/statistics/prevBusDayAnns.do HTML.

    Column order: ASX Code | Date/Time | Price sens. | Headline
    """
    soup = BeautifulSoup(html, BS_PARSER)
    announcements: list[Announcement] = []
    errors: list[str] = []

    ann_data = soup.find("announcement_data")
    if ann_data is None:
        errors.append("prevBusDayAnns.do: <announcement_data> tag not found")
        return announcements, errors

    table = ann_data.find("table")
    if table is None:
        errors.append("prevBusDayAnns.do: no <table> inside <announcement_data>")
        return announcements, errors

    data_rows = [r for r in table.find_all("tr") if r.find("td")]

    for i, row in enumerate(data_rows):
        tds = row.find_all("td")
        if len(tds) < 4:
            continue
        try:
            asx_code = _clean(tds[0].get_text())
            date_raw = _clean(tds[1].find(string=True, recursive=False))
            time_span = tds[1].find("span", class_="dates-time")
            time_val = _clean(time_span.get_text()) if time_span else None
            price_sens = _is_price_sensitive(tds[2])
            hl = _parse_headline_td(tds[3])

            if not hl["ids_id"] or not hl["headline"]:
                continue

            announcements.append(
                Announcement(
                    ids_id=hl["ids_id"],
                    asx_code=asx_code,
                    date=date_raw,
                    time=time_val,
                    headline=hl["headline"],
                    pdf_url=hl["pdf_url"],
                    file_size=hl["file_size"],
                    num_pages=hl["num_pages"],
                    price_sensitive=price_sens,
                )
            )
        except Exception as exc:
            errors.append(f"prevBusDayAnns.do row {i}: {exc}")

    return announcements, errors


# ---------------------------------------------------------------------------
# Company directory
# ---------------------------------------------------------------------------


def fetch_company_tickers(session: requests.Session) -> list[str]:
    """
    Fetch the MarkitDigital company directory CSV and return a list of ASX ticker codes.
    Falls back to the old ASXListedCompanies.csv if MarkitDigital fails.
    """
    tickers = _fetch_markit_tickers(session)
    if tickers:
        log.info("Loaded %d tickers from MarkitDigital directory", len(tickers))
        return tickers

    log.warning("MarkitDigital failed, falling back to ASXListedCompanies.csv")
    tickers = _fetch_asx_listed_csv_tickers(session)
    if tickers:
        log.info("Loaded %d tickers from ASXListedCompanies.csv", len(tickers))
        return tickers

    log.error("Could not load company directory from any source")
    return []


def _fetch_markit_tickers(session: requests.Session) -> list[str]:
    """Fetch tickers from MarkitDigital CSV. Returns empty list on failure."""
    extra_headers = {"Referer": "https://www.asx.com.au/", "Origin": "https://www.asx.com.au"}
    try:
        resp = session.get(MARKIT_DIRECTORY_URL, headers=extra_headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.debug("MarkitDigital directory fetch failed: %s", exc)
        return []
    return _parse_tickers_from_csv(resp.content, col_index=0)


def _fetch_asx_listed_csv_tickers(session: requests.Session) -> list[str]:
    """Fetch tickers from ASXListedCompanies.csv. Returns empty list on failure."""
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.debug("ASXListedCompanies.csv fetch failed: %s", exc)
        return []
    return _parse_tickers_from_csv(resp.content, col_index=1, skip_header_rows=3)


def _parse_tickers_from_csv(
    raw: bytes, col_index: int, skip_header_rows: int = 0
) -> list[str]:
    """Decode and parse CSV bytes, returning ticker codes from the given column."""
    text = _decode_csv_bytes(raw)
    lines = text.splitlines()[skip_header_rows:]
    reader = csv.reader(io.StringIO("\n".join(lines)))
    tickers: list[str] = []
    for i, row in enumerate(reader):
        if i == 0:
            continue  # skip column header row
        if len(row) > col_index:
            ticker = row[col_index].strip()
            if re.match(r"^[A-Z0-9]{2,6}$", ticker):
                tickers.append(ticker)
    return tickers


def _decode_csv_bytes(raw: bytes) -> str:
    """Try multiple encodings to decode CSV bytes."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


# ---------------------------------------------------------------------------
# PDF resolution and download
# ---------------------------------------------------------------------------


def resolve_direct_pdf_url(session: requests.Session, ids_id: str) -> str | None:
    """
    Two-step PDF resolution:
      1. GET displayAnnouncement.do?display=pdf&idsId=... -> HTML terms page
      2. Parse <input name="pdfURL"> -> real CDN URL
    Returns None when the URL cannot be resolved.
    """
    url = f"{DISPLAY_ANNOUNCEMENT_URL}?display=pdf&idsId={ids_id}"
    resp = safe_get(session, url)
    if resp is None:
        return None
    soup = BeautifulSoup(resp.text, BS_PARSER)
    pdf_input = soup.find("input", {"name": "pdfURL"})
    if pdf_input and pdf_input.get("value"):
        return str(pdf_input["value"])
    log.debug("No pdfURL input found for idsId=%s", ids_id)
    return None


def download_pdf(
    session: requests.Session, ids_id: str, asx_code: str, direct_url: str
) -> tuple[str, str] | None:
    """
    Download a PDF to documents/{asx_code}/{ids_id}.pdf.
    Returns (direct_url, local_path) on success, None on failure.
    """
    if not TICKER_RE.match(asx_code) or not IDS_ID_RE.match(ids_id):
        log.warning("Rejected suspicious asx_code=%r ids_id=%r", asx_code, ids_id)
        return None

    dest_dir = DOCUMENTS_DIR / asx_code
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{ids_id}.pdf"

    if dest_path.exists():
        log.debug("Already downloaded: %s", dest_path)
        return direct_url, str(dest_path)

    try:
        with session.get(direct_url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            first_chunk = True
            with dest_path.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    if first_chunk and not chunk[:4].startswith(b"%PDF"):
                        log.warning("Response for ids_id=%s does not look like a PDF", ids_id)
                        return None
                    first_chunk = False
                    fh.write(chunk)
    except requests.RequestException as exc:
        log.warning("PDF download failed for ids_id=%s: %s", ids_id, exc)
        if dest_path.exists():
            dest_path.unlink()
        return None

    log.debug("Saved %s (%d bytes)", dest_path, dest_path.stat().st_size)
    return direct_url, str(dest_path)


def _resolve_and_download_worker(row: dict) -> tuple[str, str, str] | None:
    """
    Thread worker: resolve PDF URL and download. Returns (ids_id, direct_url, path)
    on success, None on failure. Does NO database writes (SQLite is not thread-safe).
    """
    session = make_session()
    ids_id = row["ids_id"]
    asx_code = row["asx_code"]

    direct_url = resolve_direct_pdf_url(session, ids_id)
    if not direct_url:
        log.warning("Could not resolve PDF URL for ids_id=%s", ids_id)
        return None

    result = download_pdf(session, ids_id, asx_code, direct_url)
    if result:
        url, path = result
        log.info("Downloaded %s -> %s", ids_id, path)
        return ids_id, url, path

    log.warning("PDF download failed for ids_id=%s", ids_id)
    return None


def batch_download(conn: sqlite3.Connection, rows: list[dict], workers: int = 5) -> int:
    """Download all undownloaded PDFs in parallel. Returns count of successful downloads."""
    if not rows:
        log.info("No PDFs to download.")
        return 0

    log.info("Downloading %d PDFs with %d workers...", len(rows), workers)
    completed: list[tuple[str, str, str]] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_resolve_and_download_worker, row): row["ids_id"]
            for row in rows
        }
        for future in as_completed(futures):
            ids_id = futures[future]
            try:
                result = future.result()
                if result:
                    completed.append(result)
            except Exception as exc:
                log.error("Unexpected error downloading ids_id=%s: %s", ids_id, exc)

    # Update database from main thread (SQLite is single-threaded)
    for ids_id, direct_url, path in completed:
        mark_downloaded(conn, ids_id, direct_url, path)

    log.info("Download complete: %d/%d succeeded", len(completed), len(rows))
    return len(completed)


# ---------------------------------------------------------------------------
# Crawl logic
# ---------------------------------------------------------------------------


def crawl_ticker(
    session: requests.Session,
    conn: sqlite3.Connection,
    ticker: str,
    period: str | None = None,
    year: int | None = None,
) -> CrawlResult:
    """Crawl announcements for a single ASX ticker. Returns a CrawlResult."""
    started_at = datetime.utcnow().isoformat()

    if year:
        params = {"by": "asxCode", "asxCode": ticker, "timeframe": "Y", "year": str(year)}
        period_label = f"year={year}"
    else:
        params = {"by": "asxCode", "asxCode": ticker, "timeframe": "D", "period": period or "M6"}
        period_label = period or "M6"

    log.info("Crawling %s period=%s", ticker, period_label)

    resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
    if resp is None:
        return CrawlResult(
            crawl_type="per_company",
            ticker=ticker,
            period=period_label,
            announcements_found=0,
            announcements_new=0,
            started_at=started_at,
            completed_at=datetime.utcnow().isoformat(),
            errors=[f"HTTP request failed for {ticker}"],
        )

    announcements, errors = parse_announcements_do(resp.text)

    new_count = 0
    for ann in announcements:
        if upsert_announcement(conn, ann):
            new_count += 1

    if errors:
        for err in errors:
            log.warning("[%s] Parse warning: %s", ticker, err)

    log.info(
        "[%s] found=%d new=%d errors=%d",
        ticker,
        len(announcements),
        new_count,
        len(errors),
    )

    return CrawlResult(
        crawl_type="per_company",
        ticker=ticker,
        period=period_label,
        announcements_found=len(announcements),
        announcements_new=new_count,
        started_at=started_at,
        completed_at=datetime.utcnow().isoformat(),
        errors=errors,
    )


def crawl_prev_bus_day(
    session: requests.Session, conn: sqlite3.Connection
) -> CrawlResult:
    """Crawl all companies' previous business day announcements in a single request."""
    started_at = datetime.utcnow().isoformat()
    log.info("Crawling prevBusDayAnns.do (all companies)...")

    resp = safe_get(session, PREV_BUS_DAY_URL)
    if resp is None:
        return CrawlResult(
            crawl_type="prev_bus_day",
            ticker=None,
            period=None,
            announcements_found=0,
            announcements_new=0,
            started_at=started_at,
            completed_at=datetime.utcnow().isoformat(),
            errors=["HTTP request failed for prevBusDayAnns.do"],
        )

    announcements, errors = parse_prev_bus_day_anns(resp.text)

    new_count = 0
    for ann in announcements:
        if upsert_announcement(conn, ann):
            new_count += 1

    if errors:
        for err in errors:
            log.warning("[prevBusDayAnns] Parse warning: %s", err)

    log.info(
        "[prevBusDayAnns] found=%d new=%d errors=%d",
        len(announcements),
        new_count,
        len(errors),
    )

    return CrawlResult(
        crawl_type="prev_bus_day",
        ticker=None,
        period=None,
        announcements_found=len(announcements),
        announcements_new=new_count,
        started_at=started_at,
        completed_at=datetime.utcnow().isoformat(),
        errors=errors,
    )


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


def cmd_crawl(args: argparse.Namespace) -> None:
    """crawl command: fetch announcements for one or more tickers."""
    conn = get_db()
    try:
        _do_crawl(conn, args)
    finally:
        conn.close()


def _do_crawl(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    """Internal crawl logic."""
    session = make_session()

    if args.all_day:
        result = crawl_prev_bus_day(session, conn)
        log_crawl(conn, result)
        _print_crawl_summary([result])

        if args.download:
            rows = fetch_undownloaded(conn)
            batch_download(conn, rows, workers=args.workers)
        return

    # Resolve ticker list
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        log.info("Fetching company directory...")
        tickers = fetch_company_tickers(session)
        if not tickers:
            log.error("Could not obtain ticker list. Use --tickers to specify manually.")
            sys.exit(1)

    if args.max_companies:
        tickers = tickers[: args.max_companies]

    log.info("Will crawl %d tickers", len(tickers))

    results: list[CrawlResult] = []
    for i, ticker in enumerate(tickers, 1):
        log.info("[%d/%d] %s", i, len(tickers), ticker)
        result = crawl_ticker(
            session, conn, ticker, period=args.period, year=args.year
        )
        log_crawl(conn, result)
        results.append(result)

        if i < len(tickers):
            time.sleep(FETCH_DELAY)

    _print_crawl_summary(results)

    if args.download:
        rows = fetch_undownloaded(conn)
        batch_download(conn, rows, workers=args.workers)


def cmd_monitor(args: argparse.Namespace) -> None:
    """monitor command: poll prevBusDayAnns.do on a fixed interval."""
    conn = get_db()
    try:
        session = make_session()
        log.info("Monitoring started. Interval: %ds. Press Ctrl+C to stop.", args.interval)

        while True:
            log.info("--- Monitor tick at %s ---", datetime.utcnow().isoformat())
            result = crawl_prev_bus_day(session, conn)
            log_crawl(conn, result)

            if args.download and result.announcements_new > 0:
                log.info("Downloading %d new PDFs...", result.announcements_new)
                rows = fetch_undownloaded(conn)
                batch_download(conn, rows)

            log.info(
                "Tick done. found=%d new=%d. Sleeping %ds...",
                result.announcements_found,
                result.announcements_new,
                args.interval,
            )
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Monitor stopped by user.")
    finally:
        conn.close()


def cmd_export(args: argparse.Namespace) -> None:
    """export command: write cached announcements to a JSON file."""
    conn = get_db()
    try:
        _do_export(conn, args)
    finally:
        conn.close()


def _do_export(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    """Internal export logic."""
    query = "SELECT * FROM announcements WHERE 1=1"
    params: list[Any] = []

    if args.ticker:
        query += " AND asx_code = ?"
        params.append(args.ticker.upper())

    if args.downloaded_only:
        query += " AND downloaded = TRUE"

    query += " ORDER BY date DESC, time DESC"

    cur = conn.execute(query, params)
    rows = [dict(row) for row in cur.fetchall()]

    output_path = Path(args.output)
    output_path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")

    log.info("Exported %d announcements to %s", len(rows), output_path)
    print(f"Exported {len(rows)} announcements -> {output_path}")


def cmd_stats(args: argparse.Namespace) -> None:
    """stats command: print a summary of the local cache."""
    conn = get_db()
    try:
        _do_stats(conn)
    finally:
        conn.close()


def _do_stats(conn: sqlite3.Connection) -> None:
    """Internal stats logic."""
    total = conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
    downloaded = conn.execute(
        "SELECT COUNT(*) FROM announcements WHERE downloaded = TRUE"
    ).fetchone()[0]
    companies = conn.execute(
        "SELECT COUNT(DISTINCT asx_code) FROM announcements"
    ).fetchone()[0]
    price_sens = conn.execute(
        "SELECT COUNT(*) FROM announcements WHERE price_sensitive = TRUE"
    ).fetchone()[0]
    crawls = conn.execute("SELECT COUNT(*) FROM crawl_log").fetchone()[0]

    earliest = conn.execute(
        "SELECT MIN(created_at) FROM announcements"
    ).fetchone()[0]
    latest = conn.execute(
        "SELECT MAX(created_at) FROM announcements"
    ).fetchone()[0]

    top_companies = conn.execute(
        "SELECT asx_code, COUNT(*) as cnt FROM announcements "
        "GROUP BY asx_code ORDER BY cnt DESC LIMIT 10"
    ).fetchall()

    print("=" * 50)
    print("  ASX Scraper Cache Statistics")
    print("=" * 50)
    print(f"  Total announcements : {total:>8,}")
    print(f"  Downloaded PDFs     : {downloaded:>8,}")
    print(f"  Unique companies    : {companies:>8,}")
    print(f"  Price sensitive     : {price_sens:>8,}")
    print(f"  Total crawl runs    : {crawls:>8,}")
    print(f"  Earliest cached     : {earliest or 'N/A'}")
    print(f"  Latest cached       : {latest or 'N/A'}")
    print(f"  SQLite DB           : {DB_PATH.resolve()}")
    print(f"  Documents dir       : {DOCUMENTS_DIR.resolve()}")
    print(f"  BS4 parser          : {BS_PARSER}")
    print()

    if top_companies:
        print("  Top 10 companies by announcement count:")
        for row in top_companies:
            print(f"    {row['asx_code']:<8}  {row['cnt']:>6,}")

    print("=" * 50)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _print_crawl_summary(results: list[CrawlResult]) -> None:
    """Print a concise table of crawl results."""
    total_found = sum(r.announcements_found for r in results)
    total_new = sum(r.announcements_new for r in results)
    total_errors = sum(len(r.errors) for r in results)
    print(
        f"\nCrawl summary: {len(results)} ticker(s)  "
        f"found={total_found}  new={total_new}  parse_errors={total_errors}"
    )


# ---------------------------------------------------------------------------
# CLI setup
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scraper.py",
        description="ASX Announcements Scraper",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- crawl --
    crawl_p = sub.add_parser("crawl", help="Crawl ASX announcements")
    crawl_p.add_argument("--tickers", help="Comma-separated ASX codes (e.g. BHP,CBA,NAB)")
    crawl_p.add_argument("--max-companies", type=int, help="Limit number of companies to crawl")
    crawl_p.add_argument(
        "--period",
        default="M6",
        choices=list(VALID_PERIODS),
        help="Time period (default: M6)",
    )
    crawl_p.add_argument("--year", type=int, help="Calendar year to crawl (overrides --period)")
    crawl_p.add_argument("--download", action="store_true", help="Download PDFs after crawling")
    crawl_p.add_argument(
        "--workers", type=int, default=5, help="Parallel download workers (default: 5)"
    )
    crawl_p.add_argument(
        "--all-day",
        action="store_true",
        help="Use prevBusDayAnns.do to get all companies' previous day announcements",
    )

    # -- monitor --
    monitor_p = sub.add_parser("monitor", help="Continuously monitor for new announcements")
    monitor_p.add_argument(
        "--interval", type=int, default=300, help="Check interval in seconds (default: 300)"
    )
    monitor_p.add_argument("--download", action="store_true", help="Download new PDFs automatically")

    # -- export --
    export_p = sub.add_parser("export", help="Export cached data to JSON")
    export_p.add_argument("--output", default="filings.json", help="Output file path")
    export_p.add_argument("--ticker", help="Filter by ASX ticker code")
    export_p.add_argument(
        "--downloaded-only", action="store_true", help="Only include downloaded entries"
    )

    # -- stats --
    sub.add_parser("stats", help="Show cache statistics")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    command_map = {
        "crawl": cmd_crawl,
        "monitor": cmd_monitor,
        "export": cmd_export,
        "stats": cmd_stats,
    }

    handler = command_map.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    handler(args)


if __name__ == "__main__":
    main()
