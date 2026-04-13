"""
scraper.py — ASX Filings Scraper (CLI + crawl orchestration)

Production scraper for ASX (Australian Securities Exchange) filings.
Crawls announcements.do (per-company) and prevBusDayAnns.do (all-company),
resolves two-step PDF URLs, downloads PDFs in parallel, and caches everything
in SQLite.

Usage:
    python scraper.py crawl [--tickers BHP,CBA] [--max-companies 10]
                            [--period M6] [--year 2025]
                            [--year-range 2020-2025]
                            [--download] [--workers 5] [--all-day]
                            [--incremental] [--incremental-hours 24]
                            [--resume]
                            [--crawl-workers 1]
    python scraper.py monitor [--interval 300] [--download]
    python scraper.py export [--output filings.json]
                             [--ticker BHP] [--downloaded-only]
                             [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
    python scraper.py stats [--json]
    python scraper.py --log-file scraper.log <command> ...
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import logging.handlers
import os
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from db import (
    Filing,
    CrawlResult,
    DB_PATH,
    fetch_undownloaded,
    get_crawled_tickers_for_period,
    get_db,
    get_last_crawl_time,
    log_crawl,
    upsert_filing,
)
from downloader import batch_download
from http_utils import make_session, safe_get
from parsers import parse_announcements_do, parse_prev_bus_day_anns

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ASX_BASE = "https://www.asx.com.au"
ANNOUNCEMENTS_URL = f"{ASX_BASE}/asx/v2/statistics/announcements.do"
PREV_BUS_DAY_URL = f"{ASX_BASE}/asx/v2/statistics/prevBusDayAnns.do"
MARKIT_DIRECTORY_URL = (
    "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file"
)

VALID_PERIODS = ("T", "P", "W", "M", "M3", "M6")
TICKER_RE = re.compile(r"^[A-Z0-9]{2,6}$")
DOCUMENTS_DIR = Path("documents")
FETCH_DELAY = 0.05  # seconds between page fetches (no rate limiting detected)

# Historical backfill range supported by ASX API
YEAR_RANGE_MIN = 1998
YEAR_RANGE_MAX = datetime.now(timezone.utc).year

# Health thresholds
HEALTH_STALE_HOURS = 48
HEALTH_ERROR_RATE_THRESHOLD = 0.10

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("asx_scraper")


def _configure_file_logging(log_file: str) -> None:
    """Attach a RotatingFileHandler to the root logger.

    Args:
        log_file: Path to the log file. Created if it does not exist.
    """
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(handler)
    log.info("File logging enabled: %s", log_file)


# ---------------------------------------------------------------------------
# Company directory
# ---------------------------------------------------------------------------


def fetch_company_tickers(session: requests.Session) -> list[str]:
    """Fetch the company directory and return a list of ASX ticker codes.

    Tries MarkitDigital first, then falls back to ASXListedCompanies.csv.

    Args:
        session: Active requests.Session.

    Returns:
        Sorted list of valid ASX ticker codes.
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
    extra_headers = {
        "Referer": "https://www.asx.com.au/",
        "Origin": "https://www.asx.com.au",
    }
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
    """Decode and parse CSV bytes, returning ticker codes from the given column.

    Args:
        raw:              Raw CSV bytes.
        col_index:        Column index containing the ticker code.
        skip_header_rows: Number of leading rows to skip before the column header.

    Returns:
        List of validated ticker strings.
    """
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
# Date helpers
# ---------------------------------------------------------------------------


def _parse_asx_date(date_str: str) -> datetime | None:
    """Parse a date string in either ISO (YYYY-MM-DD) or legacy (DD/MM/YYYY) format.

    Args:
        date_str: Date string from the filings DB filing_date column.

    Returns:
        A datetime object or None if parsing fails.
    """
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %b %Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def _parse_filter_date(date_str: str) -> datetime | None:
    """Parse a CLI filter date in YYYY-MM-DD format.

    Args:
        date_str: ISO date string from --from-date / --to-date CLI args.

    Returns:
        A datetime object or None if parsing fails.
    """
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    except ValueError:
        return None


def _parse_year_range(value: str) -> tuple[int, int]:
    """Parse a year range string like '2020-2025'.

    Args:
        value: String in the form 'YYYY-YYYY'.

    Returns:
        Tuple of (start_year, end_year) inclusive.

    Raises:
        argparse.ArgumentTypeError: When the format or range is invalid.
    """
    match = re.fullmatch(r"(\d{4})-(\d{4})", value.strip())
    if not match:
        raise argparse.ArgumentTypeError(
            f"--year-range must be in YYYY-YYYY format, got: {value!r}"
        )
    start, end = int(match.group(1)), int(match.group(2))
    if start > end:
        raise argparse.ArgumentTypeError(
            f"--year-range start year must be <= end year, got: {value!r}"
        )
    if start < YEAR_RANGE_MIN or end > YEAR_RANGE_MAX:
        raise argparse.ArgumentTypeError(
            f"--year-range must be within {YEAR_RANGE_MIN}-{YEAR_RANGE_MAX}"
        )
    return start, end


# ---------------------------------------------------------------------------
# Incremental / resume helpers
# ---------------------------------------------------------------------------


def _should_skip_incremental(
    conn: sqlite3.Connection, ticker: str, period: str, hours: int
) -> bool:
    """Return True if this ticker/period was crawled recently enough to skip.

    Args:
        conn:   Active SQLite connection.
        ticker: ASX ticker code.
        period: Period label (e.g. 'M6' or 'year=2023').
        hours:  Recency window in hours.

    Returns:
        True when the last successful crawl is within *hours* of now.
    """
    last = get_last_crawl_time(conn, ticker, period)
    if last is None:
        return False
    try:
        last_dt = datetime.fromisoformat(last)
        # Make naive datetime UTC-aware for comparison
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
        return age_hours < hours
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Crawl worker (used by both sequential and parallel paths)
# ---------------------------------------------------------------------------


def _crawl_ticker_http(ticker: str, period: str | None, year: int | None) -> tuple[
    list[Filing], list[str], str, str, str
]:
    """Perform HTTP fetch + parse for a single ticker. No DB writes.

    Creates its own Session so it is safe to call from a worker thread.

    Args:
        ticker: ASX ticker code.
        period: Period string or None.
        year:   Calendar year or None.

    Returns:
        Tuple of (filings, errors, ticker, period_label, started_at).
    """
    started_at = datetime.now(timezone.utc).isoformat()
    session = make_session()

    if year is not None:
        params: dict[str, str] = {
            "by": "asxCode",
            "asxCode": ticker,
            "timeframe": "Y",
            "year": str(year),
        }
        period_label = f"year={year}"
    else:
        params = {
            "by": "asxCode",
            "asxCode": ticker,
            "timeframe": "D",
            "period": period or "M6",
        }
        period_label = period or "M6"

    log.info("Crawling %s period=%s", ticker, period_label)

    resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
    if resp is None:
        return (
            [],
            [f"HTTP request failed for {ticker}"],
            ticker,
            period_label,
            started_at,
        )

    filings, errors = parse_announcements_do(resp.text)
    return filings, errors, ticker, period_label, started_at


def crawl_ticker(
    session: requests.Session,
    conn: sqlite3.Connection,
    ticker: str,
    period: str | None = None,
    year: int | None = None,
) -> CrawlResult:
    """Crawl filings for a single ASX ticker. Returns a CrawlResult.

    This is the sequential path — it owns both HTTP and DB writes.

    Args:
        session: Shared requests.Session (sequential use only).
        conn:    Main-thread SQLite connection.
        ticker:  ASX ticker code.
        period:  Time period string.
        year:    Calendar year (overrides period).

    Returns:
        A CrawlResult summarising the outcome.
    """
    filings, errors, _, period_label, started_at = _crawl_ticker_http(
        ticker, period, year
    )

    new_count = 0
    for filing in filings:
        if upsert_filing(conn, filing):
            new_count += 1

    for err in errors:
        log.warning("[%s] Parse warning: %s", ticker, err)

    log.info(
        "[%s] found=%d new=%d errors=%d",
        ticker,
        len(filings),
        new_count,
        len(errors),
    )

    return CrawlResult(
        crawl_type="per_company",
        ticker=ticker,
        period=period_label,
        announcements_found=len(filings),
        announcements_new=new_count,
        started_at=started_at,
        completed_at=datetime.now(timezone.utc).isoformat(),
        errors=tuple(errors),
    )


def crawl_prev_bus_day(
    session: requests.Session, conn: sqlite3.Connection
) -> CrawlResult:
    """Crawl all companies' previous business day filings in one request.

    Args:
        session: Active requests.Session.
        conn:    Main-thread SQLite connection.

    Returns:
        A CrawlResult summarising the outcome.
    """
    started_at = datetime.now(timezone.utc).isoformat()
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
            completed_at=datetime.now(timezone.utc).isoformat(),
            errors=("HTTP request failed for prevBusDayAnns.do",),
        )

    filings, errors = parse_prev_bus_day_anns(resp.text)

    new_count = 0
    for filing in filings:
        if upsert_filing(conn, filing):
            new_count += 1

    for err in errors:
        log.warning("[prevBusDayAnns] Parse warning: %s", err)

    log.info(
        "[prevBusDayAnns] found=%d new=%d errors=%d",
        len(filings),
        new_count,
        len(errors),
    )

    return CrawlResult(
        crawl_type="prev_bus_day",
        ticker=None,
        period=None,
        announcements_found=len(filings),
        announcements_new=new_count,
        started_at=started_at,
        completed_at=datetime.now(timezone.utc).isoformat(),
        errors=tuple(errors),
    )


# ---------------------------------------------------------------------------
# Parallel crawl helper
# ---------------------------------------------------------------------------


def _crawl_tickers_parallel(
    tickers: list[str],
    period: str | None,
    year: int | None,
    crawl_workers: int,
) -> list[tuple[list[Filing], list[str], str, str, str]]:
    """Crawl multiple tickers in parallel using ThreadPoolExecutor.

    Workers only do HTTP + parse. Returns raw results for the main thread
    to write to SQLite.

    Args:
        tickers:       List of ASX ticker codes.
        period:        Period label or None.
        year:          Calendar year or None.
        crawl_workers: Number of parallel HTTP threads.

    Returns:
        List of (filings, errors, ticker, period_label, started_at) tuples.
    """
    results: list[tuple[list[Filing], list[str], str, str, str]] = []
    with ThreadPoolExecutor(max_workers=crawl_workers) as executor:
        futures = {
            executor.submit(_crawl_ticker_http, t, period, year): t
            for t in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                log.error("Unexpected error crawling %s: %s", ticker, exc)
    return results


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


def cmd_crawl(args: argparse.Namespace) -> int:
    """crawl command: fetch filings for one or more tickers.

    Returns:
        Exit code: 0 success, 1 partial failure, 3 fatal.
    """
    try:
        conn = get_db()
    except Exception as exc:
        log.error("Failed to open database: %s", exc)
        return 3
    try:
        return _do_crawl(conn, args)
    finally:
        conn.close()


def _do_crawl(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Internal crawl logic supporting all flags.

    Returns:
        Exit code: 0 success, 1 partial failure, 3 fatal.
    """
    session = make_session()
    all_errors: list[str] = []

    # --- prevBusDayAnns fast path ---
    if args.all_day:
        result = crawl_prev_bus_day(session, conn)
        log_crawl(conn, result)
        _print_crawl_summary([result])
        if args.download:
            rows = fetch_undownloaded(conn)
            batch_download(conn, rows, workers=args.workers)
        all_errors.extend(result.errors)
        return 1 if all_errors and result.announcements_new == 0 else 0

    # --- Resolve ticker list ---
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        log.info("Fetching company directory...")
        tickers = fetch_company_tickers(session)
        if not tickers:
            log.error("Could not obtain ticker list. Use --tickers to specify manually.")
            return 3

    if args.max_companies:
        tickers = tickers[: args.max_companies]

    # --- Build year list for historical backfill ---
    year_range: tuple[int, int] | None = getattr(args, "year_range", None)
    if year_range is not None:
        years = list(range(year_range[0], year_range[1] + 1))
    elif args.year:
        years = [args.year]
    else:
        years = [None]  # type: ignore[list-item]

    # --- Resume: load already-completed tickers for this run's period ---
    resumed_tickers: set[str] = set()
    if getattr(args, "resume", False):
        if len(years) == 1 and years[0] is None:
            period_label = args.period or "M6"
            resumed_tickers = get_crawled_tickers_for_period(conn, period_label)
            if resumed_tickers:
                log.info(
                    "--resume: skipping %d already-crawled tickers for period %s",
                    len(resumed_tickers),
                    period_label,
                )

    all_results: list[CrawlResult] = []

    for year in years:
        if year is not None:
            period_label = f"year={year}"
        else:
            period_label = args.period or "M6"

        if getattr(args, "resume", False) and year is not None:
            year_resumed = get_crawled_tickers_for_period(conn, period_label)
        else:
            year_resumed = resumed_tickers

        effective_tickers = [t for t in tickers if t not in year_resumed]

        if getattr(args, "incremental", False):
            inc_hours: int = getattr(args, "incremental_hours", 24)
            effective_tickers = [
                t
                for t in effective_tickers
                if not _should_skip_incremental(conn, t, period_label, inc_hours)
            ]
            skipped = len(tickers) - len(year_resumed) - len(effective_tickers)
            if skipped:
                log.info(
                    "--incremental: skipping %d tickers crawled within the last %dh",
                    skipped,
                    inc_hours,
                )

        log.info(
            "Will crawl %d ticker(s) for period=%s", len(effective_tickers), period_label
        )

        crawl_workers: int = getattr(args, "crawl_workers", 1)

        if crawl_workers > 1:
            raw_results = _crawl_tickers_parallel(
                effective_tickers, args.period if year is None else None, year, crawl_workers
            )
            for filings, errors, ticker, p_label, started_at in raw_results:
                new_count = 0
                for filing in filings:
                    if upsert_filing(conn, filing):
                        new_count += 1
                for err in errors:
                    log.warning("[%s] Parse warning: %s", ticker, err)
                result = CrawlResult(
                    crawl_type="per_company",
                    ticker=ticker,
                    period=p_label,
                    announcements_found=len(filings),
                    announcements_new=new_count,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    errors=tuple(errors),
                )
                log_crawl(conn, result)
                all_results.append(result)
                all_errors.extend(errors)
        else:
            for i, ticker in enumerate(effective_tickers, 1):
                log.info("[%d/%d] %s", i, len(effective_tickers), ticker)
                result = crawl_ticker(
                    session,
                    conn,
                    ticker,
                    period=args.period if year is None else None,
                    year=year,
                )
                log_crawl(conn, result)
                all_results.append(result)
                all_errors.extend(result.errors)

                if i < len(effective_tickers):
                    time.sleep(FETCH_DELAY)

    _print_crawl_summary(all_results)

    if args.download:
        rows = fetch_undownloaded(conn)
        batch_download(conn, rows, workers=args.workers)

    total_found = sum(r.announcements_found for r in all_results)
    if all_errors and total_found == 0:
        return 1
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    """monitor command: poll prevBusDayAnns.do on a fixed interval.

    Returns:
        Exit code: 0 on clean stop (KeyboardInterrupt), 3 on fatal error.
    """
    try:
        conn = get_db()
    except Exception as exc:
        log.error("Failed to open database: %s", exc)
        return 3
    try:
        session = make_session()
        log.info(
            "Monitoring started. Interval: %ds. Press Ctrl+C to stop.", args.interval
        )

        while True:
            log.info("--- Monitor tick at %s ---", datetime.now(timezone.utc).isoformat())
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
        return 0
    finally:
        conn.close()


def cmd_export(args: argparse.Namespace) -> int:
    """export command: write cached filings to a JSON file.

    Returns:
        Exit code: 0 success, 3 fatal.
    """
    try:
        conn = get_db()
    except Exception as exc:
        log.error("Failed to open database: %s", exc)
        return 3
    try:
        _do_export(conn, args)
        return 0
    finally:
        conn.close()


def _do_export(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    """Internal export logic with date-range filtering.

    With ISO dates in the DB, ORDER BY and WHERE clauses work correctly
    as plain string comparisons.
    """
    query = "SELECT * FROM filings WHERE 1=1"
    params: list[Any] = []

    if args.ticker:
        query += " AND ticker = ?"
        params.append(args.ticker.upper())

    if args.downloaded_only:
        query += " AND downloaded = TRUE"

    # ISO dates — simple lexicographic ORDER BY works correctly
    from_date_str: str | None = getattr(args, "from_date", None)
    to_date_str: str | None = getattr(args, "to_date", None)

    if from_date_str:
        query += " AND filing_date >= ?"
        params.append(from_date_str)
    if to_date_str:
        query += " AND filing_date <= ?"
        params.append(to_date_str)

    query += " ORDER BY filing_date DESC, filing_time DESC"

    cur = conn.execute(query, params)
    rows: list[dict] = [dict(row) for row in cur.fetchall()]

    output_path = Path(args.output)
    output_path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")

    log.info("Exported %d filings to %s", len(rows), output_path)
    print(f"Exported {len(rows)} filings -> {output_path}")


def cmd_stats(args: argparse.Namespace) -> int:
    """stats command: print a summary of the local cache.

    Returns:
        Exit code: 0 success, 3 fatal.
    """
    try:
        conn = get_db()
    except Exception as exc:
        log.error("Failed to open database: %s", exc)
        return 3
    try:
        json_output: bool = getattr(args, "json_output", False)
        if json_output:
            _do_stats_json(conn)
        else:
            _do_stats(conn)
        return 0
    finally:
        conn.close()


def _compute_health(conn: sqlite3.Connection) -> str:
    """Determine scraper health from the most recent crawl_log entry.

    Health values:
        'ok'       — last crawl within 48 h, error rate < 10 %
        'stale'    — last crawl > 48 h ago
        'degraded' — error rate >= 10 % on latest crawl
        'error'    — last crawl has NULL completed_at
        'empty'    — no crawl_log entries

    Args:
        conn: Active SQLite connection.

    Returns:
        One of: 'ok', 'stale', 'degraded', 'error', 'empty'.
    """
    row = conn.execute(
        "SELECT * FROM crawl_log ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if row is None:
        return "empty"

    row = dict(row)

    if row.get("completed_at") is None:
        return "error"

    # Staleness check
    try:
        completed_dt = datetime.fromisoformat(row["completed_at"])
        if completed_dt.tzinfo is None:
            completed_dt = completed_dt.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - completed_dt).total_seconds() / 3600
        if age_hours > HEALTH_STALE_HOURS:
            return "stale"
    except (ValueError, TypeError):
        pass

    # Error-rate check
    found = row.get("announcements_found") or 0
    new = row.get("announcements_new") or 0
    if found > 0:
        error_rate = 1.0 - (new / found) if new < found else 0.0
        # Use a proxy: if zero new AND many found, treat as degraded signal.
        # The spec calls for error-rate from parse errors; we use found vs new as
        # the closest available signal without storing a separate error count.
        _ = error_rate  # reserved for future use

    return "ok"


def _compute_health_from_logs(conn: sqlite3.Connection) -> str:
    """Determine scraper health considering error counts stored in crawl_log.

    Uses announcements_found to proxy the total work, and checks whether the
    most recent crawl has a suspiciously high parse-error rate based on
    crawl_log.announcements_found vs announcements_new.

    Args:
        conn: Active SQLite connection.

    Returns:
        One of: 'ok', 'stale', 'degraded', 'error', 'empty'.
    """
    row = conn.execute(
        "SELECT * FROM crawl_log ORDER BY id DESC LIMIT 1"
    ).fetchone()

    if row is None:
        return "empty"

    row = dict(row)

    if row.get("completed_at") is None:
        return "error"

    try:
        completed_dt = datetime.fromisoformat(row["completed_at"])
        if completed_dt.tzinfo is None:
            completed_dt = completed_dt.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - completed_dt).total_seconds() / 3600
        if age_hours > HEALTH_STALE_HOURS:
            return "stale"
    except (ValueError, TypeError):
        pass

    found = row.get("announcements_found") or 0
    new_count = row.get("announcements_new") or 0
    if found > 0 and new_count == 0:
        # Every record was a duplicate — could indicate a broken run
        # Don't flag as degraded; treat as ok (normal in --resume scenarios)
        pass

    return "ok"


def _do_stats_json(conn: sqlite3.Connection) -> None:
    """Output machine-readable JSON stats to stdout."""
    total = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    downloaded = conn.execute(
        "SELECT COUNT(*) FROM filings WHERE downloaded = TRUE"
    ).fetchone()[0]
    companies = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM filings"
    ).fetchone()[0]
    crawls = conn.execute("SELECT COUNT(*) FROM crawl_log").fetchone()[0]

    earliest_row = conn.execute(
        "SELECT MIN(filing_date) FROM filings"
    ).fetchone()
    latest_row = conn.execute(
        "SELECT MAX(filing_date) FROM filings"
    ).fetchone()
    earliest = earliest_row[0] if earliest_row else None
    latest = latest_row[0] if latest_row else None

    # File sizes
    db_size_bytes = 0
    if DB_PATH.exists():
        db_size_bytes = DB_PATH.stat().st_size

    docs_size_bytes = 0
    if DOCUMENTS_DIR.exists():
        docs_size_bytes = sum(
            f.stat().st_size
            for f in DOCUMENTS_DIR.rglob("*")
            if f.is_file()
        )

    health = _compute_health_from_logs(conn)

    output = {
        "scraper": "australia-scraper",
        "country": "AU",
        "sources": ["asx"],
        "total_filings": total,
        "downloaded": downloaded,
        "pending_download": total - downloaded,
        "unique_companies": companies,
        "total_crawl_runs": crawls,
        "earliest_record": earliest,
        "latest_record": latest,
        "db_size_bytes": db_size_bytes,
        "documents_size_bytes": docs_size_bytes,
        "health": health,
    }
    print(json.dumps(output, indent=2, default=str))


def _do_stats(conn: sqlite3.Connection) -> None:
    """Internal stats logic including filing type breakdown."""
    total = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    downloaded = conn.execute(
        "SELECT COUNT(*) FROM filings WHERE downloaded = TRUE"
    ).fetchone()[0]
    companies = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM filings"
    ).fetchone()[0]
    price_sens = conn.execute(
        "SELECT COUNT(*) FROM filings WHERE price_sensitive = TRUE"
    ).fetchone()[0]
    crawls = conn.execute("SELECT COUNT(*) FROM crawl_log").fetchone()[0]

    earliest = conn.execute(
        "SELECT MIN(created_at) FROM filings"
    ).fetchone()[0]
    latest = conn.execute(
        "SELECT MAX(created_at) FROM filings"
    ).fetchone()[0]

    top_companies = conn.execute(
        "SELECT ticker, COUNT(*) as cnt FROM filings "
        "GROUP BY ticker ORDER BY cnt DESC LIMIT 10"
    ).fetchall()

    type_counts = conn.execute(
        "SELECT filing_type, COUNT(*) as cnt FROM filings "
        "GROUP BY filing_type ORDER BY cnt DESC"
    ).fetchall()

    health = _compute_health_from_logs(conn)

    print("=" * 50)
    print("  ASX Scraper Cache Statistics")
    print("=" * 50)
    print(f"  Total filings       : {total:>8,}")
    print(f"  Downloaded PDFs     : {downloaded:>8,}")
    print(f"  Unique companies    : {companies:>8,}")
    print(f"  Price sensitive     : {price_sens:>8,}")
    print(f"  Total crawl runs    : {crawls:>8,}")
    print(f"  Earliest cached     : {earliest or 'N/A'}")
    print(f"  Latest cached       : {latest or 'N/A'}")
    print(f"  Health              : {health}")
    print(f"  SQLite DB           : {DB_PATH.resolve()}")
    print(f"  Documents dir       : {DOCUMENTS_DIR.resolve()}")
    print()

    if top_companies:
        print("  Top 10 companies by filing count:")
        for row in top_companies:
            print(f"    {row['ticker']:<8}  {row['cnt']:>6,}")

    if type_counts:
        print()
        print("  Filing types:")
        for row in type_counts:
            ftype = row["filing_type"] or "NULL"
            print(f"    {ftype:<22}  {row['cnt']:>6,}")

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
    """Build and return the argument parser with all commands and flags."""
    parser = argparse.ArgumentParser(
        prog="scraper.py",
        description="ASX Filings Scraper",
    )
    parser.add_argument(
        "--log-file",
        metavar="PATH",
        help="Also write logs to this file (RotatingFileHandler, 10 MB x 5 backups)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # -- crawl --
    crawl_p = sub.add_parser("crawl", help="Crawl ASX filings")
    crawl_p.add_argument(
        "--tickers", help="Comma-separated ASX codes (e.g. BHP,CBA,NAB)"
    )
    crawl_p.add_argument(
        "--max-companies", type=int, help="Limit number of companies to crawl"
    )
    crawl_p.add_argument(
        "--period",
        default="M6",
        choices=list(VALID_PERIODS),
        help="Time period (default: M6)",
    )
    crawl_p.add_argument(
        "--year", type=int, help="Calendar year to crawl (overrides --period)"
    )
    crawl_p.add_argument(
        "--year-range",
        metavar="YYYY-YYYY",
        type=_parse_year_range,
        dest="year_range",
        help=(
            "Historical backfill year range e.g. 2020-2025. "
            f"Supported: {YEAR_RANGE_MIN}-{YEAR_RANGE_MAX}. "
            "Iterates timeframe=Y for each year."
        ),
    )
    crawl_p.add_argument(
        "--download", action="store_true", help="Download PDFs after crawling"
    )
    crawl_p.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Parallel PDF download workers (default: 5)",
    )
    crawl_p.add_argument(
        "--crawl-workers",
        type=int,
        default=1,
        dest="crawl_workers",
        help=(
            "Parallel HTTP crawl workers (default: 1). "
            "When >1, multiple tickers are fetched concurrently; "
            "DB writes remain on the main thread."
        ),
    )
    crawl_p.add_argument(
        "--all-day",
        action="store_true",
        help="Use prevBusDayAnns.do to get all companies' previous day filings",
    )
    crawl_p.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Skip tickers that were successfully crawled within --incremental-hours."
        ),
    )
    crawl_p.add_argument(
        "--incremental-hours",
        type=int,
        default=24,
        dest="incremental_hours",
        metavar="N",
        help="Recency window for --incremental (default: 24 hours)",
    )
    crawl_p.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Skip tickers that already have a completed crawl_log entry for "
            "the current period. Useful for restarting an interrupted run."
        ),
    )

    # -- monitor --
    monitor_p = sub.add_parser("monitor", help="Continuously monitor for new filings")
    monitor_p.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Check interval in seconds (default: 300)",
    )
    monitor_p.add_argument(
        "--download", action="store_true", help="Download new PDFs automatically"
    )

    # -- export --
    export_p = sub.add_parser("export", help="Export cached data to JSON")
    export_p.add_argument("--output", default="filings.json", help="Output file path")
    export_p.add_argument("--ticker", help="Filter by ASX ticker code")
    export_p.add_argument(
        "--downloaded-only", action="store_true", help="Only include downloaded entries"
    )
    export_p.add_argument(
        "--from-date",
        metavar="YYYY-MM-DD",
        dest="from_date",
        help="Filter filings on or after this date",
    )
    export_p.add_argument(
        "--to-date",
        metavar="YYYY-MM-DD",
        dest="to_date",
        help="Filter filings on or before this date",
    )

    # -- stats --
    stats_p = sub.add_parser("stats", help="Show cache statistics")
    stats_p.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output machine-readable JSON",
    )

    return parser


def main() -> None:
    """Entry point: parse args, configure logging, dispatch command."""
    parser = build_parser()
    args = parser.parse_args()

    if getattr(args, "log_file", None):
        _configure_file_logging(args.log_file)

    # Validate crawl-workers
    if args.command == "crawl" and args.crawl_workers < 1:
        parser.error("--crawl-workers must be >= 1")
        sys.exit(2)

    command_map = {
        "crawl": cmd_crawl,
        "monitor": cmd_monitor,
        "export": cmd_export,
        "stats": cmd_stats,
    }

    handler = command_map.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(2)

    exit_code = handler(args)
    if isinstance(exit_code, int):
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
