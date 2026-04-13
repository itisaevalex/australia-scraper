"""
db.py — SQLite persistence layer for the ASX announcements scraper.

Handles schema creation, migrations, upserts, and query helpers.
All SQLite writes must happen on the thread that owns the connection.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger("asx_scraper")

DB_PATH = Path("filings_cache.db")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS announcements (
    ids_id           TEXT PRIMARY KEY,
    asx_code         TEXT NOT NULL,
    date             TEXT NOT NULL,
    time             TEXT,
    headline         TEXT NOT NULL,
    announcement_type TEXT,
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

# Incremental migrations applied after initial schema creation.
MIGRATIONS: list[tuple[str, str]] = [
    (
        "announcement_type",
        "ALTER TABLE announcements ADD COLUMN announcement_type TEXT",
    ),
]


# ---------------------------------------------------------------------------
# Data models (frozen dataclasses for immutability)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Announcement:
    """Immutable representation of a single ASX announcement row."""

    ids_id: str
    asx_code: str
    date: str
    time: str | None
    headline: str
    announcement_type: str
    pdf_url: str | None
    file_size: str | None
    num_pages: int | None
    price_sensitive: bool


@dataclass(frozen=True)
class CrawlResult:
    """Immutable crawl outcome returned by crawl functions."""

    crawl_type: str
    ticker: str | None
    period: str | None
    announcements_found: int
    announcements_new: int
    started_at: str
    completed_at: str
    errors: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


def get_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite cache and ensure the schema exists.

    Applies any pending column migrations on every call so the DB is always
    up-to-date without a full schema recreation.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    _apply_migrations(conn)
    return conn


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply incremental ALTER TABLE migrations that may not exist yet."""
    existing_cols = {
        row[1].lower()
        for row in conn.execute("PRAGMA table_info(announcements)").fetchall()
    }
    for col_name, sql in MIGRATIONS:
        if col_name.lower() not in existing_cols:
            log.info("DB migration: adding column %r", col_name)
            try:
                conn.execute(sql)
                conn.commit()
            except sqlite3.OperationalError as exc:
                log.warning("Migration skipped (%s): %s", col_name, exc)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def upsert_announcement(conn: sqlite3.Connection, ann: Announcement) -> bool:
    """Insert announcement if not already present. Returns True when new.

    Uses INSERT OR IGNORE so existing rows are never overwritten (idempotent).
    """
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO announcements
            (ids_id, asx_code, date, time, headline, announcement_type,
             pdf_url, file_size, num_pages, price_sensitive)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ann.ids_id,
            ann.asx_code,
            ann.date,
            ann.time,
            ann.headline,
            ann.announcement_type,
            ann.pdf_url,
            ann.file_size,
            ann.num_pages,
            ann.price_sensitive,
        ),
    )
    inserted = cur.rowcount > 0
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


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


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


def get_last_crawl_time(
    conn: sqlite3.Connection, ticker: str, period: str
) -> str | None:
    """Return the completed_at timestamp of the most recent successful crawl.

    A crawl is considered successful when completed_at is not NULL.
    Returns an ISO-8601 string or None if no previous crawl exists.
    """
    row = conn.execute(
        """
        SELECT completed_at FROM crawl_log
        WHERE crawl_type = 'per_company'
          AND ticker = ?
          AND period = ?
          AND completed_at IS NOT NULL
        ORDER BY completed_at DESC
        LIMIT 1
        """,
        (ticker, period),
    ).fetchone()
    return row["completed_at"] if row else None


def get_crawled_tickers_for_period(
    conn: sqlite3.Connection, period: str
) -> set[str]:
    """Return the set of tickers that have a completed crawl entry for *period*.

    Used by the --resume flag to skip already-crawled tickers.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT ticker FROM crawl_log
        WHERE crawl_type = 'per_company'
          AND period = ?
          AND completed_at IS NOT NULL
        """,
        (period,),
    ).fetchall()
    return {row["ticker"] for row in rows if row["ticker"]}
