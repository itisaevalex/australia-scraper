"""
db.py — SQLite persistence layer for the ASX filings scraper.

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
CREATE TABLE IF NOT EXISTS filings (
    filing_id        TEXT PRIMARY KEY,
    source           TEXT NOT NULL DEFAULT 'asx',
    country          TEXT NOT NULL DEFAULT 'AU',
    ticker           TEXT NOT NULL,
    filing_date      TEXT NOT NULL,
    filing_time      TEXT,
    headline         TEXT NOT NULL,
    filing_type      TEXT,
    document_url     TEXT,
    direct_pdf_url   TEXT,
    file_size        TEXT,
    num_pages        INTEGER,
    price_sensitive  BOOLEAN DEFAULT FALSE,
    downloaded       BOOLEAN DEFAULT FALSE,
    download_path    TEXT,
    raw_metadata     TEXT,
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
# Each entry is (migration_id, sql).
MIGRATIONS: list[tuple[str, str]] = [
    # Table rename: old installs have `announcements`, new installs use `filings`
    # Handled specially in _apply_migrations — not a simple column add.
    # Column renames (SQLite 3.25+ supports ALTER TABLE RENAME COLUMN)
    ("__rename_col_ids_id",            "ALTER TABLE filings RENAME COLUMN ids_id TO filing_id"),
    ("__rename_col_asx_code",          "ALTER TABLE filings RENAME COLUMN asx_code TO ticker"),
    ("__rename_col_date",              "ALTER TABLE filings RENAME COLUMN date TO filing_date"),
    ("__rename_col_time",              "ALTER TABLE filings RENAME COLUMN time TO filing_time"),
    ("__rename_col_announcement_type", "ALTER TABLE filings RENAME COLUMN announcement_type TO filing_type"),
    ("__rename_col_pdf_url",           "ALTER TABLE filings RENAME COLUMN pdf_url TO document_url"),
    # New columns
    ("source",       "ALTER TABLE filings ADD COLUMN source TEXT NOT NULL DEFAULT 'asx'"),
    ("country",      "ALTER TABLE filings ADD COLUMN country TEXT NOT NULL DEFAULT 'AU'"),
    ("raw_metadata", "ALTER TABLE filings ADD COLUMN raw_metadata TEXT"),
    # Legacy column that may exist on very old installs (from original MIGRATIONS list)
    ("announcement_type",
     "ALTER TABLE announcements ADD COLUMN announcement_type TEXT"),
]


# ---------------------------------------------------------------------------
# Data models (frozen dataclasses for immutability)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Filing:
    """Immutable representation of a single ASX filing row."""

    filing_id: str
    source: str
    country: str
    ticker: str
    filing_date: str
    filing_time: str | None
    headline: str
    filing_type: str
    document_url: str | None
    file_size: str | None
    num_pages: int | None
    price_sensitive: bool


# Keep backwards-compatible alias so callers that import Announcement still work
# during the transition period. Remove once all callers are updated.
Announcement = Filing


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


def get_db(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite cache and ensure the schema exists.

    Applies any pending column migrations on every call so the DB is always
    up-to-date without a full schema recreation.

    Args:
        db_path: Path to the SQLite database file, or ':memory:' for an
                 in-memory database used in tests.

    Returns:
        An open sqlite3.Connection with row_factory set to sqlite3.Row.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    _ensure_filings_table(conn)
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    _apply_migrations(conn)
    return conn


def _ensure_filings_table(conn: sqlite3.Connection) -> None:
    """Rename legacy `announcements` table to `filings` if it exists.

    Handles migration from L2 databases that used the old table name.
    If `filings` already exists (new install or already migrated), does nothing.
    """
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "announcements" in tables and "filings" not in tables:
        log.info("DB migration: renaming table announcements -> filings")
        conn.execute("ALTER TABLE announcements RENAME TO filings")
        conn.commit()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply incremental ALTER TABLE migrations that may not exist yet.

    Column-rename migrations use RENAME COLUMN (SQLite 3.25+).
    ADD COLUMN migrations use the standard idempotent pattern.
    """
    # Build current column set for the filings table
    existing_cols = {
        row[1].lower()
        for row in conn.execute("PRAGMA table_info(filings)").fetchall()
    }

    for migration_id, sql in MIGRATIONS:
        # Skip special-case table-rename (handled in _ensure_filings_table)
        if migration_id == "__rename_table_filings":
            continue

        # Skip legacy announcements-table migration if filings already has the column
        if migration_id == "announcement_type" and "announcement_type" not in existing_cols:
            # This was for an old `announcements` table — irrelevant for `filings`
            continue

        # Column-rename migrations: only apply if old column still exists
        if migration_id.startswith("__rename_col_"):
            old_col = _extract_old_col_name(sql)
            if old_col and old_col.lower() in existing_cols:
                log.info("DB migration: renaming column via %r", migration_id)
                try:
                    conn.execute(sql)
                    conn.commit()
                    # Refresh column set after rename
                    existing_cols = {
                        row[1].lower()
                        for row in conn.execute("PRAGMA table_info(filings)").fetchall()
                    }
                except sqlite3.OperationalError as exc:
                    log.warning("Migration skipped (%s): %s", migration_id, exc)
            continue

        # ADD COLUMN migrations: skip if column already present
        col_name = migration_id.lower()
        if col_name not in existing_cols:
            log.info("DB migration: adding column %r", migration_id)
            try:
                conn.execute(sql)
                conn.commit()
                existing_cols.add(col_name)
            except sqlite3.OperationalError as exc:
                log.warning("Migration skipped (%s): %s", migration_id, exc)


def _extract_old_col_name(sql: str) -> str | None:
    """Extract the old column name from a RENAME COLUMN statement.

    Args:
        sql: SQL string of the form 'ALTER TABLE ... RENAME COLUMN old TO new'.

    Returns:
        The old column name in lowercase, or None if parsing fails.
    """
    import re
    match = re.search(r"RENAME\s+COLUMN\s+(\w+)\s+TO", sql, re.I)
    return match.group(1).lower() if match else None


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def upsert_filing(conn: sqlite3.Connection, filing: Filing) -> bool:
    """Insert filing if not already present. Returns True when new.

    Uses INSERT OR IGNORE so existing rows are never overwritten (idempotent).

    Args:
        conn:   Active SQLite connection.
        filing: The Filing dataclass to persist.

    Returns:
        True when a new row was inserted; False when the row already existed.
    """
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO filings
            (filing_id, source, country, ticker, filing_date, filing_time,
             headline, filing_type, document_url, file_size, num_pages,
             price_sensitive)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filing.filing_id,
            filing.source,
            filing.country,
            filing.ticker,
            filing.filing_date,
            filing.filing_time,
            filing.headline,
            filing.filing_type,
            filing.document_url,
            filing.file_size,
            filing.num_pages,
            filing.price_sensitive,
        ),
    )
    inserted = cur.rowcount > 0
    conn.commit()
    return inserted


# Backwards-compatible alias
upsert_announcement = upsert_filing


def mark_downloaded(
    conn: sqlite3.Connection, filing_id: str, direct_url: str, path: str
) -> None:
    """Update filing row after a successful PDF download.

    Args:
        conn:       Active SQLite connection.
        filing_id:  The filing primary key.
        direct_url: Resolved CDN URL of the downloaded PDF.
        path:       Local filesystem path where the file was saved.
    """
    conn.execute(
        """
        UPDATE filings
        SET downloaded = TRUE, direct_pdf_url = ?, download_path = ?
        WHERE filing_id = ?
        """,
        (direct_url, path, filing_id),
    )
    conn.commit()


def log_crawl(conn: sqlite3.Connection, result: CrawlResult) -> None:
    """Write a crawl_log entry.

    Args:
        conn:   Active SQLite connection.
        result: The CrawlResult to persist.
    """
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
    """Return rows that have a document_url but have not been downloaded yet.

    Args:
        conn:   Active SQLite connection.
        ticker: Optional ASX ticker code to filter by.

    Returns:
        List of row dicts with at minimum 'filing_id', 'ticker', 'document_url'.
    """
    if ticker:
        cur = conn.execute(
            "SELECT * FROM filings WHERE downloaded = FALSE AND document_url IS NOT NULL "
            "AND ticker = ?",
            (ticker,),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM filings WHERE downloaded = FALSE AND document_url IS NOT NULL"
        )
    return [dict(row) for row in cur.fetchall()]


def get_last_crawl_time(
    conn: sqlite3.Connection, ticker: str, period: str
) -> str | None:
    """Return the completed_at timestamp of the most recent successful crawl.

    A crawl is considered successful when completed_at is not NULL.

    Args:
        conn:   Active SQLite connection.
        ticker: ASX ticker code.
        period: Period label (e.g. 'M6').

    Returns:
        An ISO-8601 string, or None if no previous crawl exists.
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

    Args:
        conn:   Active SQLite connection.
        period: Period label (e.g. 'M6').

    Returns:
        Set of ticker strings.
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
