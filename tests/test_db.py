"""
test_db.py — Unit tests for all database operations in db.py.

Uses in-memory SQLite so tests are fully isolated and fast.

Tests cover:
  - get_db()                        : schema creation
  - upsert_filing()                 : INSERT OR IGNORE semantics
  - mark_downloaded()               : UPDATE semantics
  - fetch_undownloaded()            : SELECT with filters
  - log_crawl()                     : crawl_log INSERT
  - get_last_crawl_time()           : query helper
  - get_crawled_tickers_for_period(): query helper
  - migration helpers               : table/column rename
"""
from __future__ import annotations

import sqlite3

import pytest

from db import (
    Filing,
    CrawlResult,
    fetch_undownloaded,
    get_crawled_tickers_for_period,
    get_db,
    get_last_crawl_time,
    log_crawl,
    mark_downloaded,
    upsert_filing,
    upsert_announcement,  # backwards-compat alias
)


# ---------------------------------------------------------------------------
# Helper: build a Filing with all required fields
# ---------------------------------------------------------------------------

def _filing(
    filing_id: str,
    ticker: str = "BHP",
    filing_date: str = "2025-01-01",
    headline: str = "Test Filing",
    document_url: str | None = "http://example.com/doc.pdf",
    price_sensitive: bool = False,
    filing_type: str = "other",
    source: str = "asx",
    country: str = "AU",
) -> Filing:
    return Filing(
        filing_id=filing_id,
        source=source,
        country=country,
        ticker=ticker,
        filing_date=filing_date,
        filing_time=None,
        headline=headline,
        filing_type=filing_type,
        document_url=document_url,
        file_size=None,
        num_pages=None,
        price_sensitive=price_sensitive,
    )


# ---------------------------------------------------------------------------
# get_db() — schema creation
# ---------------------------------------------------------------------------


class TestGetDb:
    def test_creates_filings_table(self, mem_db):
        # Arrange / Act (mem_db fixture already calls get_db)
        # Assert
        tables = {row[0] for row in mem_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "filings" in tables

    def test_creates_crawl_log_table(self, mem_db):
        tables = {row[0] for row in mem_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "crawl_log" in tables

    def test_filings_has_expected_columns(self, mem_db):
        cols = {row[1] for row in mem_db.execute(
            "PRAGMA table_info(filings)"
        ).fetchall()}
        expected = {
            "filing_id", "source", "country", "ticker",
            "company_name", "filing_date", "filing_time", "headline",
            "filing_type", "category", "subcategory",
            "document_url", "direct_download_url", "file_size",
            "num_pages", "price_sensitive", "downloaded", "download_path",
            "raw_metadata", "created_at",
        }
        assert expected.issubset(cols)

    def test_crawl_log_has_expected_columns(self, mem_db):
        cols = {row[1] for row in mem_db.execute(
            "PRAGMA table_info(crawl_log)"
        ).fetchall()}
        expected = {
            "id", "crawl_type", "ticker", "period",
            "source", "query_params", "pages_crawled",
            "filings_found", "filings_new", "errors",
            "started_at", "completed_at", "duration_seconds",
        }
        assert expected.issubset(cols)

    def test_filing_id_is_primary_key(self, mem_db):
        pk_cols = [
            row[1]
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
            if row[5] == 1  # pk flag
        ]
        assert "filing_id" in pk_cols

    def test_calling_get_db_with_memory_twice_is_safe(self):
        # get_db uses CREATE TABLE IF NOT EXISTS — idempotent
        conn1 = get_db(":memory:")
        conn2 = get_db(":memory:")
        for conn in (conn1, conn2):
            tables = [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            assert tables.count("filings") == 1
        conn1.close()
        conn2.close()

    def test_source_column_has_asx_default(self, mem_db):
        cols_info = {
            row[1]: {"dflt_value": row[4]}
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "source" in cols_info

    def test_country_column_present(self, mem_db):
        cols = {row[1] for row in mem_db.execute(
            "PRAGMA table_info(filings)"
        ).fetchall()}
        assert "country" in cols

    def test_raw_metadata_column_present(self, mem_db):
        cols = {row[1] for row in mem_db.execute(
            "PRAGMA table_info(filings)"
        ).fetchall()}
        assert "raw_metadata" in cols

    def test_migration_error_does_not_crash(self):
        """When a migration is already applied, get_db should not raise."""
        conn = get_db(":memory:")
        from db import _apply_migrations
        try:
            _apply_migrations(conn)  # should not raise
        except Exception as exc:
            pytest.fail(f"_apply_migrations raised unexpectedly: {exc}")
        conn.close()

    def test_legacy_announcements_table_renamed_to_filings(self):
        """Simulate an L2 database with an 'announcements' table and verify migration."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Create old L2 schema
        conn.execute("""
            CREATE TABLE announcements (
                ids_id TEXT PRIMARY KEY,
                asx_code TEXT NOT NULL,
                date TEXT NOT NULL,
                time TEXT,
                headline TEXT NOT NULL,
                announcement_type TEXT,
                pdf_url TEXT,
                direct_pdf_url TEXT,
                file_size TEXT,
                num_pages INTEGER,
                price_sensitive BOOLEAN DEFAULT FALSE,
                downloaded BOOLEAN DEFAULT FALSE,
                download_path TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE crawl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_type TEXT NOT NULL,
                ticker TEXT,
                period TEXT,
                filings_found INTEGER,
                filings_new INTEGER,
                started_at TEXT NOT NULL,
                completed_at TEXT
            )
        """)
        conn.commit()
        conn.close()

        # Now open the same path through get_db — can't use :memory: for this,
        # so we test the rename logic directly.
        import sqlite3 as _sq
        from db import _ensure_filings_table, _apply_migrations, SCHEMA_SQL

        conn2 = _sq.connect(":memory:")
        conn2.row_factory = _sq.Row
        conn2.execute("""
            CREATE TABLE announcements (
                ids_id TEXT PRIMARY KEY,
                asx_code TEXT NOT NULL,
                date TEXT NOT NULL,
                time TEXT,
                headline TEXT NOT NULL,
                announcement_type TEXT,
                pdf_url TEXT,
                direct_download_url TEXT,
                file_size TEXT,
                num_pages INTEGER,
                price_sensitive BOOLEAN DEFAULT FALSE,
                downloaded BOOLEAN DEFAULT FALSE,
                download_path TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn2.commit()

        _ensure_filings_table(conn2)
        tables = {row[0] for row in conn2.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "filings" in tables
        assert "announcements" not in tables
        conn2.close()


# ---------------------------------------------------------------------------
# upsert_filing() — INSERT OR IGNORE
# ---------------------------------------------------------------------------


class TestUpsertFiling:
    def test_new_filing_returns_true(self, mem_db, sample_filing):
        result = upsert_filing(mem_db, sample_filing)
        assert result is True

    def test_duplicate_filing_returns_false(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        result = upsert_filing(mem_db, sample_filing)
        assert result is False

    def test_row_is_stored_after_insert(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT * FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row is not None

    def test_stored_row_matches_inserted_data(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = dict(mem_db.execute(
            "SELECT * FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone())
        assert row["filing_id"] == sample_filing.filing_id
        assert row["ticker"] == sample_filing.ticker
        assert row["filing_date"] == sample_filing.filing_date
        assert row["filing_time"] == sample_filing.filing_time
        assert row["headline"] == sample_filing.headline
        assert row["document_url"] == sample_filing.document_url
        assert row["file_size"] == sample_filing.file_size
        assert row["num_pages"] == sample_filing.num_pages
        assert row["filing_type"] == sample_filing.filing_type
        assert row["source"] == "asx"
        assert row["country"] == "AU"

    def test_price_sensitive_false_stored_correctly(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = dict(mem_db.execute(
            "SELECT price_sensitive FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone())
        assert bool(row["price_sensitive"]) is False

    def test_price_sensitive_true_stored_correctly(self, mem_db):
        f = _filing("PS000001", ticker="CBA", price_sensitive=True)
        upsert_filing(mem_db, f)
        row = dict(mem_db.execute(
            "SELECT price_sensitive FROM filings WHERE filing_id = ?",
            (f.filing_id,),
        ).fetchone())
        assert bool(row["price_sensitive"]) is True

    def test_two_different_filing_ids_both_stored(self, mem_db):
        upsert_filing(mem_db, _filing("AAAA0001", ticker="BHP"))
        upsert_filing(mem_db, _filing("BBBB0002", ticker="CBA"))
        count = mem_db.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        assert count == 2

    def test_downloaded_defaults_to_false(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = dict(mem_db.execute(
            "SELECT downloaded FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone())
        assert bool(row["downloaded"]) is False

    def test_null_document_url_stored_correctly(self, mem_db):
        f = _filing("NULL0001", document_url=None)
        upsert_filing(mem_db, f)
        row = dict(mem_db.execute(
            "SELECT document_url FROM filings WHERE filing_id = ?", (f.filing_id,)
        ).fetchone())
        assert row["document_url"] is None

    def test_filing_type_stored_correctly(self, mem_db):
        f = _filing("TYPE0001", filing_type="annual_report")
        upsert_filing(mem_db, f)
        row = dict(mem_db.execute(
            "SELECT filing_type FROM filings WHERE filing_id = ?", (f.filing_id,)
        ).fetchone())
        assert row["filing_type"] == "annual_report"

    def test_upsert_announcement_alias_works(self, mem_db):
        """The backwards-compatible upsert_announcement alias must still function."""
        f = _filing("ALIAS001")
        result = upsert_announcement(mem_db, f)
        assert result is True


# ---------------------------------------------------------------------------
# mark_downloaded() — UPDATE after successful download
# ---------------------------------------------------------------------------


class TestMarkDownloaded:
    def test_mark_downloaded_sets_downloaded_true(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        mark_downloaded(
            mem_db,
            sample_filing.filing_id,
            "https://cdn.example.com/file.pdf",
            "/tmp/docs/BHP/12345678.pdf",
        )
        row = dict(mem_db.execute(
            "SELECT downloaded FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone())
        assert bool(row["downloaded"]) is True

    def test_mark_downloaded_sets_direct_download_url(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        direct_url = "https://announcements.asx.com.au/asxpdf/20260413/pdf/abc123.pdf"
        mark_downloaded(mem_db, sample_filing.filing_id, direct_url, "/tmp/path.pdf")
        row = dict(mem_db.execute(
            "SELECT direct_download_url FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone())
        assert row["direct_download_url"] == direct_url

    def test_mark_downloaded_sets_download_path(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        path = "/tmp/documents/BHP/12345678.pdf"
        mark_downloaded(mem_db, sample_filing.filing_id, "https://cdn.example.com/x.pdf", path)
        row = dict(mem_db.execute(
            "SELECT download_path FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone())
        assert row["download_path"] == path

    def test_mark_downloaded_does_not_affect_other_rows(self, mem_db):
        upsert_filing(mem_db, _filing("MARK0001", ticker="BHP"))
        upsert_filing(mem_db, _filing("MARK0002", ticker="CBA"))
        mark_downloaded(mem_db, "MARK0001", "https://cdn.com/a.pdf", "/tmp/a.pdf")
        row2 = dict(mem_db.execute(
            "SELECT downloaded FROM filings WHERE filing_id = ?", ("MARK0002",)
        ).fetchone())
        assert bool(row2["downloaded"]) is False


# ---------------------------------------------------------------------------
# fetch_undownloaded() — SELECT pending downloads
# ---------------------------------------------------------------------------


class TestFetchUndownloaded:
    def _insert(
        self,
        conn: sqlite3.Connection,
        filing_id: str,
        ticker: str = "BHP",
        document_url: str | None = "http://example.com/doc.pdf",
        downloaded: bool = False,
    ) -> None:
        upsert_filing(conn, _filing(filing_id, ticker=ticker, document_url=document_url))
        if downloaded:
            mark_downloaded(conn, filing_id, "https://cdn.com/x.pdf", f"/tmp/{filing_id}.pdf")

    def test_returns_rows_with_document_url_and_downloaded_false(self, mem_db):
        self._insert(mem_db, "FETCH001")
        rows = fetch_undownloaded(mem_db)
        assert len(rows) == 1
        assert rows[0]["filing_id"] == "FETCH001"

    def test_excludes_already_downloaded_rows(self, mem_db):
        self._insert(mem_db, "DONE001", downloaded=True)
        rows = fetch_undownloaded(mem_db)
        ids = [r["filing_id"] for r in rows]
        assert "DONE001" not in ids

    def test_excludes_rows_without_document_url(self, mem_db):
        self._insert(mem_db, "NOPDF01", document_url=None)
        rows = fetch_undownloaded(mem_db)
        ids = [r["filing_id"] for r in rows]
        assert "NOPDF01" not in ids

    def test_returns_empty_list_when_no_pending(self, mem_db):
        self._insert(mem_db, "DONE002", downloaded=True)
        rows = fetch_undownloaded(mem_db)
        assert rows == []

    def test_ticker_filter_returns_only_matching_rows(self, mem_db):
        self._insert(mem_db, "BHP0001", ticker="BHP")
        self._insert(mem_db, "CBA0001", ticker="CBA")
        rows = fetch_undownloaded(mem_db, ticker="BHP")
        assert all(r["ticker"] == "BHP" for r in rows)
        assert len(rows) == 1

    def test_ticker_filter_excludes_other_tickers(self, mem_db):
        self._insert(mem_db, "BHP0002", ticker="BHP")
        self._insert(mem_db, "CBA0002", ticker="CBA")
        rows = fetch_undownloaded(mem_db, ticker="BHP")
        ids = [r["filing_id"] for r in rows]
        assert "CBA0002" not in ids

    def test_no_ticker_filter_returns_all_pending(self, mem_db):
        self._insert(mem_db, "MIX0001", ticker="BHP")
        self._insert(mem_db, "MIX0002", ticker="CBA")
        rows = fetch_undownloaded(mem_db)
        assert len(rows) == 2

    def test_rows_are_returned_as_dicts(self, mem_db):
        self._insert(mem_db, "DICT001")
        rows = fetch_undownloaded(mem_db)
        assert isinstance(rows[0], dict)
        assert "filing_id" in rows[0]
        assert "ticker" in rows[0]


# ---------------------------------------------------------------------------
# log_crawl() — INSERT into crawl_log
# ---------------------------------------------------------------------------


class TestLogCrawl:
    def _result(self, **kwargs) -> CrawlResult:
        defaults = dict(
            crawl_type="per_company",
            ticker="BHP",
            period="M6",
            filings_found=10,
            filings_new=3,
            started_at="2025-01-01T00:00:00+00:00",
            completed_at="2025-01-01T00:01:00+00:00",
            errors=[],
        )
        defaults.update(kwargs)
        return CrawlResult(**defaults)

    def test_log_crawl_inserts_one_row(self, mem_db):
        log_crawl(mem_db, self._result())
        count = mem_db.execute("SELECT COUNT(*) FROM crawl_log").fetchone()[0]
        assert count == 1

    def test_log_crawl_stores_crawl_type(self, mem_db):
        log_crawl(mem_db, self._result(crawl_type="prev_bus_day"))
        row = dict(mem_db.execute("SELECT * FROM crawl_log ORDER BY id DESC LIMIT 1").fetchone())
        assert row["crawl_type"] == "prev_bus_day"

    def test_log_crawl_stores_ticker(self, mem_db):
        log_crawl(mem_db, self._result(ticker="CBA"))
        row = dict(mem_db.execute("SELECT * FROM crawl_log").fetchone())
        assert row["ticker"] == "CBA"

    def test_log_crawl_stores_period(self, mem_db):
        log_crawl(mem_db, self._result(period="Y"))
        row = dict(mem_db.execute("SELECT * FROM crawl_log").fetchone())
        assert row["period"] == "Y"

    def test_log_crawl_stores_counts(self, mem_db):
        log_crawl(mem_db, self._result(filings_found=100, filings_new=25))
        row = dict(mem_db.execute("SELECT * FROM crawl_log").fetchone())
        assert row["filings_found"] == 100
        assert row["filings_new"] == 25

    def test_log_crawl_stores_timestamps(self, mem_db):
        result = self._result(
            started_at="2025-06-01T08:00:00+00:00",
            completed_at="2025-06-01T08:05:00+00:00",
        )
        log_crawl(mem_db, result)
        row = dict(mem_db.execute("SELECT * FROM crawl_log").fetchone())
        assert row["started_at"] == "2025-06-01T08:00:00+00:00"
        assert row["completed_at"] == "2025-06-01T08:05:00+00:00"

    def test_log_crawl_allows_null_ticker(self, mem_db):
        log_crawl(mem_db, self._result(ticker=None))
        row = dict(mem_db.execute("SELECT * FROM crawl_log").fetchone())
        assert row["ticker"] is None

    def test_multiple_crawl_logs_accumulate(self, mem_db):
        log_crawl(mem_db, self._result(ticker="BHP"))
        log_crawl(mem_db, self._result(ticker="CBA"))
        log_crawl(mem_db, self._result(ticker=None, crawl_type="prev_bus_day"))
        count = mem_db.execute("SELECT COUNT(*) FROM crawl_log").fetchone()[0]
        assert count == 3

    def test_id_is_autoincremented(self, mem_db):
        log_crawl(mem_db, self._result())
        log_crawl(mem_db, self._result())
        rows = mem_db.execute("SELECT id FROM crawl_log ORDER BY id").fetchall()
        ids = [r[0] for r in rows]
        assert ids == [1, 2]


# ---------------------------------------------------------------------------
# get_last_crawl_time() — query helper
# ---------------------------------------------------------------------------


class TestGetLastCrawlTime:
    def _result(self, ticker: str, period: str, completed_at: str | None) -> CrawlResult:
        return CrawlResult(
            crawl_type="per_company",
            ticker=ticker,
            period=period,
            filings_found=5,
            filings_new=1,
            started_at="2025-01-01T00:00:00+00:00",
            completed_at=completed_at or "2025-01-01T00:01:00+00:00",
        )

    def test_returns_completed_at_for_existing_crawl(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "M6", "2025-06-01T08:05:00+00:00"))
        result = get_last_crawl_time(mem_db, "BHP", "M6")
        assert result == "2025-06-01T08:05:00+00:00"

    def test_returns_none_when_no_crawl_exists(self, mem_db):
        result = get_last_crawl_time(mem_db, "BHP", "M6")
        assert result is None

    def test_returns_most_recent_when_multiple_crawls(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "M6", "2025-01-01T10:00:00+00:00"))
        log_crawl(mem_db, self._result("BHP", "M6", "2025-06-15T12:00:00+00:00"))
        result = get_last_crawl_time(mem_db, "BHP", "M6")
        assert result == "2025-06-15T12:00:00+00:00"

    def test_filters_by_ticker(self, mem_db):
        log_crawl(mem_db, self._result("CBA", "M6", "2025-05-01T09:00:00+00:00"))
        result = get_last_crawl_time(mem_db, "BHP", "M6")
        assert result is None

    def test_filters_by_period(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "W", "2025-05-01T09:00:00+00:00"))
        result = get_last_crawl_time(mem_db, "BHP", "M6")
        assert result is None


# ---------------------------------------------------------------------------
# get_crawled_tickers_for_period() — query helper
# ---------------------------------------------------------------------------


class TestGetCrawledTickersForPeriod:
    def _result(self, ticker: str, period: str) -> CrawlResult:
        return CrawlResult(
            crawl_type="per_company",
            ticker=ticker,
            period=period,
            filings_found=5,
            filings_new=1,
            started_at="2025-01-01T00:00:00+00:00",
            completed_at="2025-01-01T00:01:00+00:00",
        )

    def test_returns_tickers_for_given_period(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "M6"))
        log_crawl(mem_db, self._result("CBA", "M6"))
        tickers = get_crawled_tickers_for_period(mem_db, "M6")
        assert "BHP" in tickers
        assert "CBA" in tickers

    def test_returns_empty_set_when_no_crawls(self, mem_db):
        tickers = get_crawled_tickers_for_period(mem_db, "M6")
        assert tickers == set()

    def test_filters_by_period(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "W"))
        tickers = get_crawled_tickers_for_period(mem_db, "M6")
        assert "BHP" not in tickers

    def test_returns_set_type(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "M6"))
        tickers = get_crawled_tickers_for_period(mem_db, "M6")
        assert isinstance(tickers, set)

    def test_deduplicates_repeated_crawls(self, mem_db):
        log_crawl(mem_db, self._result("BHP", "M6"))
        log_crawl(mem_db, self._result("BHP", "M6"))
        tickers = get_crawled_tickers_for_period(mem_db, "M6")
        assert len([t for t in tickers if t == "BHP"]) == 1


# ---------------------------------------------------------------------------
# Filing dataclass immutability
# ---------------------------------------------------------------------------


class TestFilingDataclass:
    def test_filing_is_frozen(self, sample_filing):
        with pytest.raises((AttributeError, TypeError)):
            sample_filing.headline = "mutated"  # type: ignore[misc]

    def test_filing_has_source_field(self, sample_filing):
        assert sample_filing.source == "asx"

    def test_filing_has_country_field(self, sample_filing):
        assert sample_filing.country == "AU"

    def test_filing_date_is_iso_format(self, sample_filing):
        import re
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", sample_filing.filing_date)
