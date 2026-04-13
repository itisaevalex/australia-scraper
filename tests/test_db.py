"""
test_db.py — Unit tests for all database operations in db.py.

Uses in-memory SQLite so tests are fully isolated and fast.

Tests cover:
  - get_db()               : schema creation
  - upsert_announcement()  : INSERT OR IGNORE semantics
  - mark_downloaded()      : UPDATE semantics
  - fetch_undownloaded()   : SELECT with filters
  - log_crawl()            : crawl_log INSERT
  - get_last_crawl_time()  : query helper
  - get_crawled_tickers_for_period() : query helper
"""
from __future__ import annotations

import sqlite3

import pytest

from db import (
    Announcement,
    CrawlResult,
    fetch_undownloaded,
    get_crawled_tickers_for_period,
    get_db,
    get_last_crawl_time,
    log_crawl,
    mark_downloaded,
    upsert_announcement,
)


# ---------------------------------------------------------------------------
# Helper: build an Announcement with all required fields
# ---------------------------------------------------------------------------

def _ann(
    ids_id: str,
    asx_code: str = "BHP",
    date: str = "01/01/2025",
    headline: str = "Test Announcement",
    pdf_url: str | None = "http://example.com/doc.pdf",
    price_sensitive: bool = False,
    announcement_type: str = "other",
) -> Announcement:
    return Announcement(
        ids_id=ids_id,
        asx_code=asx_code,
        date=date,
        time=None,
        headline=headline,
        announcement_type=announcement_type,
        pdf_url=pdf_url,
        file_size=None,
        num_pages=None,
        price_sensitive=price_sensitive,
    )


# ---------------------------------------------------------------------------
# get_db() — schema creation
# ---------------------------------------------------------------------------


class TestGetDb:
    def test_creates_announcements_table(self, mem_db):
        # Arrange / Act (mem_db fixture already calls get_db)
        # Assert
        tables = {row[0] for row in mem_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "announcements" in tables

    def test_creates_crawl_log_table(self, mem_db):
        tables = {row[0] for row in mem_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "crawl_log" in tables

    def test_announcements_has_expected_columns(self, mem_db):
        cols = {row[1] for row in mem_db.execute(
            "PRAGMA table_info(announcements)"
        ).fetchall()}
        expected = {
            "ids_id", "asx_code", "date", "time", "headline",
            "announcement_type", "pdf_url", "direct_pdf_url", "file_size",
            "num_pages", "price_sensitive", "downloaded", "download_path",
            "created_at",
        }
        assert expected.issubset(cols)

    def test_crawl_log_has_expected_columns(self, mem_db):
        cols = {row[1] for row in mem_db.execute(
            "PRAGMA table_info(crawl_log)"
        ).fetchall()}
        expected = {
            "id", "crawl_type", "ticker", "period",
            "announcements_found", "announcements_new",
            "started_at", "completed_at",
        }
        assert expected.issubset(cols)

    def test_ids_id_is_primary_key(self, mem_db):
        pk_cols = [
            row[1]
            for row in mem_db.execute("PRAGMA table_info(announcements)").fetchall()
            if row[5] == 1  # pk flag
        ]
        assert "ids_id" in pk_cols

    def test_calling_get_db_with_memory_twice_is_safe(self):
        # get_db uses CREATE TABLE IF NOT EXISTS — idempotent
        conn1 = get_db(":memory:")  # type: ignore[arg-type]
        conn2 = get_db(":memory:")  # type: ignore[arg-type]
        for conn in (conn1, conn2):
            tables = [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            assert tables.count("announcements") == 1
        conn1.close()
        conn2.close()

    def test_migrations_applied_on_new_db(self, mem_db):
        # The announcement_type migration should already be applied by get_db
        cols = {row[1].lower() for row in mem_db.execute(
            "PRAGMA table_info(announcements)"
        ).fetchall()}
        assert "announcement_type" in cols

    def test_migration_error_does_not_crash(self):
        """When a migration fails (e.g. column already exists with different type),
        get_db should log a warning and continue rather than raise."""
        import sqlite3 as _sqlite3
        from unittest.mock import patch

        conn = get_db(":memory:")  # type: ignore[arg-type]

        # Simulate a second call to _apply_migrations where the column already exists
        # (the normal idempotency path). get_db should not raise.
        from db import _apply_migrations
        # Calling it again should be safe (column already exists — OperationalError caught)
        try:
            _apply_migrations(conn)  # should not raise
        except Exception as exc:
            pytest.fail(f"_apply_migrations raised unexpectedly: {exc}")
        conn.close()


# ---------------------------------------------------------------------------
# upsert_announcement() — INSERT OR IGNORE
# ---------------------------------------------------------------------------


class TestUpsertAnnouncement:
    def test_new_announcement_returns_true(self, mem_db, sample_announcement):
        result = upsert_announcement(mem_db, sample_announcement)
        assert result is True

    def test_duplicate_announcement_returns_false(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        result = upsert_announcement(mem_db, sample_announcement)
        assert result is False

    def test_row_is_stored_after_insert(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        row = mem_db.execute(
            "SELECT * FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone()
        assert row is not None

    def test_stored_row_matches_inserted_data(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        row = dict(mem_db.execute(
            "SELECT * FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone())
        assert row["ids_id"] == sample_announcement.ids_id
        assert row["asx_code"] == sample_announcement.asx_code
        assert row["date"] == sample_announcement.date
        assert row["time"] == sample_announcement.time
        assert row["headline"] == sample_announcement.headline
        assert row["pdf_url"] == sample_announcement.pdf_url
        assert row["file_size"] == sample_announcement.file_size
        assert row["num_pages"] == sample_announcement.num_pages
        assert row["announcement_type"] == sample_announcement.announcement_type

    def test_price_sensitive_false_stored_correctly(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        row = dict(mem_db.execute(
            "SELECT price_sensitive FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone())
        assert bool(row["price_sensitive"]) is False

    def test_price_sensitive_true_stored_correctly(self, mem_db):
        ann = _ann("PS000001", asx_code="CBA", price_sensitive=True)
        upsert_announcement(mem_db, ann)
        row = dict(mem_db.execute(
            "SELECT price_sensitive FROM announcements WHERE ids_id = ?",
            (ann.ids_id,),
        ).fetchone())
        assert bool(row["price_sensitive"]) is True

    def test_two_different_ids_ids_both_stored(self, mem_db):
        upsert_announcement(mem_db, _ann("AAAA0001", asx_code="BHP"))
        upsert_announcement(mem_db, _ann("BBBB0002", asx_code="CBA"))
        count = mem_db.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
        assert count == 2

    def test_downloaded_defaults_to_false(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        row = dict(mem_db.execute(
            "SELECT downloaded FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone())
        assert bool(row["downloaded"]) is False

    def test_null_pdf_url_stored_correctly(self, mem_db):
        ann = _ann("NULL0001", pdf_url=None)
        upsert_announcement(mem_db, ann)
        row = dict(mem_db.execute(
            "SELECT pdf_url FROM announcements WHERE ids_id = ?", (ann.ids_id,)
        ).fetchone())
        assert row["pdf_url"] is None

    def test_announcement_type_stored_correctly(self, mem_db):
        ann = _ann("TYPE0001", announcement_type="annual_report")
        upsert_announcement(mem_db, ann)
        row = dict(mem_db.execute(
            "SELECT announcement_type FROM announcements WHERE ids_id = ?", (ann.ids_id,)
        ).fetchone())
        assert row["announcement_type"] == "annual_report"


# ---------------------------------------------------------------------------
# mark_downloaded() — UPDATE after successful download
# ---------------------------------------------------------------------------


class TestMarkDownloaded:
    def test_mark_downloaded_sets_downloaded_true(self, mem_db, sample_announcement):
        # Arrange
        upsert_announcement(mem_db, sample_announcement)
        # Act
        mark_downloaded(
            mem_db,
            sample_announcement.ids_id,
            "https://cdn.example.com/file.pdf",
            "/tmp/docs/BHP/12345678.pdf",
        )
        # Assert
        row = dict(mem_db.execute(
            "SELECT downloaded FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone())
        assert bool(row["downloaded"]) is True

    def test_mark_downloaded_sets_direct_pdf_url(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        direct_url = "https://announcements.asx.com.au/asxpdf/20260413/pdf/abc123.pdf"
        mark_downloaded(mem_db, sample_announcement.ids_id, direct_url, "/tmp/path.pdf")
        row = dict(mem_db.execute(
            "SELECT direct_pdf_url FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone())
        assert row["direct_pdf_url"] == direct_url

    def test_mark_downloaded_sets_download_path(self, mem_db, sample_announcement):
        upsert_announcement(mem_db, sample_announcement)
        path = "/tmp/documents/BHP/12345678.pdf"
        mark_downloaded(mem_db, sample_announcement.ids_id, "https://cdn.example.com/x.pdf", path)
        row = dict(mem_db.execute(
            "SELECT download_path FROM announcements WHERE ids_id = ?",
            (sample_announcement.ids_id,),
        ).fetchone())
        assert row["download_path"] == path

    def test_mark_downloaded_does_not_affect_other_rows(self, mem_db):
        upsert_announcement(mem_db, _ann("MARK0001", asx_code="BHP"))
        upsert_announcement(mem_db, _ann("MARK0002", asx_code="CBA"))
        mark_downloaded(mem_db, "MARK0001", "https://cdn.com/a.pdf", "/tmp/a.pdf")
        row2 = dict(mem_db.execute(
            "SELECT downloaded FROM announcements WHERE ids_id = ?", ("MARK0002",)
        ).fetchone())
        assert bool(row2["downloaded"]) is False


# ---------------------------------------------------------------------------
# fetch_undownloaded() — SELECT pending downloads
# ---------------------------------------------------------------------------


class TestFetchUndownloaded:
    def _insert(
        self,
        conn: sqlite3.Connection,
        ids_id: str,
        asx_code: str = "BHP",
        pdf_url: str | None = "http://example.com/doc.pdf",
        downloaded: bool = False,
    ) -> None:
        upsert_announcement(conn, _ann(ids_id, asx_code=asx_code, pdf_url=pdf_url))
        if downloaded:
            mark_downloaded(conn, ids_id, "https://cdn.com/x.pdf", f"/tmp/{ids_id}.pdf")

    def test_returns_rows_with_pdf_url_and_downloaded_false(self, mem_db):
        self._insert(mem_db, "FETCH001")
        rows = fetch_undownloaded(mem_db)
        assert len(rows) == 1
        assert rows[0]["ids_id"] == "FETCH001"

    def test_excludes_already_downloaded_rows(self, mem_db):
        self._insert(mem_db, "DONE001", downloaded=True)
        rows = fetch_undownloaded(mem_db)
        ids = [r["ids_id"] for r in rows]
        assert "DONE001" not in ids

    def test_excludes_rows_without_pdf_url(self, mem_db):
        self._insert(mem_db, "NOPDF01", pdf_url=None)
        rows = fetch_undownloaded(mem_db)
        ids = [r["ids_id"] for r in rows]
        assert "NOPDF01" not in ids

    def test_returns_empty_list_when_no_pending(self, mem_db):
        self._insert(mem_db, "DONE002", downloaded=True)
        rows = fetch_undownloaded(mem_db)
        assert rows == []

    def test_ticker_filter_returns_only_matching_rows(self, mem_db):
        self._insert(mem_db, "BHP0001", asx_code="BHP")
        self._insert(mem_db, "CBA0001", asx_code="CBA")
        rows = fetch_undownloaded(mem_db, ticker="BHP")
        assert all(r["asx_code"] == "BHP" for r in rows)
        assert len(rows) == 1

    def test_ticker_filter_excludes_other_tickers(self, mem_db):
        self._insert(mem_db, "BHP0002", asx_code="BHP")
        self._insert(mem_db, "CBA0002", asx_code="CBA")
        rows = fetch_undownloaded(mem_db, ticker="BHP")
        ids = [r["ids_id"] for r in rows]
        assert "CBA0002" not in ids

    def test_no_ticker_filter_returns_all_pending(self, mem_db):
        self._insert(mem_db, "MIX0001", asx_code="BHP")
        self._insert(mem_db, "MIX0002", asx_code="CBA")
        rows = fetch_undownloaded(mem_db)
        assert len(rows) == 2

    def test_rows_are_returned_as_dicts(self, mem_db):
        self._insert(mem_db, "DICT001")
        rows = fetch_undownloaded(mem_db)
        assert isinstance(rows[0], dict)
        assert "ids_id" in rows[0]
        assert "asx_code" in rows[0]


# ---------------------------------------------------------------------------
# log_crawl() — INSERT into crawl_log
# ---------------------------------------------------------------------------


class TestLogCrawl:
    def _result(self, **kwargs) -> CrawlResult:
        defaults = dict(
            crawl_type="per_company",
            ticker="BHP",
            period="M6",
            announcements_found=10,
            announcements_new=3,
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
        log_crawl(mem_db, self._result(announcements_found=100, announcements_new=25))
        row = dict(mem_db.execute("SELECT * FROM crawl_log").fetchone())
        assert row["announcements_found"] == 100
        assert row["announcements_new"] == 25

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
            announcements_found=5,
            announcements_new=1,
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
            announcements_found=5,
            announcements_new=1,
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
        # Same ticker crawled twice — should appear only once in the set
        log_crawl(mem_db, self._result("BHP", "M6"))
        log_crawl(mem_db, self._result("BHP", "M6"))
        tickers = get_crawled_tickers_for_period(mem_db, "M6")
        assert tickers.count("BHP") == 1 if hasattr(tickers, "count") else len([t for t in tickers if t == "BHP"]) == 1
