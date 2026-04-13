"""
test_scraper.py — Unit tests for scraper.py CLI logic.

Tests cover:
  - build_parser()       : argument parsing including --json flag on stats
  - _do_stats_json()     : JSON output structure and fields
  - _compute_health*()   : health detection from crawl_log
  - _do_export()         : ISO date filtering in SQL
  - date helpers         : _parse_asx_date, _parse_filter_date
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from db import CrawlResult, Filing, get_db, log_crawl, upsert_filing
from scraper import (
    _compute_health_from_logs,
    _do_export,
    _do_stats,
    _do_stats_json,
    _parse_asx_date,
    _parse_filter_date,
    build_parser,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_filing(
    filing_id: str,
    ticker: str = "BHP",
    filing_date: str = "2025-01-01",
    document_url: str | None = "https://cdn.example.com/doc.pdf",
) -> Filing:
    return Filing(
        filing_id=filing_id,
        source="asx",
        country="AU",
        ticker=ticker,
        filing_date=filing_date,
        filing_time=None,
        headline=f"Headline {filing_id}",
        filing_type="other",
        document_url=document_url,
        file_size=None,
        num_pages=None,
        price_sensitive=False,
    )


def _make_crawl_result(
    completed_at: str | None = None,
    filings_found: int = 10,
    filings_new: int = 5,
    crawl_type: str = "per_company",
    ticker: str = "BHP",
    period: str = "M6",
) -> CrawlResult:
    started = datetime.now(timezone.utc).isoformat()
    if completed_at is None:
        completed_at = datetime.now(timezone.utc).isoformat()
    return CrawlResult(
        crawl_type=crawl_type,
        ticker=ticker,
        period=period,
        filings_found=filings_found,
        filings_new=filings_new,
        started_at=started,
        completed_at=completed_at,
    )


# ---------------------------------------------------------------------------
# build_parser() — argument parsing
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_stats_subcommand_exists(self):
        parser = build_parser()
        args = parser.parse_args(["stats"])
        assert args.command == "stats"

    def test_stats_json_flag_defaults_false(self):
        parser = build_parser()
        args = parser.parse_args(["stats"])
        assert args.json_output is False

    def test_stats_json_flag_set_when_passed(self):
        parser = build_parser()
        args = parser.parse_args(["stats", "--json"])
        assert args.json_output is True

    def test_crawl_subcommand_exists(self):
        parser = build_parser()
        args = parser.parse_args(["crawl"])
        assert args.command == "crawl"

    def test_export_subcommand_exists(self):
        parser = build_parser()
        args = parser.parse_args(["export"])
        assert args.command == "export"

    def test_export_from_date_flag(self):
        parser = build_parser()
        args = parser.parse_args(["export", "--from-date", "2025-01-01"])
        assert args.from_date == "2025-01-01"

    def test_export_to_date_flag(self):
        parser = build_parser()
        args = parser.parse_args(["export", "--to-date", "2025-12-31"])
        assert args.to_date == "2025-12-31"


# ---------------------------------------------------------------------------
# _compute_health_from_logs() — health detection
# ---------------------------------------------------------------------------


class TestComputeHealth:
    def test_empty_returns_empty(self):
        conn = get_db(":memory:")
        health = _compute_health_from_logs(conn)
        assert health == "empty"
        conn.close()

    def test_ok_when_recent_crawl(self):
        conn = get_db(":memory:")
        log_crawl(conn, _make_crawl_result())
        health = _compute_health_from_logs(conn)
        assert health == "ok"
        conn.close()

    def test_stale_when_crawl_over_48h_ago(self):
        conn = get_db(":memory:")
        old_dt = datetime.now(timezone.utc) - timedelta(hours=50)
        log_crawl(conn, _make_crawl_result(completed_at=old_dt.isoformat()))
        health = _compute_health_from_logs(conn)
        assert health == "stale"
        conn.close()

    def test_error_when_completed_at_is_null(self):
        conn = get_db(":memory:")
        # Insert a crawl_log row with NULL completed_at directly
        conn.execute(
            "INSERT INTO crawl_log (crawl_type, ticker, period, "
            "filings_found, filings_new, started_at, completed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("per_company", "BHP", "M6", 5, 2,
             datetime.now(timezone.utc).isoformat(), None),
        )
        conn.commit()
        health = _compute_health_from_logs(conn)
        assert health == "error"
        conn.close()

    def test_uses_most_recent_crawl_for_health(self):
        conn = get_db(":memory:")
        # Old stale crawl first, then fresh crawl
        old_dt = datetime.now(timezone.utc) - timedelta(hours=72)
        log_crawl(conn, _make_crawl_result(completed_at=old_dt.isoformat()))
        log_crawl(conn, _make_crawl_result())  # recent
        health = _compute_health_from_logs(conn)
        assert health == "ok"
        conn.close()


# ---------------------------------------------------------------------------
# _do_stats_json() — JSON output
# ---------------------------------------------------------------------------


class TestDoStatsJson:
    def test_outputs_valid_json(self, mem_db, capsys):
        _do_stats_json(mem_db)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, dict)

    def test_json_has_required_keys(self, mem_db, capsys):
        _do_stats_json(mem_db)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        required_keys = {
            "scraper", "country", "sources",
            "total_filings", "downloaded", "pending_download",
            "unique_companies", "total_crawl_runs",
            "earliest_record", "latest_record",
            "db_size_bytes", "documents_size_bytes",
            "health",
        }
        assert required_keys.issubset(data.keys())

    def test_scraper_field_is_australia_scraper(self, mem_db, capsys):
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["scraper"] == "australia-scraper"

    def test_country_is_AU(self, mem_db, capsys):
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["country"] == "AU"

    def test_sources_contains_asx(self, mem_db, capsys):
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert "asx" in data["sources"]

    def test_total_filings_zero_on_empty_db(self, mem_db, capsys):
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["total_filings"] == 0

    def test_total_filings_counts_inserted_rows(self, mem_db, capsys):
        upsert_filing(mem_db, _make_filing("F001"))
        upsert_filing(mem_db, _make_filing("F002"))
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["total_filings"] == 2

    def test_pending_download_equals_total_minus_downloaded(self, mem_db, capsys):
        upsert_filing(mem_db, _make_filing("F001"))
        upsert_filing(mem_db, _make_filing("F002"))
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["pending_download"] == data["total_filings"] - data["downloaded"]

    def test_health_empty_on_new_db(self, mem_db, capsys):
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["health"] == "empty"

    def test_health_ok_after_crawl(self, mem_db, capsys):
        log_crawl(mem_db, _make_crawl_result())
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["health"] == "ok"

    def test_health_stale_after_old_crawl(self, mem_db, capsys):
        old_dt = datetime.now(timezone.utc) - timedelta(hours=60)
        log_crawl(mem_db, _make_crawl_result(completed_at=old_dt.isoformat()))
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["health"] == "stale"

    def test_db_size_bytes_is_non_negative_int(self, mem_db, capsys):
        with patch("scraper.DB_PATH", Path("/nonexistent/path/db.sqlite")):
            _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data["db_size_bytes"], int)
        assert data["db_size_bytes"] >= 0

    def test_unique_companies_reflects_distinct_tickers(self, mem_db, capsys):
        upsert_filing(mem_db, _make_filing("F001", ticker="BHP"))
        upsert_filing(mem_db, _make_filing("F002", ticker="BHP"))
        upsert_filing(mem_db, _make_filing("F003", ticker="CBA"))
        _do_stats_json(mem_db)
        data = json.loads(capsys.readouterr().out)
        assert data["unique_companies"] == 2


# ---------------------------------------------------------------------------
# _do_export() — ISO date filtering
# ---------------------------------------------------------------------------


class TestDoExport:
    def _make_args(
        self,
        output: str,
        ticker: str | None = None,
        downloaded_only: bool = False,
        from_date: str | None = None,
        to_date: str | None = None,
    ):
        args = MagicMock()
        args.output = output
        args.ticker = ticker
        args.downloaded_only = downloaded_only
        args.from_date = from_date
        args.to_date = to_date
        return args

    def test_export_all_filings_to_json(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", filing_date="2025-01-15"))
        upsert_filing(mem_db, _make_filing("E002", filing_date="2025-06-20"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file))
        data = json.loads(Path(out_file).read_text())
        assert len(data) == 2

    def test_export_from_date_filters_correctly(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", filing_date="2025-01-01"))
        upsert_filing(mem_db, _make_filing("E002", filing_date="2025-06-01"))
        upsert_filing(mem_db, _make_filing("E003", filing_date="2025-12-31"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file, from_date="2025-06-01"))
        data = json.loads(Path(out_file).read_text())
        ids = [r["filing_id"] for r in data]
        assert "E001" not in ids
        assert "E002" in ids
        assert "E003" in ids

    def test_export_to_date_filters_correctly(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", filing_date="2025-01-01"))
        upsert_filing(mem_db, _make_filing("E002", filing_date="2025-06-01"))
        upsert_filing(mem_db, _make_filing("E003", filing_date="2025-12-31"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file, to_date="2025-06-01"))
        data = json.loads(Path(out_file).read_text())
        ids = [r["filing_id"] for r in data]
        assert "E001" in ids
        assert "E002" in ids
        assert "E003" not in ids

    def test_export_date_range_combined(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", filing_date="2025-01-01"))
        upsert_filing(mem_db, _make_filing("E002", filing_date="2025-06-15"))
        upsert_filing(mem_db, _make_filing("E003", filing_date="2025-12-31"))
        out_file = str(tmp_path / "out.json")
        _do_export(
            mem_db,
            self._make_args(output=out_file, from_date="2025-06-01", to_date="2025-07-01"),
        )
        data = json.loads(Path(out_file).read_text())
        ids = [r["filing_id"] for r in data]
        assert ids == ["E002"]

    def test_export_ticker_filter(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", ticker="BHP"))
        upsert_filing(mem_db, _make_filing("E002", ticker="CBA"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file, ticker="BHP"))
        data = json.loads(Path(out_file).read_text())
        assert all(r["ticker"] == "BHP" for r in data)

    def test_export_output_has_filing_date_field(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", filing_date="2025-03-15"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file))
        data = json.loads(Path(out_file).read_text())
        assert data[0]["filing_date"] == "2025-03-15"

    def test_export_output_has_filing_id_field(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file))
        data = json.loads(Path(out_file).read_text())
        assert data[0]["filing_id"] == "E001"

    def test_export_output_has_country_field(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file))
        data = json.loads(Path(out_file).read_text())
        assert data[0]["country"] == "AU"

    def test_export_ordered_by_filing_date_desc(self, mem_db, tmp_path):
        upsert_filing(mem_db, _make_filing("E001", filing_date="2025-01-01"))
        upsert_filing(mem_db, _make_filing("E002", filing_date="2025-12-31"))
        out_file = str(tmp_path / "out.json")
        _do_export(mem_db, self._make_args(output=out_file))
        data = json.loads(Path(out_file).read_text())
        assert data[0]["filing_id"] == "E002"  # most recent first


# ---------------------------------------------------------------------------
# Date parse helpers
# ---------------------------------------------------------------------------


class TestParseDateHelpers:
    def test_parse_asx_date_handles_iso(self):
        result = _parse_asx_date("2025-06-15")
        assert result == datetime(2025, 6, 15)

    def test_parse_asx_date_handles_legacy_dd_mm_yyyy(self):
        result = _parse_asx_date("15/06/2025")
        assert result == datetime(2025, 6, 15)

    def test_parse_asx_date_returns_none_on_invalid(self):
        result = _parse_asx_date("not-a-date")
        assert result is None

    def test_parse_filter_date_parses_iso(self):
        result = _parse_filter_date("2025-01-15")
        assert result == datetime(2025, 1, 15)

    def test_parse_filter_date_returns_none_on_invalid(self):
        result = _parse_filter_date("15/01/2025")
        assert result is None
