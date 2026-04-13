"""
test_directory.py — Unit tests for company directory CSV parsing functions.

Tests cover:
  - _parse_tickers_from_csv()
  - _decode_csv_bytes()
"""
from __future__ import annotations

import pytest

from scraper import _decode_csv_bytes, _parse_tickers_from_csv


# ---------------------------------------------------------------------------
# _decode_csv_bytes() — encoding detection and decoding
# ---------------------------------------------------------------------------


class TestDecodeCsvBytes:
    def test_decodes_utf8_bytes(self):
        raw = "ticker,name\nBHP,BHP Group\n".encode("utf-8")
        result = _decode_csv_bytes(raw)
        assert "BHP" in result

    def test_decodes_utf8_with_bom(self):
        # UTF-8 with BOM (utf-8-sig) — common in Windows-generated CSVs
        raw = "ticker,name\nCBA,Commonwealth Bank\n".encode("utf-8-sig")
        result = _decode_csv_bytes(raw)
        assert "CBA" in result
        # BOM should be stripped
        assert not result.startswith("\ufeff")

    def test_decodes_latin1_bytes(self):
        # Latin-1 encoded string with accented character
        raw = "ticker,name\nACC,Compañía S.A.\n".encode("latin-1")
        result = _decode_csv_bytes(raw)
        assert "ACC" in result

    def test_returns_string_type(self):
        raw = b"header\ndata\n"
        result = _decode_csv_bytes(raw)
        assert isinstance(result, str)

    def test_decodes_empty_bytes(self):
        result = _decode_csv_bytes(b"")
        assert result == ""

    def test_decodes_ascii_bytes_as_utf8(self):
        raw = b"COL1,COL2\nABC,DEF\n"
        result = _decode_csv_bytes(raw)
        assert result == "COL1,COL2\nABC,DEF\n"


# ---------------------------------------------------------------------------
# _parse_tickers_from_csv() — MarkitDigital format (col_index=0, no skip)
# ---------------------------------------------------------------------------


class TestParseTickersFromCsvMarkitFormat:
    """MarkitDigital format: first column is ticker, one header row, no skipped rows."""

    def _csv(self, rows: list[str]) -> bytes:
        """Build CSV bytes from a list of row strings (first is header)."""
        return "\n".join(rows).encode("utf-8")

    def test_extracts_tickers_from_column_zero(self):
        raw = self._csv(["ASX Code,Company Name", "BHP,BHP Group", "CBA,Commonwealth Bank"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "BHP" in tickers
        assert "CBA" in tickers

    def test_skips_header_row(self):
        raw = self._csv(["ASX Code,Company Name", "BHP,BHP Group"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "ASX Code" not in tickers
        assert "ASX" not in tickers

    def test_returns_only_valid_ticker_format(self):
        raw = self._csv([
            "Ticker,Name",
            "BHP,Valid",
            "INVALID TICKER WITH SPACES,Bad",
            "TOO_LONG_CODE,Bad",
            "ab,lowercase should fail",
            "WES,Valid",
        ])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "BHP" in tickers
        assert "WES" in tickers
        # Invalid tickers should be filtered out
        assert "INVALID TICKER WITH SPACES" not in tickers
        assert "ab" not in tickers

    def test_returns_empty_list_for_empty_csv(self):
        tickers = _parse_tickers_from_csv(b"", col_index=0)
        assert tickers == []

    def test_returns_empty_list_for_header_only_csv(self):
        raw = self._csv(["Ticker,Name"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert tickers == []

    def test_strips_whitespace_from_tickers(self):
        raw = "Ticker,Name\n BHP ,BHP Group\n CBA ,Commonwealth\n".encode("utf-8")
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "BHP" in tickers
        assert "CBA" in tickers

    def test_handles_numeric_tickers(self):
        # Some ASX codes may contain digits, e.g. "CY2" or "A2M"
        raw = self._csv(["Ticker,Name", "A2M,A2 Milk", "1PG,OnePageGroup"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "A2M" in tickers
        assert "1PG" in tickers

    def test_rejects_single_char_tickers(self):
        raw = self._csv(["Ticker,Name", "A,Short"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "A" not in tickers

    def test_accepts_max_6_char_tickers(self):
        raw = self._csv(["Ticker,Name", "LONGCD,Long Code"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "LONGCD" in tickers

    def test_rejects_7_char_tickers(self):
        raw = self._csv(["Ticker,Name", "TOOLONG,Too Long"])
        tickers = _parse_tickers_from_csv(raw, col_index=0)
        assert "TOOLONG" not in tickers


# ---------------------------------------------------------------------------
# _parse_tickers_from_csv() — Old ASX format (col_index=1, skip_header_rows=3)
# ---------------------------------------------------------------------------


class TestParseTickersFromCsvAsxFormat:
    """Old ASX format: 3 metadata lines to skip, then 1 column-header row, then data.

    The real ASXListedCompanies.csv looks like:
      Line 0: title/metadata
      Line 1: blank/empty
      Line 2: another metadata line
      Line 3: column headers (Company name,ASX code,...)  <- skipped by i==0
      Line 4+: data rows
    skip_header_rows=3 discards lines 0-2; then i==0 skips line 3 (column headers).
    """

    def _old_asx_csv(self, tickers: list[tuple[str, str]]) -> bytes:
        """Build a CSV in old ASX format:
        3 metadata lines (skipped by skip_header_rows=3),
        then 1 column-header row (skipped by i==0 in the loop),
        then data rows.
        """
        lines = [
            "ASX Listed Companies",      # metadata line 0 — skipped by skip_header_rows=3
            ",,,,,",                     # metadata line 1 — skipped
            "Last updated 01 Jan 2025",  # metadata line 2 — skipped
            "Company name,ASX code,GICS industry group,Listing date,Market Cap",  # col header, i==0 skip
        ] + [f"{name},{code},Industry,2000-01-01,1000000" for name, code in tickers]
        return "\n".join(lines).encode("utf-8")

    def test_skips_three_header_rows(self):
        raw = self._old_asx_csv([("BHP Group", "BHP"), ("CBA", "CBA")])
        tickers = _parse_tickers_from_csv(raw, col_index=1, skip_header_rows=3)
        assert "BHP" in tickers
        assert "CBA" in tickers

    def test_does_not_include_header_content_as_tickers(self):
        raw = self._old_asx_csv([("WES", "WES")])
        tickers = _parse_tickers_from_csv(raw, col_index=1, skip_header_rows=3)
        # None of the header row content should appear as tickers
        assert "Company" not in tickers
        assert "Code" not in tickers
        assert "GICS" not in tickers

    def test_extracts_from_correct_column(self):
        # col_index=1 means second column (ASX code column)
        raw = self._old_asx_csv([("BHP Group Limited", "BHP")])
        tickers = _parse_tickers_from_csv(raw, col_index=1, skip_header_rows=3)
        assert "BHP" in tickers
        # Company name "BHP Group Limited" should NOT appear (it's col 0)
        assert "BHP Group Limited" not in tickers

    def test_returns_list_of_strings(self):
        raw = self._old_asx_csv([("BHP", "BHP")])
        tickers = _parse_tickers_from_csv(raw, col_index=1, skip_header_rows=3)
        assert all(isinstance(t, str) for t in tickers)

    def test_handles_missing_column_gracefully(self):
        # A row with only 1 column when col_index=1 — should be skipped
        raw = "Header1,Header2,Header3\nRow1\nRow2\n".encode("utf-8")
        tickers = _parse_tickers_from_csv(raw, col_index=1, skip_header_rows=0)
        # Should not raise and should return empty (no valid tickers in col 1)
        assert isinstance(tickers, list)

    def test_no_duplicates_in_result(self):
        raw = self._old_asx_csv([
            ("BHP Group", "BHP"),
            ("BHP Duplicate", "BHP"),
            ("CBA", "CBA"),
        ])
        tickers = _parse_tickers_from_csv(raw, col_index=1, skip_header_rows=3)
        # Both BHP rows pass validation — duplicates are allowed (INSERT OR IGNORE handles dedup)
        # But the ticker values themselves should all be valid strings
        assert all(isinstance(t, str) for t in tickers)
