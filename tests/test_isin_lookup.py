"""
test_isin_lookup.py — Unit tests for isin_lookup module.

Tests cover:
  - load_isin_map() — happy path, network failure, xlrd missing, bad format
  - get_isin()      — hit, miss, empty map, case insensitivity
  - _download_xls() — success, HTTP error, exception
  - _parse_xls()    — ordinary-fully-paid filter, missing columns, empty sheet
"""
from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from isin_lookup import _download_xls, _parse_xls, get_isin, load_isin_map


# ---------------------------------------------------------------------------
# Helpers to build a synthetic .xls file in memory
# ---------------------------------------------------------------------------


def _make_xls_bytes(rows: list[list[str]]) -> bytes:
    """Build a minimal valid .xls file using xlwt (or xlrd write support).

    If xlwt is not available we fall back to xlrd's in-memory workbook API.
    The file always has the canonical ASX ISIN.xls header as row 0.

    Args:
        rows: Data rows (not including the header row).

    Returns:
        Raw bytes of a valid OLE2 .xls file.
    """
    import xlwt  # noqa: PLC0415

    header = ["ASX code", "Company name", "Security type", "ISIN code"]
    wb = xlwt.Workbook()
    ws = wb.add_sheet("Sheet1")
    for col, h in enumerate(header):
        ws.write(0, col, h)
    for row_idx, row_data in enumerate(rows, start=1):
        for col, val in enumerate(row_data):
            ws.write(row_idx, col, val)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _bhp_row() -> list[str]:
    return ["BHP", "BHP Group Limited", "ORDINARY FULLY PAID", "AU000000BHP4"]


def _cba_row() -> list[str]:
    return ["CBA", "Commonwealth Bank of Australia", "ORDINARY FULLY PAID", "AU000000CBA7"]


def _warrant_row() -> list[str]:
    return ["BHPWA", "BHP Warrant", "WARRANT", "AU0000BHPWA2"]


# ---------------------------------------------------------------------------
# get_isin() — pure dict lookup
# ---------------------------------------------------------------------------


class TestGetIsin:
    def test_returns_isin_for_known_ticker(self):
        isin_map = {"BHP": "AU000000BHP4", "CBA": "AU000000CBA7"}
        assert get_isin("BHP", isin_map) == "AU000000BHP4"

    def test_returns_none_for_unknown_ticker(self):
        isin_map = {"BHP": "AU000000BHP4"}
        assert get_isin("XYZ", isin_map) is None

    def test_returns_none_for_empty_map(self):
        assert get_isin("BHP", {}) is None

    def test_case_insensitive_lookup(self):
        isin_map = {"BHP": "AU000000BHP4"}
        assert get_isin("bhp", isin_map) == "AU000000BHP4"
        assert get_isin("Bhp", isin_map) == "AU000000BHP4"

    def test_strips_whitespace_from_ticker(self):
        isin_map = {"BHP": "AU000000BHP4"}
        assert get_isin("  BHP  ", isin_map) == "AU000000BHP4"

    def test_returns_correct_isin_for_multiple_tickers(self):
        isin_map = {"BHP": "AU000000BHP4", "CBA": "AU000000CBA7"}
        assert get_isin("CBA", isin_map) == "AU000000CBA7"


# ---------------------------------------------------------------------------
# _download_xls() — HTTP layer
# ---------------------------------------------------------------------------


class TestDownloadXls:
    def _mock_session(self, content: bytes = b"xls", status: int = 200) -> MagicMock:
        session = MagicMock()
        resp = MagicMock()
        resp.content = content
        resp.status_code = status
        resp.raise_for_status = MagicMock()
        if status >= 400:
            import requests
            resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
        session.get.return_value = resp
        return session

    def test_returns_bytes_on_success(self):
        session = self._mock_session(content=b"fake-xls-data")
        result = _download_xls(session)
        assert result == b"fake-xls-data"

    def test_returns_none_on_http_error(self):
        session = self._mock_session(status=404)
        result = _download_xls(session)
        assert result is None

    def test_returns_none_on_connection_error(self):
        import requests as rq
        session = MagicMock()
        session.get.side_effect = rq.ConnectionError("connection refused")
        result = _download_xls(session)
        assert result is None

    def test_requests_correct_url(self):
        session = self._mock_session()
        _download_xls(session)
        call_url = session.get.call_args[0][0]
        assert "ISIN.xls" in call_url
        assert "asx.com.au" in call_url


# ---------------------------------------------------------------------------
# _parse_xls() — spreadsheet parsing
# ---------------------------------------------------------------------------


class TestParseXls:
    @pytest.fixture(autouse=True)
    def _require_xlwt(self):
        pytest.importorskip("xlwt", reason="xlwt required to build test XLS fixtures")

    def test_parses_ordinary_fully_paid_rows(self):
        data = _make_xls_bytes([_bhp_row(), _cba_row()])
        result = _parse_xls(data)
        assert result == {"BHP": "AU000000BHP4", "CBA": "AU000000CBA7"}

    def test_filters_out_non_ordinary_rows(self):
        data = _make_xls_bytes([_bhp_row(), _warrant_row()])
        result = _parse_xls(data)
        assert "BHPWA" not in result
        assert result == {"BHP": "AU000000BHP4"}

    def test_returns_empty_dict_for_empty_sheet(self):
        # Sheet with header only, no data rows
        data = _make_xls_bytes([])
        result = _parse_xls(data)
        assert result == {}

    def test_returns_empty_dict_when_xlrd_unavailable(self):
        with patch.dict("sys.modules", {"xlrd": None}):
            with patch("builtins.__import__", side_effect=ImportError("no module named xlrd")):
                result = _parse_xls(b"irrelevant")
        assert result == {}

    def test_returns_empty_dict_on_corrupt_bytes(self):
        result = _parse_xls(b"this is not a valid xls file")
        assert result == {}

    def test_keys_are_uppercase(self):
        # Ensure ticker keys come back in uppercase regardless of sheet content
        import xlwt
        wb = xlwt.Workbook()
        ws = wb.add_sheet("Sheet1")
        for col, h in enumerate(["ASX code", "Company name", "Security type", "ISIN code"]):
            ws.write(0, col, h)
        ws.write(1, 0, "bhp")  # lowercase in file
        ws.write(1, 1, "BHP Group")
        ws.write(1, 2, "ORDINARY FULLY PAID")
        ws.write(1, 3, "AU000000BHP4")
        buf = BytesIO()
        wb.save(buf)
        result = _parse_xls(buf.getvalue())
        assert "BHP" in result


# ---------------------------------------------------------------------------
# load_isin_map() — integration of download + parse
# ---------------------------------------------------------------------------


class TestLoadIsinMap:
    @pytest.fixture(autouse=True)
    def _require_xlwt(self):
        pytest.importorskip("xlwt", reason="xlwt required to build test XLS fixtures")

    def _session_with_xls(self, rows: list[list[str]]) -> MagicMock:
        data = _make_xls_bytes(rows)
        session = MagicMock()
        resp = MagicMock()
        resp.content = data
        resp.raise_for_status = MagicMock()
        session.get.return_value = resp
        return session

    def _failing_session(self) -> MagicMock:
        import requests as rq
        session = MagicMock()
        session.get.side_effect = rq.ConnectionError("network unreachable")
        return session

    def test_returns_populated_map_on_success(self):
        session = self._session_with_xls([_bhp_row(), _cba_row()])
        result = load_isin_map(session)
        assert result["BHP"] == "AU000000BHP4"
        assert result["CBA"] == "AU000000CBA7"

    def test_returns_empty_dict_on_download_failure(self):
        result = load_isin_map(self._failing_session())
        assert result == {}

    def test_returns_empty_dict_on_empty_spreadsheet(self):
        session = self._session_with_xls([])
        result = load_isin_map(session)
        assert result == {}

    def test_excludes_non_ordinary_securities(self):
        session = self._session_with_xls([_bhp_row(), _warrant_row()])
        result = load_isin_map(session)
        assert "BHPWA" not in result

    def test_returns_empty_dict_when_xlrd_missing(self):
        import sys
        session = self._session_with_xls([_bhp_row()])
        original = sys.modules.get("xlrd")
        sys.modules["xlrd"] = None  # type: ignore[assignment]
        try:
            result = load_isin_map(session)
        finally:
            if original is None:
                sys.modules.pop("xlrd", None)
            else:
                sys.modules["xlrd"] = original
        assert result == {}
