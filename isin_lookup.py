"""
isin_lookup.py — Session-scoped ISIN lookup table for the ASX scraper.

Downloads the ASX ISIN.xls bulk file once at startup, parses it with xlrd,
and exposes a simple get_isin(ticker) function.  If the download or parse
fails the module logs a warning and all lookups return None — the scraper
continues without ISIN data rather than hard-failing.

Usage::

    from isin_lookup import load_isin_map, get_isin

    isin_map = load_isin_map(session)          # call once at startup
    isin = get_isin("BHP", isin_map)           # O(1) dict lookup per filing
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import requests

log = logging.getLogger("asx_scraper")

_ISIN_XLS_URL = "https://www.asx.com.au/content/dam/asx/issuers/ISIN.xls"
_REQUEST_TIMEOUT = 20  # seconds
_ORDINARY_FULLY_PAID = "ORDINARY FULLY PAID"

# Expected column headers in the ASX ISIN.xls spreadsheet.
_COL_TICKER = "ASX code"
_COL_SEC_TYPE = "Security type"
_COL_ISIN = "ISIN code"


def load_isin_map(session: requests.Session) -> dict[str, str]:
    """Download and parse the ASX ISIN.xls file into a ticker-to-ISIN dict.

    Filters to rows where security type is "ORDINARY FULLY PAID".  Returns
    an empty dict on any failure (network error, missing xlrd, bad format)
    so callers can fall back to isin=None without crashing.

    Args:
        session: A configured requests.Session used for the HTTP download.

    Returns:
        ``{ticker: isin}`` mapping, potentially empty on failure.
    """
    raw = _download_xls(session)
    if raw is None:
        return {}
    return _parse_xls(raw)


def get_isin(ticker: str, isin_map: dict[str, str]) -> str | None:
    """Look up the ISIN for a given ASX ticker code.

    Args:
        ticker:   ASX ticker symbol (e.g. ``"BHP"``).
        isin_map: Mapping returned by :func:`load_isin_map`.

    Returns:
        ISIN string when found, otherwise ``None``.
    """
    return isin_map.get(ticker.strip().upper())


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _download_xls(session: requests.Session) -> bytes | None:
    """Fetch the ASX ISIN.xls file.

    Args:
        session: Configured requests.Session.

    Returns:
        Raw bytes on success, ``None`` on any network or HTTP error.
    """
    try:
        resp = session.get(_ISIN_XLS_URL, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        log.info("ASX ISIN.xls downloaded (%d bytes)", len(resp.content))
        return resp.content
    except Exception as exc:  # noqa: BLE001
        log.warning("ASX ISIN.xls download failed — ISIN data unavailable: %s", exc)
        return None


def _parse_xls(data: bytes) -> dict[str, str]:
    """Parse raw XLS bytes into a ``{ticker: isin}`` dict.

    Args:
        data: Raw bytes of the ASX ISIN.xls OLE2 file.

    Returns:
        Mapping of ticker to ISIN string.  Returns ``{}`` on any parse error.
    """
    try:
        import xlrd  # noqa: PLC0415
    except ImportError:
        log.warning(
            "xlrd is not installed — ASX ISIN.xls cannot be parsed. "
            "Install it with: pip install xlrd"
        )
        return {}

    try:
        workbook = xlrd.open_workbook(file_contents=data)
    except Exception as exc:  # noqa: BLE001
        log.warning("ASX ISIN.xls could not be opened: %s", exc)
        return {}

    sheet = workbook.sheet_by_index(0)
    if sheet.nrows < 2:
        log.warning("ASX ISIN.xls has fewer than 2 rows — empty file?")
        return {}

    headers = [str(sheet.cell_value(0, col)).strip() for col in range(sheet.ncols)]

    required = {_COL_TICKER, _COL_SEC_TYPE, _COL_ISIN}
    missing = required - set(headers)
    if missing:
        log.warning("ASX ISIN.xls missing expected columns: %s", missing)
        return {}

    idx_ticker = headers.index(_COL_TICKER)
    idx_sec_type = headers.index(_COL_SEC_TYPE)
    idx_isin = headers.index(_COL_ISIN)

    result: dict[str, str] = {}
    for row in range(1, sheet.nrows):
        sec_type = str(sheet.cell_value(row, idx_sec_type)).strip().upper()
        if sec_type != _ORDINARY_FULLY_PAID:
            continue
        ticker = str(sheet.cell_value(row, idx_ticker)).strip().upper()
        isin = str(sheet.cell_value(row, idx_isin)).strip()
        if ticker and isin:
            result[ticker] = isin

    log.info("ASX ISIN map built: %d entries", len(result))
    return result
