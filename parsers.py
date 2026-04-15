"""
parsers.py — HTML parsing and filing type classification.

Parses two ASX HTML endpoints:
  - announcements.do  (per-company view)
  - prevBusDayAnns.do (all-company previous business day view)

Also provides filing type classification via regex patterns.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from db import Filing

log = logging.getLogger("asx_scraper")

ASX_BASE = "https://www.asx.com.au"

try:
    import lxml  # noqa: F401
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"

# ---------------------------------------------------------------------------
# Filing type classification
# ---------------------------------------------------------------------------

# Ordered list of (type_name, compiled_regex) pairs. First match wins.
TYPE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("annual_report",       re.compile(r"\bannual\s+report\b", re.I)),
    ("half_yearly",         re.compile(r"\bhalf[\s-]year", re.I)),
    ("quarterly",           re.compile(r"\bquarterly\b", re.I)),
    ("financial_results",   re.compile(r"\bfinancial\s+results?\b", re.I)),
    ("dividend",            re.compile(r"\bdividend\b", re.I)),
    ("placement",           re.compile(r"\bplacement\b", re.I)),
    ("prospectus",          re.compile(r"\bprospectus\b", re.I)),
    ("takeover",            re.compile(r"\btakeover\b", re.I)),
    ("buyback",             re.compile(r"\bbuyback\b", re.I)),
    ("trading_halt",        re.compile(r"\btrading\s+halt\b", re.I)),
    ("cessation",           re.compile(r"\bcessation\b", re.I)),
    ("substantial_holder",  re.compile(r"\bsubstantial\s+hold", re.I)),
    ("director_interest",   re.compile(r"\bdirector.{0,5}interest\b", re.I)),
    ("agm",                 re.compile(r"\b(AGM|annual\s+general\s+meeting)\b", re.I)),
    ("nta",                 re.compile(r"\bnet\s+tangible\s+asset\b", re.I)),
    ("quotation",           re.compile(r"\bquotation\s+of\s+securities\b", re.I)),
    ("corporate_action",    re.compile(r"\bcorporate\s+action\b", re.I)),
]


def classify_filing_type(headline: str) -> str:
    """Return the first matching filing type label or 'other'.

    Iterates TYPE_PATTERNS in order; first match wins.

    Args:
        headline: The filing headline text.

    Returns:
        A lowercase type string such as 'annual_report' or 'other'.
    """
    for type_name, pattern in TYPE_PATTERNS:
        if pattern.search(headline):
            return type_name
    return "other"


# Backwards-compatible alias
classify_announcement_type = classify_filing_type


# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------


def _normalize_date(raw: str) -> str:
    """Convert DD/MM/YYYY to YYYY-MM-DD. Returns raw if parse fails.

    Args:
        raw: Date string potentially in DD/MM/YYYY format.

    Returns:
        ISO 8601 date string (YYYY-MM-DD) on success, or the original raw
        string when the format cannot be parsed.
    """
    raw = raw.strip()
    try:
        dt = datetime.strptime(raw, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return raw


# ---------------------------------------------------------------------------
# HTML utility helpers
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
    """Return True if the pricesens cell contains the price-sensitive icon."""
    if td is None:
        return False
    if td.find("img", class_="pricesens"):
        return True
    return td.find("img", alt=re.compile(r"asterix|price", re.I)) is not None


def _parse_headline_td(td: Any) -> dict[str, Any]:
    """Parse the headline <td>, returning headline, document_url, ids_id, file_size, num_pages."""
    result: dict[str, Any] = {
        "headline": None,
        "document_url": None,
        "ids_id": None,
        "file_size": None,
        "num_pages": None,
    }
    anchor = td.find("a", href=re.compile(r"displayAnnouncement\.do"))
    if anchor is None:
        return result

    href = anchor.get("href", "")
    result["document_url"] = (ASX_BASE + href) if href.startswith("/") else href
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


# ---------------------------------------------------------------------------
# Page parsers
# ---------------------------------------------------------------------------


def parse_announcements_do(html: str) -> tuple[list[Filing], list[str]]:
    """Parse /asx/v2/statistics/announcements.do HTML.

    Column order: Date/Time | Price sens. | Headline
    ASX code comes from the page <h2> header.

    Args:
        html: Raw HTML response body.

    Returns:
        A tuple of (filings, error_strings).
    """
    soup = BeautifulSoup(html, BS_PARSER)
    filings: list[Filing] = []
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
        return filings, errors

    table = ann_data.find("table")
    if table is None:
        errors.append("announcements.do: no <table> inside <announcement_data>")
        return filings, errors

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

            filing_type = classify_filing_type(hl["headline"])

            filings.append(
                Filing(
                    filing_id=hl["ids_id"],
                    source="asx",
                    country="AU",
                    ticker=asx_code or "",
                    filing_date=_normalize_date(date_raw),
                    filing_time=time_val,
                    headline=hl["headline"],
                    filing_type=filing_type,
                    document_url=hl["document_url"],
                    file_size=hl["file_size"],
                    num_pages=hl["num_pages"],
                    price_sensitive=price_sens,
                    # ISIN and LEI are not present in the ASX announcement HTML
                    # response; they require a separate company-level lookup.
                    isin=None,
                    lei=None,
                    language="en",
                )
            )
        except Exception as exc:
            errors.append(f"announcements.do row {i}: {exc}")

    return filings, errors


def parse_prev_bus_day_anns(html: str) -> tuple[list[Filing], list[str]]:
    """Parse /asx/v2/statistics/prevBusDayAnns.do HTML.

    Column order: ASX Code | Date/Time | Price sens. | Headline

    Args:
        html: Raw HTML response body.

    Returns:
        A tuple of (filings, error_strings).
    """
    soup = BeautifulSoup(html, BS_PARSER)
    filings: list[Filing] = []
    errors: list[str] = []

    ann_data = soup.find("announcement_data")
    if ann_data is None:
        errors.append("prevBusDayAnns.do: <announcement_data> tag not found")
        return filings, errors

    table = ann_data.find("table")
    if table is None:
        errors.append("prevBusDayAnns.do: no <table> inside <announcement_data>")
        return filings, errors

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

            filing_type = classify_filing_type(hl["headline"])

            filings.append(
                Filing(
                    filing_id=hl["ids_id"],
                    source="asx",
                    country="AU",
                    ticker=asx_code,
                    filing_date=_normalize_date(date_raw),
                    filing_time=time_val,
                    headline=hl["headline"],
                    filing_type=filing_type,
                    document_url=hl["document_url"],
                    file_size=hl["file_size"],
                    num_pages=hl["num_pages"],
                    price_sensitive=price_sens,
                    # ISIN and LEI are not present in the ASX announcement HTML
                    # response; they require a separate company-level lookup.
                    isin=None,
                    lei=None,
                    language="en",
                )
            )
        except Exception as exc:
            errors.append(f"prevBusDayAnns.do row {i}: {exc}")

    return filings, errors
