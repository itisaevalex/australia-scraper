"""
exp_asx_parse_announcements.py

Reverse-engineering experiment: Parse ASX announcement HTML from two endpoints.

Endpoints tested:
  1. announcements.do  — company-specific search (BHP, 6-month window)
  2. prevBusDayAnns.do — all companies from previous business day

HTML structure discovered via manual inspection:
  - Custom <announcement_data> wrapper tag contains the <table>
  - Each announcement is a <tr> with class "" or "altrow"
  - prevBusDayAnns: columns = [ASX code, Date/Time, Price sens., Headline]
  - announcements.do:  columns = [Date/Time, Price sens., Headline]
    (no ASX code column; company is identified from page header)
  - Date: first text node in the date <td>
  - Time: <span class="dates-time"> inside the date <td>
  - Price sensitivity: <td class="pricesens"> containing
    <img class="pricesens"> when sensitive; plain " " text when not
  - Headline link: <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=...">
    - First text node of the <a> is the headline text
  - Pages: <span class="page">N pages</span> inside the headline <a>
  - File size: <span class="filesize"> inside the headline <a>
"""

from __future__ import annotations

import re
import sys
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.asx.com.au"

ENDPOINTS: dict[str, str] = {
    "announcements_bhp": (
        "https://www.asx.com.au/asx/v2/statistics/announcements.do"
        "?by=asxCode&asxCode=BHP&timeframe=D&period=M6"
    ),
    "prev_bus_day": (
        "https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do"
    ),
}

HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ---------------------------------------------------------------------------
# BeautifulSoup import with parser fallback
# ---------------------------------------------------------------------------

try:
    import lxml  # noqa: F401 — presence check only
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"

from bs4 import BeautifulSoup  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(text: str | None) -> str:
    """Strip whitespace and collapse internal spaces."""
    if text is None:
        return ""
    return " ".join(text.split())


def _extract_ids_id(href: str) -> str | None:
    """Extract idsId query-parameter value from a displayAnnouncement URL."""
    parsed = urlparse(href)
    params = parse_qs(parsed.query)
    ids = params.get("idsId")
    return ids[0] if ids else None


def _parse_page_count(span_text: str) -> int | None:
    """Parse page count from strings like '4\\n\\t\\t\\t\\t\\t\\t\\t\\tpages'."""
    match = re.search(r"(\d+)", span_text)
    return int(match.group(1)) if match else None


def _parse_row_headline_td(td) -> dict[str, Any]:
    """
    Parse the headline <td> cell that is shared by both endpoints.

    Returns a partial dict with keys:
        headline, pdf_url, ids_id, file_size, num_pages
    """
    result: dict[str, Any] = {
        "headline": None,
        "pdf_url": None,
        "ids_id": None,
        "file_size": None,
        "num_pages": None,
    }

    anchor = td.find("a", href=re.compile(r"displayAnnouncement\.do"))
    if anchor is None:
        return result

    href = anchor.get("href", "")
    result["pdf_url"] = BASE_URL + href if href.startswith("/") else href
    result["ids_id"] = _extract_ids_id(href)

    # Headline: first NavigableString inside the anchor (before any child tags)
    for node in anchor.children:
        text = getattr(node, "string", None) or (
            str(node) if hasattr(node, "strip") else None
        )
        if text and _clean(text):
            # Discard empty strings; stop at first real text
            cleaned = _clean(str(node))
            if cleaned:
                result["headline"] = cleaned
                break

    # Pages: <span class="page">
    page_span = anchor.find("span", class_="page")
    if page_span:
        result["num_pages"] = _parse_page_count(page_span.get_text())

    # File size: <span class="filesize">
    size_span = anchor.find("span", class_="filesize")
    if size_span:
        result["file_size"] = _clean(size_span.get_text())

    return result


def _is_price_sensitive(td) -> bool:
    """
    Return True if the <td class="pricesens"> contains the sensitivity icon.
    The icon is <img class="pricesens"> or any img with alt="asterix".
    An empty/whitespace-only cell means NOT price sensitive.
    """
    if td is None:
        return False
    img = td.find("img", class_="pricesens")
    if img:
        return True
    # Fallback: any img with alt containing "asterix" or "price"
    img_fallback = td.find("img", alt=re.compile(r"asterix|price", re.I))
    return img_fallback is not None


# ---------------------------------------------------------------------------
# Endpoint-specific parsers
# ---------------------------------------------------------------------------

def parse_announcements_do(html: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Parse /asx/v2/statistics/announcements.do response.

    Column order: Date/Time | Price sens. | Headline
    The ASX code is NOT in the table rows; it comes from the page <h2>.

    Returns (announcements, errors).
    """
    soup = BeautifulSoup(html, BS_PARSER)
    announcements: list[dict[str, Any]] = []
    errors: list[str] = []

    # Extract ASX code from the page header: "BHP GROUP LIMITED (BHP)"
    asx_code: str | None = None
    h2_tags = soup.find_all("h2")
    for h2 in h2_tags:
        match = re.search(r"\(([A-Z0-9]{2,6})\)", h2.get_text())
        if match:
            asx_code = match.group(1)
            break

    ann_data = soup.find("announcement_data")
    if ann_data is None:
        errors.append("announcements.do: <announcement_data> tag not found")
        return announcements, errors

    table = ann_data.find("table")
    if table is None:
        errors.append("announcements.do: <table> inside <announcement_data> not found")
        return announcements, errors

    rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")[1:]

    for i, row in enumerate(rows):
        tds = row.find_all("td")
        if len(tds) < 3:
            errors.append(
                f"announcements.do row {i}: expected ≥3 tds, got {len(tds)} — skipping"
            )
            continue

        try:
            # td[0]: Date + time
            date_td = tds[0]
            date_raw = _clean(date_td.find(string=True, recursive=False))
            time_span = date_td.find("span", class_="dates-time")
            time_val = _clean(time_span.get_text()) if time_span else None

            # td[1]: Price sensitivity
            price_sens = _is_price_sensitive(tds[1])

            # td[2]: Headline
            headline_data = _parse_row_headline_td(tds[2])

            record: dict[str, Any] = {
                "asx_code": asx_code,
                "date": date_raw,
                "time": time_val,
                "price_sensitive": price_sens,
                **headline_data,
            }
            announcements.append(record)

        except Exception as exc:
            errors.append(f"announcements.do row {i}: parse error — {exc}")

    return announcements, errors


def parse_prev_bus_day_anns(html: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Parse /asx/v2/statistics/prevBusDayAnns.do response.

    Column order: ASX Code | Date/Time | Price sens. | Headline

    Returns (announcements, errors).
    """
    soup = BeautifulSoup(html, BS_PARSER)
    announcements: list[dict[str, Any]] = []
    errors: list[str] = []

    ann_data = soup.find("announcement_data")
    if ann_data is None:
        errors.append("prevBusDayAnns.do: <announcement_data> tag not found")
        return announcements, errors

    table = ann_data.find("table")
    if table is None:
        errors.append("prevBusDayAnns.do: <table> inside <announcement_data> not found")
        return announcements, errors

    # Header row has <th> elements; data rows do not
    all_rows = table.find_all("tr")
    data_rows = [r for r in all_rows if r.find("td")]

    for i, row in enumerate(data_rows):
        tds = row.find_all("td")
        if len(tds) < 4:
            errors.append(
                f"prevBusDayAnns.do row {i}: expected ≥4 tds, got {len(tds)} — skipping"
            )
            continue

        try:
            # td[0]: ASX code (plain text)
            asx_code = _clean(tds[0].get_text())

            # td[1]: Date + time
            date_td = tds[1]
            date_raw = _clean(date_td.find(string=True, recursive=False))
            time_span = date_td.find("span", class_="dates-time")
            time_val = _clean(time_span.get_text()) if time_span else None

            # td[2]: Price sensitivity
            price_sens = _is_price_sensitive(tds[2])

            # td[3]: Headline
            headline_data = _parse_row_headline_td(tds[3])

            record: dict[str, Any] = {
                "asx_code": asx_code,
                "date": date_raw,
                "time": time_val,
                "price_sensitive": price_sens,
                **headline_data,
            }
            announcements.append(record)

        except Exception as exc:
            errors.append(f"prevBusDayAnns.do row {i}: parse error — {exc}")

    return announcements, errors


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch(url: str) -> str:
    """Fetch URL with browser headers; raise on non-200."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

def print_report(
    label: str,
    announcements: list[dict[str, Any]],
    errors: list[str],
) -> None:
    """Print a structured summary of parsed announcements."""
    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  ENDPOINT: {label}")
    print(f"  Parser:   BeautifulSoup({BS_PARSER!r})")
    print(sep)

    print(f"\nTotal announcements found: {len(announcements)}")

    if errors:
        print(f"\n--- Parse errors ({len(errors)}) ---")
        for err in errors:
            print(f"  [ERROR] {err}")

    if not announcements:
        print("  (no announcements to display)")
        return

    # First 5
    print("\n--- First 5 announcements ---")
    for ann in announcements[:5]:
        _print_ann(ann)

    # Last 5 (avoid duplicates when len <= 10)
    if len(announcements) > 5:
        print("\n--- Last 5 announcements ---")
        for ann in announcements[-5:]:
            _print_ann(ann)

    # Unique ASX codes
    codes = sorted({a["asx_code"] for a in announcements if a["asx_code"]})
    print(f"\nUnique ASX codes ({len(codes)}): {', '.join(codes)}")

    # Field coverage stats
    total = len(announcements)
    missing: dict[str, int] = {}
    for field in ("headline", "pdf_url", "ids_id", "file_size", "num_pages", "time"):
        count = sum(1 for a in announcements if not a.get(field))
        if count:
            missing[field] = count
    if missing:
        print("\n--- Missing field counts ---")
        for field, count in missing.items():
            print(f"  {field}: {count}/{total} rows missing")
    else:
        print("\nAll key fields present in every row.")


def _print_ann(ann: dict[str, Any]) -> None:
    print(
        f"  {{\n"
        f"    asx_code:        {ann.get('asx_code')!r}\n"
        f"    date:            {ann.get('date')!r}\n"
        f"    time:            {ann.get('time')!r}\n"
        f"    price_sensitive: {ann.get('price_sensitive')}\n"
        f"    headline:        {ann.get('headline')!r}\n"
        f"    ids_id:          {ann.get('ids_id')!r}\n"
        f"    pdf_url:         {ann.get('pdf_url')!r}\n"
        f"    file_size:       {ann.get('file_size')!r}\n"
        f"    num_pages:       {ann.get('num_pages')}\n"
        f"  }}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Using BeautifulSoup parser: {BS_PARSER!r}")
    print(f"Python: {sys.version}")

    results: dict[str, tuple[list[dict[str, Any]], list[str]]] = {}

    for name, url in ENDPOINTS.items():
        print(f"\nFetching {name} ...")
        print(f"  URL: {url}")
        try:
            html = fetch(url)
            print(f"  HTTP 200 OK — {len(html):,} bytes received")
        except requests.HTTPError as exc:
            print(f"  [FETCH ERROR] HTTP {exc.response.status_code}: {exc}")
            results[name] = ([], [f"HTTP error: {exc}"])
            continue
        except Exception as exc:
            print(f"  [FETCH ERROR] {exc}")
            results[name] = ([], [f"Fetch error: {exc}"])
            continue

        if name == "announcements_bhp":
            anns, errors = parse_announcements_do(html)
        else:
            anns, errors = parse_prev_bus_day_anns(html)

        results[name] = (anns, errors)

    # Print reports
    for name, (anns, errors) in results.items():
        print_report(name, anns, errors)

    # Cross-endpoint summary
    print("\n" + "=" * 72)
    print("  SUMMARY")
    print("=" * 72)
    for name, (anns, errors) in results.items():
        status = "OK" if not errors else f"{len(errors)} error(s)"
        print(f"  {name:30s}  announcements={len(anns):>5d}  status={status}")


if __name__ == "__main__":
    main()
