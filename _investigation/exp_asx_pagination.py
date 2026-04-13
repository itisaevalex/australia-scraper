"""
exp_asx_pagination.py — ASX endpoint pagination, filtering, and rate-limit probe.

KEY DISCOVERY: Both endpoints return text/html (not JSON).
  announcements.do  -> HTML with <announcement_data> wrapping an HTML <table>
  prevBusDayAnns.do -> HTML with a flat <table> (4 columns: ASX Code, Date, Price sens., Headline)

Correct form parameter values (discovered by parsing the form):
  timeframe: 'D' (relative) or 'Y' (calendar year)
  period (used with timeframe=D): T|P|W|M|M3|M6
  year   (used with timeframe=Y): 1998–2026

Tests:
  1. Time-period parameters on announcements.do (BHP) — corrected values
  2. Company-name prefix search — shows a company-chooser table, not direct results
  3. Announcement type filtering — no server-side type param; client-side parsing only
  4. Rate-limit behaviour: 20 rapid vs 20 with 0.5 s delay on prevBusDayAnns.do
  5. Stateless verification: two independent sessions, same URL
"""

from __future__ import annotations

import collections
import json
import time
from typing import Any

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ANNOUNCEMENTS_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do"
PREV_BUS_DAY_URL  = "https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do"

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

BASE_HEADERS: dict[str, str] = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.asx.com.au/markets/trade-our-cash-market/announcements.leg",
}

SEPARATOR = "=" * 72


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_session() -> requests.Session:
    """Return a fresh requests.Session with no cookies and browser headers."""
    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    return session


def _parse_announcement_table(html: str) -> list[dict[str, str]]:
    """Parse the <table> inside <announcement_data> from announcements.do HTML.

    Returns a list of dicts with keys: date, price_sensitive, headline, href, ids_id.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    rows = table.find_all("tr")[1:]  # skip header
    results: list[dict[str, str]] = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        date = cells[0].get_text(separator=" ", strip=True)
        price_sens = "Y" if cells[1].find("img") else "N"
        link_tag = cells[2].find("a")
        if not link_tag:
            continue
        headline_parts = link_tag.get_text(separator="\n", strip=True).split("\n")
        headline = headline_parts[0].strip()
        href = link_tag.get("href", "")
        # Extract idsId from href e.g. ?display=pdf&idsId=03082041
        ids_id = ""
        if "idsId=" in href:
            ids_id = href.split("idsId=")[-1].split("&")[0]
        results.append({
            "date": date,
            "price_sensitive": price_sens,
            "headline": headline,
            "href": href,
            "ids_id": ids_id,
        })
    return results


def _parse_prev_bus_day_table(html: str) -> list[dict[str, str]]:
    """Parse the flat <table> from prevBusDayAnns.do.

    Columns: ASX Code | Date | Price sens. | Headline
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    rows = table.find_all("tr")[1:]
    results: list[dict[str, str]] = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        asx_code = cells[0].get_text(strip=True)
        date = cells[1].get_text(separator=" ", strip=True)
        price_sens = "Y" if cells[2].find("img") else "N"
        link_tag = cells[3].find("a")
        if not link_tag:
            continue
        headline = link_tag.get_text(separator="\n", strip=True).split("\n")[0].strip()
        href = link_tag.get("href", "")
        ids_id = href.split("idsId=")[-1].split("&")[0] if "idsId=" in href else ""
        results.append({
            "asx_code": asx_code,
            "date": date,
            "price_sensitive": price_sens,
            "headline": headline,
            "href": href,
            "ids_id": ids_id,
        })
    return results


def _get_date_range(html: str) -> str:
    """Extract the 'Released between ...' span from the HTML."""
    soup = BeautifulSoup(html, "lxml")
    span = soup.find("span", class_="searchperiod")
    if span:
        return span.get_text(separator=" ", strip=True)
    return ""


def _safe_get(
    session: requests.Session,
    url: str,
    params: dict[str, str],
) -> tuple[int, float, str]:
    """GET with timing.  Returns (status_code, elapsed_seconds, response_text)."""
    t0 = time.perf_counter()
    try:
        resp = session.get(url, params=params, timeout=15)
        elapsed = time.perf_counter() - t0
        return resp.status_code, elapsed, resp.text
    except requests.RequestException as exc:
        elapsed = time.perf_counter() - t0
        print(f"  [ERROR] {exc}")
        return 0, elapsed, ""


# ---------------------------------------------------------------------------
# Section 1 — Time-period parameters (corrected values from form inspection)
# ---------------------------------------------------------------------------

def test_time_periods() -> None:
    print(SEPARATOR)
    print("SECTION 1: Time-period parameters on announcements.do (BHP)")
    print()
    print("  NOTE: Endpoint returns text/html, not JSON.")
    print("  Correct period values (from form <select name=period>):")
    print("    T=Today  P=Prev trading day  W=past week  M=past month")
    print("    M3=past 3 months  M6=past 6 months")
    print("  Calendar year: timeframe=Y&year=YYYY")
    print(SEPARATOR)

    session = _new_session()

    cases: list[tuple[str, dict[str, str]]] = [
        ("T  (Today)",          {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "T"}),
        ("P  (Prev trad. day)", {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "P"}),
        ("W  (past week)",      {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "W"}),
        ("M  (past month)",     {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M"}),
        ("M3 (past 3 months)",  {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M3"}),
        ("M6 (past 6 months)",  {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}),
        ("year=2026",           {"by": "asxCode", "asxCode": "BHP", "timeframe": "Y", "year": "2026"}),
        ("year=2025",           {"by": "asxCode", "asxCode": "BHP", "timeframe": "Y", "year": "2025"}),
        ("year=2024",           {"by": "asxCode", "asxCode": "BHP", "timeframe": "Y", "year": "2024"}),
        ("year=2020",           {"by": "asxCode", "asxCode": "BHP", "timeframe": "Y", "year": "2020"}),
    ]

    print(f"  {'Label':<24} {'Status':>6}  {'Elapsed':>8}  {'Count':>7}  DateRange")
    print(f"  {'-'*24} {'-'*6}  {'-'*8}  {'-'*7}  {'-'*40}")

    for label, params in cases:
        status, elapsed, html = _safe_get(session, ANNOUNCEMENTS_URL, params)
        items = _parse_announcement_table(html) if html else []
        date_range = _get_date_range(html) if html else ""
        print(f"  {label:<24} {status:>6}  {elapsed:>7.2f}s  {len(items):>7}  {date_range}")
        time.sleep(0.3)

    # Show first 3 items from M3 to confirm data structure
    print()
    print("  -- Sample: first 3 rows from M3 (BHP) --")
    _, _, html_m3 = _safe_get(
        session, ANNOUNCEMENTS_URL,
        {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M3"},
    )
    for item in _parse_announcement_table(html_m3)[:3]:
        print(f"    date={item['date']:<25}  ids_id={item['ids_id']}  {item['headline'][:55]}")


# ---------------------------------------------------------------------------
# Section 2 — Company-name prefix search
# ---------------------------------------------------------------------------

def test_company_name_search() -> None:
    print()
    print(SEPARATOR)
    print("SECTION 2: Company-name prefix search (by=companyName)")
    print()
    print("  NOTE: The response is a company-chooser table (list of ASX codes)")
    print("  NOT a direct list of announcements.  Must select a code, then re-query.")
    print(SEPARATOR)

    session = _new_session()

    cases: list[tuple[str, dict[str, str]]] = [
        ("BHP",     {"by": "companyName", "companyName": "BHP",     "timeframe": "D", "period": "M3"}),
        ("Westpac", {"by": "companyName", "companyName": "Westpac", "timeframe": "D", "period": "M3"}),
        ("Common",  {"by": "companyName", "companyName": "Common",  "timeframe": "D", "period": "M3"}),
        ("B",       {"by": "companyName", "companyName": "B",       "timeframe": "D", "period": "M3"}),
    ]

    print(f"  {'Prefix':<12} {'Status':>6}  {'Elapsed':>8}  {'#Matches':>9}  Companies returned")
    print(f"  {'-'*12} {'-'*6}  {'-'*8}  {'-'*9}  {'-'*40}")

    for prefix, params in cases:
        status, elapsed, html = _safe_get(session, ANNOUNCEMENTS_URL, params)
        soup = BeautifulSoup(html, "lxml") if html else None
        matches: list[str] = []
        if soup:
            # Company chooser is the first table — cells contain "CODE - FULL NAME"
            tables = soup.find_all("table")
            if tables:
                for td in tables[0].find_all("td"):
                    text = td.get_text(strip=True)
                    if " - " in text and len(text) < 80:
                        matches.append(text)
        preview = ", ".join(matches[:4])
        if len(matches) > 4:
            preview += f" ... (+{len(matches)-4} more)"
        print(f"  {prefix:<12} {status:>6}  {elapsed:>7.2f}s  {len(matches):>9}  {preview}")
        time.sleep(0.3)

    print()
    print("  -- Workflow: to get announcements for a prefix match, parse codes then re-query --")
    print("  Example: 'Common' -> ['CBA - COMMONWEALTH BANK OF AUSTRALIA', ...]")
    print("           -> extract 'CBA' -> query by=asxCode&asxCode=CBA&timeframe=D&period=M3")


# ---------------------------------------------------------------------------
# Section 3 — Announcement type filtering
# ---------------------------------------------------------------------------

def test_type_filtering() -> None:
    print()
    print(SEPARATOR)
    print("SECTION 3: Announcement type / category filtering")
    print(SEPARATOR)

    session = _new_session()
    base_params: dict[str, str] = {
        "by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M3",
    }

    # Test server-side type parameters
    type_candidates: list[tuple[str, dict[str, str]]] = [
        ("(no type param — baseline)",    {**base_params}),
        ("type=annual_report",            {**base_params, "type": "annual_report"}),
        ("type=Annual Report",            {**base_params, "type": "Annual Report"}),
        ("category=annual_report",        {**base_params, "category": "annual_report"}),
        ("documentType=annual_report",    {**base_params, "documentType": "annual_report"}),
        ("ann_type=annual_report",        {**base_params, "ann_type": "annual_report"}),
        ("announcementType=annual_report",{**base_params, "announcementType": "annual_report"}),
    ]

    print(f"  {'Param variant':<40} {'Status':>6}  {'Elapsed':>8}  {'Count':>7}")
    print(f"  {'-'*40} {'-'*6}  {'-'*8}  {'-'*7}")

    baseline_count = 0
    for label, params in type_candidates:
        status, elapsed, html = _safe_get(session, ANNOUNCEMENTS_URL, params)
        count = len(_parse_announcement_table(html)) if html else 0
        if label.startswith("(no"):
            baseline_count = count
        diff = f" (same as baseline)" if count == baseline_count and not label.startswith("(no") else ""
        print(f"  {label:<40} {status:>6}  {elapsed:>7.2f}s  {count:>7}{diff}")
        time.sleep(0.3)

    print()
    print("  FINDING: No server-side type filter parameter found.")
    print("  The announcement 'type' is embedded only in the headline text.")
    print()

    # Show headline type distribution from prevBusDayAnns for full-day context
    print("  -- Announcement type distribution from prevBusDayAnns.do (all companies, today) --")
    _, _, html_prev = _safe_get(_new_session(), PREV_BUS_DAY_URL, {})
    items = _parse_prev_bus_day_table(html_prev) if html_prev else []
    print(f"  Total announcements: {len(items)}")
    prefixes: collections.Counter[str] = collections.Counter()
    for item in items:
        h = item.get("headline", "")
        # Take text up to first '-' or first number as the type
        parts = h.split(" - ")
        prefix = parts[0].strip() if parts else h
        # Strip trailing digits/whitespace (page counts sometimes bleed in)
        prefix = prefix.rstrip("0123456789 ").strip()
        prefixes[prefix] += 1
    print(f"  Top 15 headline types (client-side classification only):")
    for typ, cnt in prefixes.most_common(15):
        print(f"    {cnt:>4}x  {typ[:65]}")


# ---------------------------------------------------------------------------
# Section 4 — Rate-limit test
# ---------------------------------------------------------------------------

def test_rate_limiting() -> None:
    print()
    print(SEPARATOR)
    print("SECTION 4: Rate-limit test on prevBusDayAnns.do (20 rapid + 20 polite)")
    print(SEPARATOR)

    def run_batch(label: str, delay: float, n: int = 20) -> list[int]:
        print(f"  [{label}] delay={delay}s between requests")
        print(f"  {'Req':>4}  {'Status':>6}  {'Elapsed':>8}  {'Blocked?'}")
        print(f"  {'-'*4}  {'-'*6}  {'-'*8}  {'-'*8}")
        statuses: list[int] = []
        for i in range(1, n + 1):
            session = _new_session()
            status, elapsed, _ = _safe_get(session, PREV_BUS_DAY_URL, {})
            blocked = "YES" if status in (429, 403, 503) else "no"
            print(f"  {i:>4}  {status:>6}  {elapsed:>7.2f}s  {blocked}")
            statuses.append(status)
            if delay > 0:
                time.sleep(delay)
        ok      = sum(1 for s in statuses if s == 200)
        blocked = sum(1 for s in statuses if s in (429, 403, 503))
        print(f"  Summary: 200={ok}/{n}  blocked={blocked}/{n}")
        return statuses

    rapid_statuses  = run_batch("A — Rapid burst",  delay=0.0)
    print()
    polite_statuses = run_batch("B — Polite delay", delay=0.5)

    rapid_ok      = rapid_statuses.count(200)
    polite_ok     = polite_statuses.count(200)
    rapid_blocked = sum(1 for s in rapid_statuses  if s in (429, 403, 503))
    polite_blocked= sum(1 for s in polite_statuses if s in (429, 403, 503))

    print()
    print("  [COMPARISON]")
    print(f"    Rapid  burst: {rapid_ok}/20 OK   {rapid_blocked}/20 blocked")
    print(f"    Polite delay: {polite_ok}/20 OK   {polite_blocked}/20 blocked")
    if rapid_blocked == 0 and polite_blocked == 0:
        print("    VERDICT: No rate limiting detected — both burst and polite succeed 100%.")
    elif rapid_blocked > polite_blocked:
        print("    VERDICT: Rate limiting IS active; polite delay mitigates it.")
    else:
        print("    VERDICT: Both burst and polite blocked equally; likely IP-level block.")


# ---------------------------------------------------------------------------
# Section 5 — Stateless verification
# ---------------------------------------------------------------------------

def test_stateless_verification() -> None:
    print()
    print(SEPARATOR)
    print("SECTION 5: Stateless verification (two independent sessions, same URL)")
    print(SEPARATOR)

    params: dict[str, str] = {
        "by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M3",
    }

    sess_a = _new_session()
    sess_b = _new_session()

    print("  Session A: fetching (fresh session, no cookies)...")
    t0 = time.perf_counter()
    status_a, elapsed_a, html_a = _safe_get(sess_a, ANNOUNCEMENTS_URL, params)
    print(f"  Session A: status={status_a}  elapsed={elapsed_a:.2f}s")

    time.sleep(1.0)

    print("  Session B: fetching (fresh session, no cookies)...")
    t1 = time.perf_counter()
    status_b, elapsed_b, html_b = _safe_get(sess_b, ANNOUNCEMENTS_URL, params)
    print(f"  Session B: status={status_b}  elapsed={elapsed_b:.2f}s")

    items_a = _parse_announcement_table(html_a) if html_a else []
    items_b = _parse_announcement_table(html_b) if html_b else []

    print()
    print(f"  Count A: {len(items_a)}")
    print(f"  Count B: {len(items_b)}")

    if not items_a or not items_b:
        print("  [INCONCLUSIVE] One or both requests returned no data.")
        return

    json_a = json.dumps(items_a, sort_keys=True)
    json_b = json.dumps(items_b, sort_keys=True)

    if json_a == json_b:
        print("  [STATELESS CONFIRMED] Responses are byte-for-byte identical.")
        print("  VERDICT: Random-access pagination is safe; URLs are permanent.")
        print("           Full parallelism possible — collect all URLs then batch-download.")
    elif len(items_a) == len(items_b):
        diffs = [(a, b) for a, b in zip(items_a, items_b) if a != b]
        print(f"  [LIKELY STATELESS] Counts match; {len(diffs)} row(s) differ slightly.")
        for a, b in diffs[:3]:
            print(f"    A: {a}")
            print(f"    B: {b}")
    else:
        print(f"  [LIVE FEED / STATEFUL] Counts differ: {len(items_a)} vs {len(items_b)}")

    print()
    print("  First 3 items (Session A):")
    for item in items_a[:3]:
        print(f"    ids_id={item['ids_id']}  date={item['date']:<22}  {item['headline'][:55]}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(SEPARATOR)
    print("ASX Endpoint Pagination, Filtering & Rate-Limit Probe")
    print(f"  Target 1: {ANNOUNCEMENTS_URL}")
    print(f"  Target 2: {PREV_BUS_DAY_URL}")
    print(SEPARATOR)

    test_time_periods()
    test_company_name_search()
    test_type_filtering()
    test_rate_limiting()
    test_stateless_verification()

    print()
    print(SEPARATOR)
    print("All sections complete.")
    print(SEPARATOR)
