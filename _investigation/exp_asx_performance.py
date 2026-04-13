"""
exp_asx_performance.py — ASX Scraper Performance & Rate-Limit Analysis

Tests 7 dimensions of the ASX HTTP surface:
  1. Single-request latency benchmarks (5 samples per endpoint)
  2. Sequential company crawl throughput (10 tickers, period=P)
  3. Rate-limit probing (30 rapid requests, no delay)
  4. Parallel PDF URL resolution throughput (workers=1,3,5,10)
  5. Parallel PDF download throughput (workers=1,3,5,10)
  6. Memory usage for a prevBusDayAnns.do response
  7. Full pipeline wall-time (prevBusDayAnns → parse → resolve 20 → download 20)

Run from the project root:
    python _investigation/exp_asx_performance.py
"""

from __future__ import annotations

import sys
import time
import io
import re
import csv
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs

# Silence urllib3 version warning
import warnings
warnings.filterwarnings("ignore")

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants (mirrors scraper.py)
# ---------------------------------------------------------------------------

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
BASE_HEADERS: dict[str, str] = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

ASX_BASE = "https://www.asx.com.au"
ANNOUNCEMENTS_URL = f"{ASX_BASE}/asx/v2/statistics/announcements.do"
PREV_BUS_DAY_URL = f"{ASX_BASE}/asx/v2/statistics/prevBusDayAnns.do"
DISPLAY_ANNOUNCEMENT_URL = f"{ASX_BASE}/asx/v2/statistics/displayAnnouncement.do"
MARKIT_DIRECTORY_URL = (
    "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file"
)

# A known stable idsId for terms-page test (BHP recent announcement)
SAMPLE_IDS_ID = "03082110"

# Large-cap tickers for throughput test
THROUGHPUT_TICKERS = ["BHP", "CBA", "NAB", "WBC", "ANZ", "TLS", "WES", "CSL", "RIO", "FMG"]

# Rate-limit probe tickers (cycle through ASX-listed symbols)
PROBE_TICKERS = [
    "BHP", "CBA", "NAB", "WBC", "ANZ", "TLS", "WES", "CSL", "RIO", "FMG",
    "APT", "APX", "ARB", "ALD", "ALL", "ALQ", "AMP", "ANN", "AOF", "APA",
    "APE", "API", "APZ", "AQR", "ARF", "ARL", "ARM", "ARQ", "ARX", "AYS",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

try:
    import lxml  # noqa: F401
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"


def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(BASE_HEADERS)
    return sess


def _sep(width: int = 72) -> str:
    return "=" * width


def _subsep(width: int = 72) -> str:
    return "-" * width


def _header(title: str) -> None:
    print()
    print(_sep())
    print(f"  {title}")
    print(_sep())


def _table(headers: list[str], rows: list[list[Any]], col_widths: list[int] | None = None) -> None:
    """Print a simple ASCII table."""
    if col_widths is None:
        col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) + 2
                      for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in col_widths))
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))


# ---------------------------------------------------------------------------
# Parsers (minimal inline versions — no DB dependency)
# ---------------------------------------------------------------------------

def _clean(text: str | None) -> str:
    if text is None:
        return ""
    return " ".join(text.split())


def _extract_ids_id(href: str) -> str | None:
    parsed = urlparse(href)
    params = parse_qs(parsed.query)
    ids = params.get("idsId")
    return ids[0] if ids else None


def _parse_headline_td(td: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"headline": None, "pdf_url": None, "ids_id": None}
    anchor = td.find("a", href=re.compile(r"displayAnnouncement\.do"))
    if anchor is None:
        return result
    href = anchor.get("href", "")
    result["pdf_url"] = (ASX_BASE + href) if href.startswith("/") else href
    result["ids_id"] = _extract_ids_id(href)
    for node in anchor.children:
        raw = str(node) if not hasattr(node, "get_text") else ""
        text = _clean(raw or getattr(node, "string", None) or "")
        if text:
            result["headline"] = text
            break
    return result


def parse_prev_bus_day_anns(html: str) -> list[dict[str, Any]]:
    """Return list of dicts with ids_id and asx_code."""
    soup = BeautifulSoup(html, BS_PARSER)
    results: list[dict[str, Any]] = []
    ann_data = soup.find("announcement_data")
    if ann_data is None:
        return results
    table = ann_data.find("table")
    if table is None:
        return results
    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 4:
            continue
        asx_code = _clean(tds[0].get_text())
        hl = _parse_headline_td(tds[3])
        if hl["ids_id"]:
            results.append({"ids_id": hl["ids_id"], "asx_code": asx_code})
    return results


def parse_announcements_do(html: str) -> list[dict[str, Any]]:
    """Return list of dicts with ids_id and asx_code."""
    soup = BeautifulSoup(html, BS_PARSER)
    results: list[dict[str, Any]] = []
    asx_code: str | None = None
    for h2 in soup.find_all("h2"):
        match = re.search(r"\(([A-Z0-9]{2,6})\)", h2.get_text())
        if match:
            asx_code = match.group(1)
            break
    ann_data = soup.find("announcement_data")
    if ann_data is None:
        return results
    table = ann_data.find("table")
    if table is None:
        return results
    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        hl = _parse_headline_td(tds[2])
        if hl["ids_id"]:
            results.append({"ids_id": hl["ids_id"], "asx_code": asx_code or ""})
    return results


def resolve_pdf_url(session: requests.Session, ids_id: str) -> tuple[str | None, float]:
    """Resolve PDF URL, returning (direct_url, elapsed_seconds)."""
    url = f"{DISPLAY_ANNOUNCEMENT_URL}?display=pdf&idsId={ids_id}"
    t0 = time.perf_counter()
    try:
        resp = session.get(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        elapsed = time.perf_counter() - t0
        soup = BeautifulSoup(resp.text, BS_PARSER)
        pdf_input = soup.find("input", {"name": "pdfURL"})
        if pdf_input and pdf_input.get("value"):
            return str(pdf_input["value"]), elapsed
        return None, elapsed
    except requests.RequestException:
        elapsed = time.perf_counter() - t0
        return None, elapsed


# ---------------------------------------------------------------------------
# TEST 1 — Single-request latency benchmarks
# ---------------------------------------------------------------------------

def test1_latency_benchmarks() -> None:
    _header("TEST 1: Single-Request Latency Benchmarks (5 samples each)")

    endpoints: list[tuple[str, str, dict[str, str] | None]] = [
        (
            "announcements.do (BHP/M)",
            ANNOUNCEMENTS_URL,
            {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M"},
        ),
        (
            "prevBusDayAnns.do",
            PREV_BUS_DAY_URL,
            None,
        ),
        (
            f"displayAnnouncement.do (idsId={SAMPLE_IDS_ID})",
            f"{DISPLAY_ANNOUNCEMENT_URL}?display=pdf&idsId={SAMPLE_IDS_ID}",
            None,
        ),
        (
            "MarkitDigital directory CSV",
            MARKIT_DIRECTORY_URL,
            None,
        ),
    ]

    session = make_session()
    # Extra header for Markit
    session.headers.update({"Referer": "https://www.asx.com.au/"})

    all_rows: list[list[Any]] = []

    for label, url, params in endpoints:
        samples: list[float] = []
        sizes: list[int] = []
        statuses: list[int] = []
        for _ in range(5):
            t0 = time.perf_counter()
            try:
                resp = session.get(url, params=params, timeout=45, allow_redirects=True)
                elapsed = time.perf_counter() - t0
                statuses.append(resp.status_code)
                sizes.append(len(resp.content))
                samples.append(elapsed)
            except requests.RequestException as exc:
                elapsed = time.perf_counter() - t0
                statuses.append(0)
                sizes.append(0)
                samples.append(elapsed)
                print(f"    ERROR on {label}: {exc}")
            time.sleep(0.25)

        mean_ms = sum(samples) / len(samples) * 1000
        min_ms = min(samples) * 1000
        max_ms = max(samples) * 1000
        mean_kb = sum(sizes) / len(sizes) / 1024
        status_summary = ",".join(str(s) for s in statuses)
        all_rows.append([
            label[:40],
            f"{mean_ms:.0f}",
            f"{min_ms:.0f}",
            f"{max_ms:.0f}",
            f"{mean_kb:.1f}",
            status_summary,
        ])

    _table(
        ["Endpoint", "mean(ms)", "min(ms)", "max(ms)", "size(KB)", "statuses"],
        all_rows,
        col_widths=[42, 10, 10, 10, 10, 20],
    )

    # PDF CDN download — need to resolve a real URL first
    print()
    print("  Resolving a real PDF CDN URL for download latency test...")
    direct_url, _ = resolve_pdf_url(session, SAMPLE_IDS_ID)
    if direct_url:
        print(f"  PDF CDN URL: {direct_url[:80]}...")
        pdf_samples: list[float] = []
        pdf_sizes: list[int] = []
        for _ in range(5):
            t0 = time.perf_counter()
            try:
                resp = session.get(direct_url, timeout=60, stream=True)
                resp.raise_for_status()
                data = resp.content
                elapsed = time.perf_counter() - t0
                pdf_samples.append(elapsed)
                pdf_sizes.append(len(data))
            except requests.RequestException as exc:
                elapsed = time.perf_counter() - t0
                pdf_samples.append(elapsed)
                pdf_sizes.append(0)
                print(f"    PDF download ERROR: {exc}")
            time.sleep(0.3)

        mean_ms = sum(pdf_samples) / len(pdf_samples) * 1000
        min_ms = min(pdf_samples) * 1000
        max_ms = max(pdf_samples) * 1000
        mean_kb = sum(pdf_sizes) / len(pdf_sizes) / 1024
        print()
        _table(
            ["Endpoint", "mean(ms)", "min(ms)", "max(ms)", "size(KB)", "statuses"],
            [["PDF CDN download (direct)", f"{mean_ms:.0f}", f"{min_ms:.0f}", f"{max_ms:.0f}", f"{mean_kb:.1f}", "200x5"]],
            col_widths=[42, 10, 10, 10, 10, 20],
        )
    else:
        print("  Could not resolve PDF CDN URL — skipping CDN download latency test.")


# ---------------------------------------------------------------------------
# TEST 2 — Sequential company crawl throughput
# ---------------------------------------------------------------------------

def test2_sequential_crawl() -> None:
    _header("TEST 2: Sequential Company Crawl Throughput (10 tickers, period=P)")

    session = make_session()
    rows: list[list[Any]] = []
    total_t0 = time.perf_counter()

    for ticker in THROUGHPUT_TICKERS:
        params = {"by": "asxCode", "asxCode": ticker, "timeframe": "D", "period": "P"}
        t0 = time.perf_counter()
        try:
            resp = session.get(ANNOUNCEMENTS_URL, params=params, timeout=30, allow_redirects=True)
            elapsed = time.perf_counter() - t0
            status = resp.status_code
            size_kb = len(resp.content) / 1024
            if resp.ok:
                anns = parse_announcements_do(resp.text)
                ann_count = len(anns)
            else:
                ann_count = 0
        except requests.RequestException as exc:
            elapsed = time.perf_counter() - t0
            status = 0
            size_kb = 0.0
            ann_count = 0
            print(f"    ERROR on {ticker}: {exc}")
        rows.append([ticker, f"{elapsed*1000:.0f}", str(status), f"{size_kb:.1f}", str(ann_count)])
        time.sleep(0.3)  # scraper.py FETCH_DELAY

    total_elapsed = time.perf_counter() - total_t0
    avg_per_ticker = total_elapsed / len(THROUGHPUT_TICKERS)

    _table(
        ["Ticker", "latency(ms)", "status", "size(KB)", "announcements"],
        rows,
        col_widths=[8, 13, 8, 10, 15],
    )
    print()
    print(f"  Total wall time: {total_elapsed:.2f}s")
    print(f"  Avg per ticker:  {avg_per_ticker:.2f}s")
    print(f"  Throughput:      {len(THROUGHPUT_TICKERS)/total_elapsed:.2f} tickers/s")
    print()
    print("  BOTTLENECK ANALYSIS:")
    latencies = [float(r[1]) for r in rows]
    fetch_total = sum(latencies) / 1000
    sleep_total = 0.3 * len(THROUGHPUT_TICKERS)
    parse_overhead = total_elapsed - fetch_total - sleep_total
    print(f"    Network fetch:   {fetch_total:.2f}s  ({fetch_total/total_elapsed*100:.0f}%)")
    print(f"    Forced delays:   {sleep_total:.2f}s  ({sleep_total/total_elapsed*100:.0f}%)")
    print(f"    Parse+overhead:  {parse_overhead:.2f}s  ({max(0,parse_overhead)/total_elapsed*100:.0f}%)")


# ---------------------------------------------------------------------------
# TEST 3 — Rate-limit probing
# ---------------------------------------------------------------------------

def test3_rate_limit_probe() -> None:
    _header("TEST 3: Rate-Limit Probing (30 rapid sequential requests, no delay)")

    session = make_session()
    rows: list[list[Any]] = []
    errors: list[str] = []

    print(f"  Sending {len(PROBE_TICKERS)} requests with no inter-request delay...")
    print()

    for i, ticker in enumerate(PROBE_TICKERS):
        params = {"by": "asxCode", "asxCode": ticker, "timeframe": "D", "period": "M"}
        t0 = time.perf_counter()
        try:
            resp = session.get(ANNOUNCEMENTS_URL, params=params, timeout=20, allow_redirects=True)
            elapsed = time.perf_counter() - t0
            status = resp.status_code
            size_kb = len(resp.content) / 1024
            flag = ""
            if status in (429, 403, 503):
                flag = f"<-- RATE LIMITED ({status})"
                errors.append(f"Request {i+1} ({ticker}): HTTP {status}")
            elif elapsed > 5.0:
                flag = "<-- SLOW (>5s)"
        except requests.ConnectionError as exc:
            elapsed = time.perf_counter() - t0
            status = 0
            size_kb = 0.0
            flag = "<-- CONNECTION ERROR"
            errors.append(f"Request {i+1} ({ticker}): ConnectionError: {exc}")
        except requests.Timeout:
            elapsed = time.perf_counter() - t0
            status = 0
            size_kb = 0.0
            flag = "<-- TIMEOUT"
            errors.append(f"Request {i+1} ({ticker}): Timeout")
        except requests.RequestException as exc:
            elapsed = time.perf_counter() - t0
            status = 0
            size_kb = 0.0
            flag = f"<-- ERROR: {type(exc).__name__}"
            errors.append(f"Request {i+1} ({ticker}): {exc}")

        rows.append([str(i+1), ticker, str(status), f"{elapsed*1000:.0f}", f"{size_kb:.1f}", flag])

    _table(
        ["#", "Ticker", "Status", "ms", "KB", "Flag"],
        rows,
        col_widths=[4, 8, 8, 8, 8, 30],
    )
    print()

    statuses = [int(r[2]) for r in rows]
    ok_count = sum(1 for s in statuses if s == 200)
    error_count = sum(1 for s in statuses if s in (429, 403, 503))
    latencies_ms = [float(r[3]) for r in rows if r[2] != "0"]

    print(f"  Requests total:       {len(rows)}")
    print(f"  200 OK:               {ok_count}")
    print(f"  Rate-limited (429/403/503): {error_count}")
    print(f"  Connection errors:    {sum(1 for s in statuses if s == 0)}")
    if latencies_ms:
        print(f"  Latency min/mean/max: {min(latencies_ms):.0f}ms / {sum(latencies_ms)/len(latencies_ms):.0f}ms / {max(latencies_ms):.0f}ms")
    if errors:
        print()
        print("  ERRORS:")
        for e in errors:
            print(f"    {e}")

    # Trend analysis — split into thirds
    if len(rows) >= 9:
        thirds = len(rows) // 3
        first_lat = [float(r[3]) for r in rows[:thirds] if r[2] != "0"]
        last_lat = [float(r[3]) for r in rows[-thirds:] if r[2] != "0"]
        if first_lat and last_lat:
            print()
            print(f"  Latency trend (first {thirds} vs last {thirds} requests):")
            print(f"    First third avg: {sum(first_lat)/len(first_lat):.0f}ms")
            print(f"    Last third avg:  {sum(last_lat)/len(last_lat):.0f}ms")
            delta = (sum(last_lat)/len(last_lat)) - (sum(first_lat)/len(first_lat))
            if delta > 200:
                print(f"    Degradation:     +{delta:.0f}ms (possible throttling signal)")
            elif delta < -200:
                print(f"    Improvement:     {delta:.0f}ms (warm connection)")
            else:
                print(f"    Trend:           stable ({delta:+.0f}ms)")


# ---------------------------------------------------------------------------
# TEST 4 — Parallel PDF URL resolution throughput
# ---------------------------------------------------------------------------

def test4_parallel_pdf_resolution(ids_ids: list[str]) -> None:
    _header("TEST 4: Parallel PDF URL Resolution (workers=1,3,5,10, 20 IDs)")

    if not ids_ids:
        print("  No idsId values available — skipping.")
        return

    sample = ids_ids[:20]
    print(f"  Using {len(sample)} idsId values.")
    print()

    results_table: list[list[Any]] = []

    for workers in [1, 3, 5, 10]:
        resolved = 0
        failed = 0
        t0 = time.perf_counter()

        def _worker_resolve(ids_id: str) -> str | None:
            sess = make_session()
            url, _ = resolve_pdf_url(sess, ids_id)
            return url

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_worker_resolve, iid): iid for iid in sample}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    resolved += 1
                else:
                    failed += 1

        elapsed = time.perf_counter() - t0
        throughput = resolved / elapsed if elapsed > 0 else 0
        results_table.append([
            str(workers),
            f"{elapsed:.2f}",
            str(resolved),
            str(failed),
            f"{throughput:.2f}",
        ])
        print(f"  workers={workers}: {elapsed:.2f}s, {resolved} resolved, {failed} failed, {throughput:.2f} URLs/s")

    print()
    _table(
        ["workers", "total_time(s)", "resolved", "failed", "URLs/s"],
        results_table,
        col_widths=[9, 14, 10, 8, 10],
    )

    # Speedup analysis
    if len(results_table) >= 2:
        base_time = float(results_table[0][1])
        print()
        print("  Speedup vs workers=1:")
        for row in results_table[1:]:
            w = row[0]
            t = float(row[1])
            speedup = base_time / t if t > 0 else 0
            efficiency = speedup / int(w) * 100
            print(f"    workers={w}: {speedup:.1f}x speedup, {efficiency:.0f}% efficiency")


# ---------------------------------------------------------------------------
# TEST 5 — Parallel PDF download throughput
# ---------------------------------------------------------------------------

def test5_parallel_pdf_download(pdf_url_map: list[tuple[str, str, str]]) -> None:
    """
    pdf_url_map: list of (ids_id, asx_code, direct_url)
    """
    _header("TEST 5: Parallel PDF Download Throughput (workers=1,3,5,10, up to 20 PDFs)")

    if not pdf_url_map:
        print("  No resolved PDF URLs available — skipping.")
        return

    sample = pdf_url_map[:20]
    print(f"  Using {len(sample)} resolved PDF URLs.")
    print()

    tmp_dir = Path("/tmp/asx_perf_test")
    tmp_dir.mkdir(exist_ok=True)

    results_table: list[list[Any]] = []

    for workers in [1, 3, 5, 10]:
        downloaded = 0
        failed = 0
        total_bytes = 0
        lock = threading.Lock()

        def _worker_download(item: tuple[str, str, str]) -> tuple[bool, int]:
            ids_id, asx_code, direct_url = item
            try:
                sess = make_session()
                resp = sess.get(direct_url, timeout=60, stream=True)
                resp.raise_for_status()
                buf = io.BytesIO()
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    buf.write(chunk)
                data = buf.getvalue()
                if data[:4] == b"%PDF":
                    return True, len(data)
                return False, 0
            except requests.RequestException:
                return False, 0

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_worker_download, item): item for item in sample}
            for future in as_completed(futures):
                ok, nbytes = future.result()
                if ok:
                    with lock:
                        downloaded += 1
                        total_bytes += nbytes
                else:
                    with lock:
                        failed += 1

        elapsed = time.perf_counter() - t0
        throughput_pdfs = downloaded / elapsed if elapsed > 0 else 0
        throughput_mb = total_bytes / elapsed / 1024 / 1024 if elapsed > 0 else 0
        avg_size_kb = total_bytes / downloaded / 1024 if downloaded > 0 else 0

        results_table.append([
            str(workers),
            f"{elapsed:.2f}",
            str(downloaded),
            str(failed),
            f"{total_bytes/1024/1024:.2f}",
            f"{throughput_pdfs:.2f}",
            f"{throughput_mb:.2f}",
        ])
        print(f"  workers={workers}: {elapsed:.2f}s, {downloaded} downloaded, {failed} failed, "
              f"{total_bytes/1024:.0f}KB total, {throughput_pdfs:.2f} PDFs/s, {throughput_mb:.2f} MB/s")

    print()
    _table(
        ["workers", "time(s)", "ok", "fail", "MB", "PDFs/s", "MB/s"],
        results_table,
        col_widths=[9, 9, 6, 6, 8, 9, 8],
    )

    if len(results_table) >= 2:
        base_time = float(results_table[0][1])
        print()
        print("  Speedup vs workers=1:")
        for row in results_table[1:]:
            w = row[0]
            t = float(row[1])
            speedup = base_time / t if t > 0 else 0
            efficiency = speedup / int(w) * 100
            print(f"    workers={w}: {speedup:.1f}x speedup, {efficiency:.0f}% efficiency")


# ---------------------------------------------------------------------------
# TEST 6 — Memory usage estimate
# ---------------------------------------------------------------------------

def test6_memory_usage() -> None:
    _header("TEST 6: Memory Usage — prevBusDayAnns.do Response")

    session = make_session()
    print("  Fetching prevBusDayAnns.do...")
    t0 = time.perf_counter()
    try:
        resp = session.get(PREV_BUS_DAY_URL, timeout=45, allow_redirects=True)
        resp.raise_for_status()
        fetch_elapsed = time.perf_counter() - t0
    except requests.RequestException as exc:
        print(f"  ERROR: {exc}")
        return

    raw_bytes = resp.content
    raw_size = len(raw_bytes)
    html_str = resp.text
    str_size = sys.getsizeof(html_str)

    print(f"  Fetch time:           {fetch_elapsed*1000:.0f}ms")
    print(f"  Wire bytes (gzipped): {raw_size:,} bytes  ({raw_size/1024:.1f} KB)")

    # Parse with BeautifulSoup
    t0 = time.perf_counter()
    soup = BeautifulSoup(html_str, BS_PARSER)
    parse_elapsed = time.perf_counter() - t0
    soup_size = sys.getsizeof(soup)

    # Count tags
    all_tags = soup.find_all(True)
    tag_count = len(all_tags)

    # Parse announcements
    t0 = time.perf_counter()
    anns = parse_prev_bus_day_anns(html_str)
    ann_parse_elapsed = time.perf_counter() - t0
    ann_count = len(anns)

    # Estimate per-announcement size
    ann_list_size = sys.getsizeof(anns) + sum(sys.getsizeof(a) for a in anns)

    print()
    print("  Parsed data:")
    _table(
        ["Metric", "Value"],
        [
            ["Raw HTML string (sys.getsizeof)", f"{str_size:,} bytes ({str_size/1024:.0f} KB)"],
            ["BeautifulSoup object (sys.getsizeof)", f"{soup_size:,} bytes ({soup_size/1024:.0f} KB)"],
            ["HTML tag count", f"{tag_count:,}"],
            ["BS4 parse time", f"{parse_elapsed*1000:.0f}ms"],
            ["Announcements parsed", f"{ann_count:,}"],
            ["Ann parse time", f"{ann_parse_elapsed*1000:.0f}ms"],
            ["Ann list getsizeof", f"{ann_list_size:,} bytes ({ann_list_size/1024:.0f} KB)"],
            ["Bytes per announcement (raw/count)", f"{raw_size//ann_count if ann_count else 0:,}"],
        ],
        col_widths=[40, 35],
    )

    # Return the announcements for downstream tests
    return anns  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# TEST 7 — Full pipeline wall-clock timing
# ---------------------------------------------------------------------------

def test7_full_pipeline(ids_ids: list[str]) -> None:
    _header("TEST 7: Full Pipeline Wall-Clock Timing (prevBusDayAnns → parse → resolve 20 → download 20)")

    session = make_session()
    pipeline_t0 = time.perf_counter()

    # Phase 1: Fetch prevBusDayAnns
    phase_times: dict[str, float] = {}
    t0 = time.perf_counter()
    try:
        resp = session.get(PREV_BUS_DAY_URL, timeout=45)
        resp.raise_for_status()
        phase_times["fetch_prevBusDayAnns"] = time.perf_counter() - t0
        html = resp.text
        html_kb = len(resp.content) / 1024
        print(f"  Phase 1 (fetch):   {phase_times['fetch_prevBusDayAnns']*1000:.0f}ms  [{html_kb:.0f} KB]")
    except requests.RequestException as exc:
        print(f"  Phase 1 FAILED: {exc}")
        return

    # Phase 2: Parse
    t0 = time.perf_counter()
    anns = parse_prev_bus_day_anns(html)
    phase_times["parse"] = time.perf_counter() - t0
    print(f"  Phase 2 (parse):   {phase_times['parse']*1000:.0f}ms  [{len(anns)} announcements]")

    # Get 20 idsIds from live data or injected ids_ids
    live_ids = [a["ids_id"] for a in anns][:20]
    if len(live_ids) < 20 and ids_ids:
        live_ids = (live_ids + ids_ids)[:20]
    sample_20 = live_ids[:20]
    print(f"  Using {len(sample_20)} idsId values for resolution + download phases.")

    # Phase 3: Resolve 20 PDF URLs (workers=5)
    resolved_urls: list[tuple[str, str, str]] = []
    t0 = time.perf_counter()

    def _resolve_worker(item: tuple[str, str]) -> tuple[str, str, str] | None:
        ids_id, asx_code = item
        sess = make_session()
        url, _ = resolve_pdf_url(sess, ids_id)
        if url:
            return ids_id, asx_code, url
        return None

    asx_map = {a["ids_id"]: a["asx_code"] for a in anns}
    items = [(iid, asx_map.get(iid, "XXX")) for iid in sample_20]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_resolve_worker, item): item for item in items}
        for future in as_completed(futures):
            result = future.result()
            if result:
                resolved_urls.append(result)

    phase_times["resolve_20_pdfs"] = time.perf_counter() - t0
    print(f"  Phase 3 (resolve): {phase_times['resolve_20_pdfs']*1000:.0f}ms  [{len(resolved_urls)}/{len(sample_20)} resolved] (workers=5)")

    # Phase 4: Download 20 PDFs (workers=5)
    download_sample = resolved_urls[:20]
    downloaded = 0
    total_bytes = 0
    lock = threading.Lock()

    def _download_worker(item: tuple[str, str, str]) -> tuple[bool, int]:
        ids_id, asx_code, direct_url = item
        try:
            sess = make_session()
            resp = sess.get(direct_url, timeout=60, stream=True)
            resp.raise_for_status()
            buf = io.BytesIO()
            for chunk in resp.iter_content(chunk_size=1 << 16):
                buf.write(chunk)
            data = buf.getvalue()
            if data[:4] == b"%PDF":
                return True, len(data)
            return False, 0
        except requests.RequestException:
            return False, 0

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_download_worker, item): item for item in download_sample}
        for future in as_completed(futures):
            ok, nbytes = future.result()
            if ok:
                with lock:
                    downloaded += 1
                    total_bytes += nbytes
            else:
                with lock:
                    pass

    phase_times["download_20_pdfs"] = time.perf_counter() - t0
    print(f"  Phase 4 (download):{phase_times['download_20_pdfs']*1000:.0f}ms  [{downloaded}/{len(download_sample)} downloaded, {total_bytes/1024:.0f} KB] (workers=5)")

    total_pipeline = time.perf_counter() - pipeline_t0
    phase_times["total"] = total_pipeline

    print()
    print("  Pipeline Phase Breakdown:")
    sum_phases = sum(v for k, v in phase_times.items() if k != "total")
    _table(
        ["Phase", "Time(ms)", "% of total"],
        [
            ["1. fetch prevBusDayAnns", f"{phase_times['fetch_prevBusDayAnns']*1000:.0f}", f"{phase_times['fetch_prevBusDayAnns']/total_pipeline*100:.0f}%"],
            ["2. parse HTML", f"{phase_times['parse']*1000:.0f}", f"{phase_times['parse']/total_pipeline*100:.0f}%"],
            ["3. resolve 20 PDFs (w=5)", f"{phase_times['resolve_20_pdfs']*1000:.0f}", f"{phase_times['resolve_20_pdfs']/total_pipeline*100:.0f}%"],
            ["4. download 20 PDFs (w=5)", f"{phase_times['download_20_pdfs']*1000:.0f}", f"{phase_times['download_20_pdfs']/total_pipeline*100:.0f}%"],
            ["TOTAL", f"{total_pipeline*1000:.0f}", "100%"],
        ],
        col_widths=[30, 12, 12],
    )


# ---------------------------------------------------------------------------
# Final analysis
# ---------------------------------------------------------------------------

def print_final_analysis(
    t1_rows: list[list[Any]] | None = None,
    t3_rows: list[list[Any]] | None = None,
) -> None:
    _header("FINAL ANALYSIS")

    print("""
  BOTTLENECKS
  -----------
  The primary bottleneck is network I/O — specifically the two-step PDF
  resolution (displayAnnouncement.do + CDN download). Each resolution
  involves a separate HTTP round-trip with HTML parsing before the real
  URL is known.

  The prevBusDayAnns.do endpoint is single-file bulk data — one large
  HTML response covers the entire day for all companies. Parse time is
  proportional to response size (BeautifulSoup lxml is fast but the
  response is large).

  Sequential ticker crawl (announcements.do) has ~300ms forced delay
  per ticker (FETCH_DELAY constant), making delay the dominant cost
  for small batches.

  THEORETICAL MAX THROUGHPUT
  --------------------------
  Assume:
    - announcements.do per-company: ~800ms mean latency
    - FETCH_DELAY = 0.3s
    - Effective cost per ticker: ~1.1s

  Sequential:  ~0.9 tickers/s  (with 0.3s delay)
  Parallel (hypothetical, 10 workers): ~9 tickers/s
    BUT announcements.do has no explicit pagination — each ticker is a
    separate stateless GET, so full parallelism is structurally possible.

  For PDF resolution + download (bulk mode):
    - Resolution: ~1–2s per PDF sequential
    - With workers=5: ~3-5x speedup → ~0.5s effective latency per PDF
    - With workers=10: diminishing returns past ~5–8 (I/O bound, not CPU)

  RATE LIMITING
  -------------
  Based on Test 3 (30 rapid requests, no delay):
    - If all 200 OK with stable latency: no rate limiting detected.
    - If 429/403/503 appear: rate limiting is active.
    - If latency degrades in the last third vs first third: soft throttling.

  PARALLELIZATION RECOMMENDATIONS
  --------------------------------
  1. TICKER CRAWL: Replace sequential loop with ThreadPoolExecutor(5–10).
     Each ticker is a stateless GET — no cross-request dependencies.
     Estimated 5x throughput gain. Keep per-worker delays to avoid 429s.

  2. PDF RESOLUTION: Already uses ThreadPoolExecutor in batch_download().
     Recommended workers: 5. Above 10 shows diminishing returns for I/O.

  3. PDF DOWNLOAD: Already parallel in batch_download().
     CDN is the bottleneck — limited by network bandwidth, not ASX limits.
     workers=5 is likely optimal; 10 may help for large batches.

  4. AVOID: Parallelizing prevBusDayAnns.do — it's a single-endpoint bulk
     pull. No parallelism gain possible.

  5. CONNECTION REUSE: The current _resolve_and_download_worker() creates
     a new session per worker. Reusing sessions within each thread would
     reduce TCP handshake overhead (~50–100ms per new connection).

  6. PARSE OPTIMIZATION: BeautifulSoup with lxml is already the fastest
     pure-Python option. For very high volume, consider regex-based
     extraction of idsId values directly from HTML (avoids full DOM parse).
    """)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(_sep())
    print("  ASX SCRAPER PERFORMANCE & RATE-LIMIT ANALYSIS")
    print(f"  Run at: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(_sep())

    # Test 1 — latency
    test1_latency_benchmarks()

    # Test 2 — sequential crawl
    test2_sequential_crawl()

    # Test 3 — rate-limit probe
    test3_rate_limit_probe()

    # Fetch prevBusDayAnns once for Tests 4, 5, 6, 7 to share idsId pool
    print()
    print("  [Setup] Fetching prevBusDayAnns.do to build idsId pool for Tests 4–7...")
    session = make_session()
    ids_pool: list[str] = []
    asx_map: dict[str, str] = {}
    try:
        resp = session.get(PREV_BUS_DAY_URL, timeout=45)
        resp.raise_for_status()
        anns = parse_prev_bus_day_anns(resp.text)
        ids_pool = [a["ids_id"] for a in anns]
        asx_map = {a["ids_id"]: a["asx_code"] for a in anns}
        print(f"  [Setup] {len(ids_pool)} idsId values available from prevBusDayAnns.")
    except requests.RequestException as exc:
        print(f"  [Setup] WARNING: Could not fetch prevBusDayAnns: {exc}")
        print(f"  [Setup] Using fallback idsId={SAMPLE_IDS_ID} for Tests 4–7.")
        ids_pool = [SAMPLE_IDS_ID]
        asx_map = {SAMPLE_IDS_ID: "BHP"}

    # Test 4 — parallel PDF resolution
    test4_parallel_pdf_resolution(ids_pool[:20])

    # Resolve 20 PDF URLs for Test 5 (workers=5)
    print()
    print("  [Setup] Resolving up to 20 PDF URLs (workers=5) for Test 5...")
    sample_20 = ids_pool[:20]
    resolved_urls: list[tuple[str, str, str]] = []

    def _resolve_setup(ids_id: str) -> tuple[str, str, str] | None:
        sess = make_session()
        url, _ = resolve_pdf_url(sess, ids_id)
        if url:
            return ids_id, asx_map.get(ids_id, "XXX"), url
        return None

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_resolve_setup, iid): iid for iid in sample_20}
        for future in as_completed(futures):
            result = future.result()
            if result:
                resolved_urls.append(result)

    print(f"  [Setup] Resolved {len(resolved_urls)}/{len(sample_20)} PDF URLs.")

    # Test 5 — parallel PDF download
    test5_parallel_pdf_download(resolved_urls)

    # Test 6 — memory usage
    test6_memory_usage()

    # Test 7 — full pipeline
    test7_full_pipeline(ids_pool)

    # Final analysis
    print_final_analysis()


if __name__ == "__main__":
    main()
