"""
exp_asx_downloads.py — ASX PDF download and company directory CSV investigation.

Tests:
  1. PDF download via displayAnnouncement.do endpoint
  2. Old ASX listed companies CSV (ASXListedCompanies.csv)
  3. MarkitDigital company directory CSV
  4. PDF URL permanence (same content across sessions)
"""

import csv
import io
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

BASE_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "*/*",
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

PDF_URL_TEMPLATE = (
    "https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do"
    "?display=pdf&idsId={ids_id}"
)

OLD_CSV_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
MARKIT_CSV_URL = (
    "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file"
)

TMP_PDF = "/tmp/test_asx.pdf"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def separator(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def sub(title: str) -> None:
    print(f"\n--- {title} ---")


def make_session() -> requests.Session:
    """Return a fresh session with browser-like headers."""
    s = requests.Session()
    s.headers.update(BASE_HEADERS)
    return s


def download_pdf(ids_id: str, dest: Optional[str] = None) -> dict:
    """
    Download a single PDF by idsId.

    Returns a result dict with keys:
      ids_id, status_code, content_type, content_length,
      elapsed_ms, is_valid_pdf, file_size, error
    """
    url = PDF_URL_TEMPLATE.format(ids_id=ids_id)
    result: dict = {
        "ids_id": ids_id,
        "url": url,
        "status_code": None,
        "content_type": None,
        "content_length": None,
        "elapsed_ms": None,
        "is_valid_pdf": False,
        "file_size": None,
        "error": None,
    }
    try:
        session = make_session()
        t0 = time.monotonic()
        resp = session.get(url, timeout=30, allow_redirects=True)
        elapsed = (time.monotonic() - t0) * 1000

        result["status_code"] = resp.status_code
        result["content_type"] = resp.headers.get("Content-Type", "")
        result["content_length"] = resp.headers.get("Content-Length", "")
        result["elapsed_ms"] = round(elapsed, 1)
        result["file_size"] = len(resp.content)

        # Check magic bytes
        result["is_valid_pdf"] = resp.content[:4] == b"%PDF"

        if dest and result["is_valid_pdf"]:
            with open(dest, "wb") as fh:
                fh.write(resp.content)
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)

    return result


def parse_csv_bytes(raw_bytes: bytes, skip_rows: int = 0) -> tuple[list[str], list[list[str]]]:
    """
    Attempt to decode and parse CSV bytes.

    Returns (column_names, rows).
    """
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw_bytes.decode("latin-1", errors="replace")

    lines = text.splitlines()
    # Skip header prose rows
    data_lines = lines[skip_rows:]
    reader = csv.reader(io.StringIO("\n".join(data_lines)))
    rows = [row for row in reader if any(cell.strip() for cell in row)]

    if not rows:
        return [], []

    columns = rows[0]
    data_rows = rows[1:]
    return columns, data_rows


# ---------------------------------------------------------------------------
# Section 1 — PDF Downloads
# ---------------------------------------------------------------------------


def section_pdf_downloads() -> None:
    separator("SECTION 1 — PDF DOWNLOADS")

    # 1a. Primary download
    sub("1a. Primary download — idsId=03082110")
    result = download_pdf("03082110", dest=TMP_PDF)
    for key, val in result.items():
        print(f"  {key:<20}: {val}")

    if result["is_valid_pdf"]:
        print(f"\n  Saved to {TMP_PDF}")
        print(f"  File on disk: {os.path.getsize(TMP_PDF):,} bytes")
    else:
        print(f"\n  NOT a valid PDF (magic bytes wrong or download failed)")
        if result["status_code"] and not result["is_valid_pdf"]:
            # Print first 200 bytes of body for diagnosis
            session = make_session()
            try:
                resp = session.get(result["url"], timeout=15)
                print(f"  First 200 chars of response: {resp.text[:200]!r}")
            except Exception as exc:  # noqa: BLE001
                print(f"  Could not re-fetch for diagnosis: {exc}")

    # 1b. Three more idsIds
    sub("1b. Additional idsIds: 03082109, 03082108, 03082107")
    for ids_id in ("03082109", "03082108", "03082107"):
        r = download_pdf(ids_id)
        status = "OK" if r["is_valid_pdf"] else "FAIL"
        print(
            f"  idsId={ids_id}  status={r['status_code']}  "
            f"valid_pdf={r['is_valid_pdf']}  size={r['file_size']:,}  "
            f"elapsed={r['elapsed_ms']}ms  [{status}]"
        )
        if r["error"]:
            print(f"    error: {r['error']}")

    # 1c. Invalid idsId
    sub("1c. Invalid idsId=99999999")
    r_bad = download_pdf("99999999")
    print(f"  status_code  : {r_bad['status_code']}")
    print(f"  content_type : {r_bad['content_type']}")
    print(f"  file_size    : {r_bad['file_size']}")
    print(f"  is_valid_pdf : {r_bad['is_valid_pdf']}")
    print(f"  elapsed_ms   : {r_bad['elapsed_ms']}")
    if r_bad["error"]:
        print(f"  error        : {r_bad['error']}")
    if r_bad["file_size"] and not r_bad["is_valid_pdf"]:
        # Show what was returned
        session = make_session()
        try:
            resp = session.get(
                PDF_URL_TEMPLATE.format(ids_id="99999999"), timeout=15
            )
            preview = resp.text[:300] if resp.text else repr(resp.content[:300])
            print(f"  Response preview: {preview!r}")
        except Exception as exc:  # noqa: BLE001
            print(f"  Could not preview: {exc}")

    # 1d. Parallel download of 5 PDFs
    sub("1d. Parallel download — 5 PDFs with ThreadPoolExecutor(max_workers=5)")
    parallel_ids = ["03082110", "03082109", "03082108", "03082107", "03082106"]
    parallel_results = []

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(download_pdf, ids_id): ids_id for ids_id in parallel_ids}
        for future in as_completed(futures):
            parallel_results.append(future.result())
    total_elapsed = (time.monotonic() - t0) * 1000

    parallel_results.sort(key=lambda r: r["ids_id"])
    ok_count = sum(1 for r in parallel_results if r["is_valid_pdf"])
    print(f"\n  Total wall time: {total_elapsed:.0f}ms  (successful: {ok_count}/{len(parallel_ids)})")
    print(f"  {'idsId':<12}  {'status':>6}  {'valid_pdf':<10}  {'size':>10}  {'elapsed_ms':>12}  {'error'}")
    print(f"  {'-'*12}  {'-'*6}  {'-'*10}  {'-'*10}  {'-'*12}  {'-'*20}")
    for r in parallel_results:
        print(
            f"  {r['ids_id']:<12}  {str(r['status_code']):>6}  "
            f"{str(r['is_valid_pdf']):<10}  {str(r['file_size'] or ''):>10}  "
            f"{str(r['elapsed_ms'] or ''):>12}  {r['error'] or ''}"
        )


# ---------------------------------------------------------------------------
# Section 2 — Old ASX CSV
# ---------------------------------------------------------------------------


def section_old_csv() -> int:
    """Returns total company count parsed from the CSV."""
    separator("SECTION 2 — OLD ASX LISTED COMPANIES CSV")
    sub(f"URL: {OLD_CSV_URL}")

    session = make_session()
    t0 = time.monotonic()
    try:
        resp = session.get(OLD_CSV_URL, timeout=30)
        elapsed = (time.monotonic() - t0) * 1000
    except Exception as exc:  # noqa: BLE001
        print(f"  FAILED: {exc}")
        return 0

    print(f"  status_code  : {resp.status_code}")
    print(f"  content_type : {resp.headers.get('Content-Type', '')}")
    print(f"  content_length (header): {resp.headers.get('Content-Length', 'N/A')}")
    print(f"  actual bytes : {len(resp.content):,}")
    print(f"  elapsed_ms   : {elapsed:.1f}")

    if resp.status_code != 200:
        print(f"  Response preview: {resp.text[:300]!r}")
        return 0

    # Show raw first 8 lines to understand structure
    raw_lines = resp.content.decode("utf-8-sig", errors="replace").splitlines()
    print(f"\n  Raw first 8 lines (to identify header prose rows):")
    for i, line in enumerate(raw_lines[:8]):
        print(f"    [{i}] {line!r}")

    # Detect how many rows to skip (non-data header rows)
    # ASXListedCompanies.csv historically has 3 header prose lines before the column row
    skip = 0
    for i, line in enumerate(raw_lines):
        stripped = line.strip()
        # Column header row contains "ASX code" or "Company name"
        if "ASX code" in stripped or "Company name" in stripped:
            skip = i
            break

    print(f"\n  Detected skip_rows={skip} (0-indexed row with column names)")
    columns, rows = parse_csv_bytes(resp.content, skip_rows=skip)

    print(f"\n  Column names ({len(columns)}):")
    for col in columns:
        print(f"    - {col!r}")

    print(f"\n  Total companies: {len(rows):,}")

    print(f"\n  First 5 rows:")
    for row in rows[:5]:
        print(f"    {row}")

    print(f"\n  Last 5 rows:")
    for row in rows[-5:]:
        print(f"    {row}")

    return len(rows)


# ---------------------------------------------------------------------------
# Section 3 — MarkitDigital CSV
# ---------------------------------------------------------------------------


def section_markit_csv() -> tuple[int, int]:
    """Returns (markit_count, old_csv_count) for comparison; old_csv_count filled in caller."""
    separator("SECTION 3 — MARKITDIGITAL COMPANY DIRECTORY CSV")
    sub(f"URL: {MARKIT_CSV_URL}")

    session = make_session()
    # MarkitDigital may need a Referer
    session.headers["Referer"] = "https://www.asx.com.au/"
    session.headers["Origin"] = "https://www.asx.com.au"

    t0 = time.monotonic()
    try:
        resp = session.get(MARKIT_CSV_URL, timeout=30)
        elapsed = (time.monotonic() - t0) * 1000
    except Exception as exc:  # noqa: BLE001
        print(f"  FAILED: {exc}")
        return 0, 0

    print(f"  status_code  : {resp.status_code}")
    print(f"  content_type : {resp.headers.get('Content-Type', '')}")
    print(f"  content_length (header): {resp.headers.get('Content-Length', 'N/A')}")
    print(f"  actual bytes : {len(resp.content):,}")
    print(f"  elapsed_ms   : {elapsed:.1f}")

    if resp.status_code != 200:
        print(f"  Response preview: {resp.text[:500]!r}")
        return 0, 0

    raw_lines = resp.content.decode("utf-8-sig", errors="replace").splitlines()
    print(f"\n  Raw first 8 lines:")
    for i, line in enumerate(raw_lines[:8]):
        print(f"    [{i}] {line!r}")

    # Detect skip rows
    skip = 0
    for i, line in enumerate(raw_lines):
        stripped = line.strip()
        if any(kw in stripped for kw in ("ASX code", "Company name", "GICS", "Ticker")):
            skip = i
            break

    print(f"\n  Detected skip_rows={skip}")
    columns, rows = parse_csv_bytes(resp.content, skip_rows=skip)

    print(f"\n  Column names ({len(columns)}):")
    for col in columns:
        print(f"    - {col!r}")

    print(f"\n  Total companies: {len(rows):,}")

    print(f"\n  First 5 rows:")
    for row in rows[:5]:
        print(f"    {row}")

    print(f"\n  Last 5 rows:")
    for row in rows[-5:]:
        print(f"    {row}")

    return len(rows), 0


# ---------------------------------------------------------------------------
# Section 4 — PDF URL Permanence
# ---------------------------------------------------------------------------


def section_pdf_permanence() -> None:
    separator("SECTION 4 — PDF URL PERMANENCE TEST")
    sub("Fetching idsId=03082110 twice with independent sessions")

    ids_id = "03082110"
    url = PDF_URL_TEMPLATE.format(ids_id=ids_id)

    session_a = make_session()
    session_b = make_session()

    t0 = time.monotonic()
    resp_a = session_a.get(url, timeout=30)
    elapsed_a = (time.monotonic() - t0) * 1000

    # Small pause to simulate different request time
    time.sleep(0.5)

    t0 = time.monotonic()
    resp_b = session_b.get(url, timeout=30)
    elapsed_b = (time.monotonic() - t0) * 1000

    print(f"  Session A: status={resp_a.status_code}  size={len(resp_a.content):,}  "
          f"Content-Length={resp_a.headers.get('Content-Length', 'N/A')}  elapsed={elapsed_a:.1f}ms")
    print(f"  Session B: status={resp_b.status_code}  size={len(resp_b.content):,}  "
          f"Content-Length={resp_b.headers.get('Content-Length', 'N/A')}  elapsed={elapsed_b:.1f}ms")

    sizes_match = len(resp_a.content) == len(resp_b.content)
    magic_a = resp_a.content[:4] == b"%PDF"
    magic_b = resp_b.content[:4] == b"%PDF"

    print(f"\n  Both valid PDFs : {magic_a and magic_b}")
    print(f"  Content lengths match: {sizes_match}")

    if sizes_match and magic_a and magic_b:
        # Deep compare first 512 bytes
        deep_match = resp_a.content[:512] == resp_b.content[:512]
        print(f"  First 512 bytes identical: {deep_match}")
        print(f"\n  VERDICT: URL is PERMANENT (same content across sessions)")
    elif not magic_a or not magic_b:
        print(f"\n  VERDICT: Download failed — cannot confirm permanence")
    else:
        print(f"\n  VERDICT: Content DIFFERS — URL may be session-scoped or dynamic")
        print(f"  Size A: {len(resp_a.content):,}  Size B: {len(resp_b.content):,}")


# ---------------------------------------------------------------------------
# Section 5 — Comparison Summary
# ---------------------------------------------------------------------------


def section_comparison(markit_count: int, old_count: int) -> None:
    separator("SECTION 5 — COMPANY DIRECTORY COMPARISON")
    print(f"  Old ASXListedCompanies.csv : {old_count:>6} companies")
    print(f"  MarkitDigital directory   : {markit_count:>6} companies")
    if old_count and markit_count:
        diff = markit_count - old_count
        sign = "+" if diff >= 0 else ""
        pct = (diff / old_count) * 100
        print(f"  Difference                : {sign}{diff} ({sign}{pct:.1f}%)")
    print()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup() -> None:
    for path in [TMP_PDF]:
        if os.path.exists(path):
            os.remove(path)
            print(f"  Cleaned up: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("ASX Download Investigation")
    print(f"Date: 2026-04-13  |  User-Agent: {BROWSER_UA[:60]}...")

    section_pdf_downloads()
    old_count = section_old_csv()
    markit_count, _ = section_markit_csv()
    section_pdf_permanence()
    section_comparison(markit_count, old_count)

    separator("CLEANUP")
    cleanup()

    separator("DONE")


if __name__ == "__main__":
    main()
