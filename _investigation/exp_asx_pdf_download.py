"""
Experiment: ASX PDF Download Flow
==================================
Verified 2026-04-13

Finding: displayAnnouncement.do returns a terms-of-use HTML page, NOT a PDF.
The actual PDF URL is in a hidden form field: <input name="pdfURL" value="https://announcements.asx.com.au/asxpdf/...">

Download flow:
1. GET /asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=XXXXXXXX
2. Parse HTML, extract pdfURL from hidden input
3. GET the pdfURL directly -> returns application/pdf

The pdfURL pattern: https://announcements.asx.com.au/asxpdf/{YYYYMMDD}/pdf/{hash}.pdf
These are permanent URLs - no session tokens, no expiry.

Old pyasx-style URLs (www.asx.com.au/asxpdf/...) 302-redirect to announcements.asx.com.au/asxpdf/...
"""

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

BASE_URL = "https://www.asx.com.au"


def resolve_pdf_url(ids_id: str) -> str | None:
    """Resolve an idsId to the actual PDF download URL."""
    url = f"{BASE_URL}/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId={ids_id}"
    r = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(r.text, "html.parser")
    pdf_input = soup.find("input", {"name": "pdfURL"})
    if pdf_input:
        return pdf_input["value"]
    return None


def download_pdf(pdf_url: str, dest_path: str) -> int:
    """Download a PDF and return the file size in bytes."""
    r = requests.get(pdf_url, headers=HEADERS)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(r.content)
    return len(r.content)


def test_resolve_and_download():
    test_ids = ["03082110", "03082109", "03082108", "03082107", "03082041"]

    print("=== Step 1: Resolve idsId -> PDF URL ===\n")
    resolved = {}
    for ids_id in test_ids:
        t0 = time.time()
        pdf_url = resolve_pdf_url(ids_id)
        elapsed = (time.time() - t0) * 1000
        resolved[ids_id] = pdf_url
        print(f"  {ids_id} -> {pdf_url} ({elapsed:.0f}ms)")
    print()

    print("=== Step 2: Verify PDFs are downloadable ===\n")
    for ids_id, pdf_url in resolved.items():
        if not pdf_url:
            print(f"  {ids_id}: SKIPPED (no URL)")
            continue
        r = requests.head(pdf_url, headers=HEADERS)
        content_type = r.headers.get("Content-Type")
        content_length = r.headers.get("Content-Length", "?")
        print(f"  {ids_id}: {r.status_code} {content_type} ({content_length} bytes)")
    print()

    print("=== Step 3: Full download + validation ===\n")
    pdf_url = resolved[test_ids[0]]
    dest = "/tmp/test_asx_announcement.pdf"
    size = download_pdf(pdf_url, dest)
    with open(dest, "rb") as f:
        magic = f.read(4)
    print(f"  Downloaded: {size} bytes")
    print(f"  Valid PDF: {magic == b'%PDF'}")
    os.remove(dest)
    print()

    print("=== Step 4: Parallel download test (5 PDFs) ===\n")
    urls = [(ids_id, url) for ids_id, url in resolved.items() if url]
    t0 = time.time()
    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(lambda u: requests.head(u, headers=HEADERS), url): ids_id
            for ids_id, url in urls
        }
        for future in as_completed(futures):
            ids_id = futures[future]
            r = future.result()
            results[ids_id] = r.status_code
    elapsed = (time.time() - t0) * 1000
    print(f"  Parallel HEAD requests: {elapsed:.0f}ms wall time")
    for ids_id, status in results.items():
        print(f"    {ids_id}: {status}")
    print()

    print("=== Step 5: Invalid idsId test ===\n")
    bad_url = resolve_pdf_url("99999999")
    print(f"  idsId=99999999 -> {bad_url}")
    bad_url2 = resolve_pdf_url("00000000")
    print(f"  idsId=00000000 -> {bad_url2}")


if __name__ == "__main__":
    test_resolve_and_download()
