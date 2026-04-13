"""
downloader.py — PDF resolution and download helpers for the ASX scraper.

Two-step PDF resolution:
  1. GET displayAnnouncement.do?display=pdf&idsId=... -> HTML terms page
  2. Parse <input name="pdfURL"> -> real CDN URL

Downloads are parallelised with ThreadPoolExecutor. Each worker creates its
own requests.Session. All SQLite writes are deferred to the calling thread.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from db import mark_downloaded
from http_utils import make_session, safe_get

log = logging.getLogger("asx_scraper")

ASX_BASE = "https://www.asx.com.au"
DISPLAY_ANNOUNCEMENT_URL = f"{ASX_BASE}/asx/v2/statistics/displayAnnouncement.do"
DOCUMENTS_DIR = Path("documents")

TICKER_RE = re.compile(r"^[A-Z0-9]{2,6}$")
IDS_ID_RE = re.compile(r"^[A-Za-z0-9]{1,64}$")

try:
    import lxml  # noqa: F401
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"


def resolve_direct_pdf_url(session: requests.Session, ids_id: str) -> str | None:
    """Resolve the real CDN PDF URL for a given idsId.

    Performs a two-step HTTP dance:
      1. Fetch the displayAnnouncement terms page.
      2. Extract the hidden <input name="pdfURL"> value.

    Args:
        session: An active requests.Session.
        ids_id:  The ASX announcement identifier string.

    Returns:
        The direct CDN URL string, or None if resolution fails.
    """
    url = f"{DISPLAY_ANNOUNCEMENT_URL}?display=pdf&idsId={ids_id}"
    resp = safe_get(session, url)
    if resp is None:
        return None
    soup = BeautifulSoup(resp.text, BS_PARSER)
    pdf_input = soup.find("input", {"name": "pdfURL"})
    if pdf_input and pdf_input.get("value"):
        return str(pdf_input["value"])
    log.debug("No pdfURL input found for idsId=%s", ids_id)
    return None


def download_pdf(
    session: requests.Session, ids_id: str, asx_code: str, direct_url: str
) -> tuple[str, str] | None:
    """Download a PDF to documents/{asx_code}/{ids_id}.pdf.

    Validates identifiers before writing to disk to prevent path traversal.
    Skips files that already exist (idempotent).

    Args:
        session:    An active requests.Session.
        ids_id:     The ASX announcement identifier.
        asx_code:   The ASX company ticker code.
        direct_url: The resolved CDN PDF URL.

    Returns:
        A tuple of (direct_url, local_path) on success, or None on failure.
    """
    if not TICKER_RE.match(asx_code) or not IDS_ID_RE.match(ids_id):
        log.warning("Rejected suspicious asx_code=%r ids_id=%r", asx_code, ids_id)
        return None

    dest_dir = DOCUMENTS_DIR / asx_code
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{ids_id}.pdf"

    if dest_path.exists():
        log.debug("Already downloaded: %s", dest_path)
        return direct_url, str(dest_path)

    try:
        with session.get(direct_url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            first_chunk = True
            with dest_path.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    if first_chunk and not chunk[:4].startswith(b"%PDF"):
                        log.warning(
                            "Response for ids_id=%s does not look like a PDF", ids_id
                        )
                        fh.close()
                        dest_path.unlink(missing_ok=True)
                        return None
                    first_chunk = False
                    fh.write(chunk)
    except requests.RequestException as exc:
        log.warning("PDF download failed for ids_id=%s: %s", ids_id, exc)
        dest_path.unlink(missing_ok=True)
        return None

    log.debug("Saved %s (%d bytes)", dest_path, dest_path.stat().st_size)
    return direct_url, str(dest_path)


def _resolve_and_download_worker(row: dict) -> tuple[str, str, str] | None:
    """Thread worker: resolve PDF URL and download.

    Creates its own requests.Session per worker.
    Does NO database writes — SQLite is not thread-safe.

    Args:
        row: A dict representing an announcements DB row.

    Returns:
        A tuple of (ids_id, direct_url, local_path) on success, or None.
    """
    session = make_session()
    ids_id = row["ids_id"]
    asx_code = row["asx_code"]

    direct_url = resolve_direct_pdf_url(session, ids_id)
    if not direct_url:
        log.warning("Could not resolve PDF URL for ids_id=%s", ids_id)
        return None

    result = download_pdf(session, ids_id, asx_code, direct_url)
    if result:
        url, path = result
        log.info("Downloaded %s -> %s", ids_id, path)
        return ids_id, url, path

    log.warning("PDF download failed for ids_id=%s", ids_id)
    return None


def batch_download(
    conn: sqlite3.Connection, rows: list[dict], workers: int = 5
) -> int:
    """Download all undownloaded PDFs in parallel.

    Workers do HTTP + parse only. The main thread handles all SQLite writes
    after the executor completes.

    Args:
        conn:    The main-thread SQLite connection for DB writes.
        rows:    List of announcement dicts with ids_id/asx_code fields.
        workers: Number of parallel download threads.

    Returns:
        Count of successfully downloaded PDFs.
    """
    if not rows:
        log.info("No PDFs to download.")
        return 0

    log.info("Downloading %d PDFs with %d workers...", len(rows), workers)
    completed: list[tuple[str, str, str]] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_resolve_and_download_worker, row): row["ids_id"]
            for row in rows
        }
        for future in as_completed(futures):
            ids_id = futures[future]
            try:
                result = future.result()
                if result:
                    completed.append(result)
            except Exception as exc:
                log.error("Unexpected error downloading ids_id=%s: %s", ids_id, exc)

    # Update database from main thread (SQLite is single-threaded)
    for ids_id, direct_url, path in completed:
        mark_downloaded(conn, ids_id, direct_url, path)

    log.info("Download complete: %d/%d succeeded", len(completed), len(rows))
    return len(completed)
