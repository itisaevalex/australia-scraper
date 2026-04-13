"""
http_utils.py — Shared HTTP session factory and retry helper.

Kept in a separate module to avoid circular imports between scraper.py
(CLI/crawl) and downloader.py (PDF resolution/download).
"""

from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger("asx_scraper")

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


def make_session() -> requests.Session:
    """Return a fresh requests.Session pre-configured with browser headers."""
    sess = requests.Session()
    sess.headers.update(BASE_HEADERS)
    return sess


def safe_get(
    session: requests.Session,
    url: str,
    params: dict[str, str] | None = None,
    retries: int = 3,
    timeout: int = 30,
) -> requests.Response | None:
    """GET with retry logic. Returns None on unrecoverable failure.

    Args:
        session: Active requests.Session.
        url:     Target URL.
        params:  Optional query parameters.
        retries: Maximum number of attempts.
        timeout: Per-attempt timeout in seconds.

    Returns:
        The successful Response, or None after all retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            log.warning(
                "HTTP %s on %s (attempt %d/%d)",
                exc.response.status_code,
                url,
                attempt,
                retries,
            )
            if exc.response.status_code in (403, 404, 410):
                return None
        except requests.RequestException as exc:
            log.warning(
                "Request error on %s (attempt %d/%d): %s", url, attempt, retries, exc
            )
        if attempt < retries:
            time.sleep(attempt * 1.5)
    return None
