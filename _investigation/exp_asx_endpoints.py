"""
exp_asx_endpoints.py — ASX endpoint reconnaissance with plain requests.

Tests each known ASX endpoint both with and without a browser User-Agent to
determine which ones respond to plain HTTP, what auth/state they require, and
whether a User-Agent header is necessary to avoid blocks.
"""

from __future__ import annotations

import time
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

INTERESTING_HEADERS = [
    "Content-Type",
    "Server",
    "Set-Cookie",
    "X-Powered-By",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
    "Retry-After",
    "CF-RAY",
    "X-Cache",
    "X-Amz-Cf-Id",
    "Akamai-Cache-Status",
    "x-akamai-transformed",
    "x-check-cacheable",
    "Via",
    "Location",
    "Access-Control-Allow-Origin",
]

ENDPOINTS: list[dict[str, Any]] = [
    {
        "label": "1. ASX announcements search (no params)",
        "url": "https://www.asx.com.au/asx/v2/statistics/announcements.do",
        "method": "GET",
        "params": None,
    },
    {
        "label": "2. ASX announcements by company (BHP, 1-month)",
        "url": "https://www.asx.com.au/asx/v2/statistics/announcements.do",
        "method": "GET",
        "params": {
            "by": "asxCode",
            "asxCode": "BHP",
            "timeframe": "D",
            "period": "M1",
        },
    },
    {
        "label": "3. Previous business day announcements (all companies)",
        "url": "https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do",
        "method": "GET",
        "params": None,
    },
    {
        "label": "4. PDF announcement display (HEAD only)",
        "url": (
            "https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do"
            "?display=pdf&idsId=03082110"
        ),
        "method": "HEAD",
        "params": None,
    },
    {
        "label": "5. ASX listed companies CSV (asx.com.au)",
        "url": "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
        "method": "GET",
        "params": None,
    },
    {
        "label": "6. MarkitDigital company directory CSV",
        "url": (
            "https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file"
        ),
        "method": "GET",
        "params": None,
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SEPARATOR = "=" * 80


def _is_binary(content: bytes) -> bool:
    """Return True when the response body looks like binary (e.g. PDF)."""
    if content[:4] == b"%PDF":
        return True
    # Treat any content with more than 30 % non-printable bytes as binary.
    sample = content[:512]
    non_printable = sum(1 for b in sample if b < 9 or (13 < b < 32) or b > 126)
    return len(sample) > 0 and (non_printable / len(sample)) > 0.30


def _body_preview(response: requests.Response) -> str:
    """Return a human-readable preview of the response body (first 500 chars)."""
    raw = response.content
    if not raw:
        return "(empty body)"
    if _is_binary(raw):
        return f"<BINARY / PDF — {len(raw)} bytes>"
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        return f"<decode error — {len(raw)} bytes>"
    preview = text[:500].strip()
    if len(text) > 500:
        preview += " … [truncated]"
    return preview


def _print_interesting_headers(headers: requests.structures.CaseInsensitiveDict) -> None:
    found_any = False
    for name in INTERESTING_HEADERS:
        value = headers.get(name)
        if value:
            # Truncate Set-Cookie for readability.
            if name.lower() == "set-cookie" and len(value) > 120:
                value = value[:120] + " … [truncated]"
            print(f"    {name}: {value}")
            found_any = True
    if not found_any:
        print("    (none of the interesting headers present)")


def probe(
    session: requests.Session,
    endpoint: dict[str, Any],
    with_ua: bool,
) -> None:
    """Run a single probe and print results."""
    label = endpoint["label"]
    url = endpoint["url"]
    method = endpoint["method"]
    params = endpoint.get("params")

    ua_tag = "WITH User-Agent" if with_ua else "WITHOUT User-Agent"
    print(f"\n--- {label}")
    print(f"    [{ua_tag}]")
    print(f"    URL: {url}")
    if params:
        print(f"    Params: {params}")

    headers: dict[str, str] = {}
    if with_ua:
        headers["User-Agent"] = BROWSER_UA

    try:
        start = time.perf_counter()
        if method == "HEAD":
            resp = session.head(url, headers=headers, allow_redirects=True, timeout=20)
        else:
            resp = session.get(
                url, headers=headers, params=params, allow_redirects=True, timeout=20
            )
        elapsed_ms = (time.perf_counter() - start) * 1000

        print(f"    Status: {resp.status_code}  |  Time: {elapsed_ms:.0f} ms")
        print(f"    Final URL: {resp.url}")
        print("    Headers:")
        _print_interesting_headers(resp.headers)

        if method != "HEAD":
            print("    Body preview:")
            print("      " + _body_preview(resp).replace("\n", "\n      "))

    except requests.exceptions.SSLError as exc:
        print(f"    ERROR (SSL): {exc}")
    except requests.exceptions.ConnectionError as exc:
        print(f"    ERROR (Connection): {exc}")
    except requests.exceptions.Timeout:
        print("    ERROR: Request timed out (>20s)")
    except Exception as exc:  # noqa: BLE001
        print(f"    ERROR (unexpected): {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(SEPARATOR)
    print("ASX ENDPOINT RECONNAISSANCE — exp_asx_endpoints.py")
    print(SEPARATOR)
    print(f"Browser UA: {BROWSER_UA}")
    print(f"Endpoints to test: {len(ENDPOINTS)}")
    print(f"Probes per endpoint: 2 (with UA + without UA)")

    # Use a single session so cookies accumulate naturally — mirrors real
    # browser behaviour for session-dependent endpoints.
    session = requests.Session()

    for endpoint in ENDPOINTS:
        print(f"\n{SEPARATOR}")
        print(f"ENDPOINT: {endpoint['label']}")
        print(SEPARATOR)

        # Test WITH User-Agent first.
        probe(session, endpoint, with_ua=True)

        # Small pause to avoid hammering.
        time.sleep(0.5)

        # Test WITHOUT User-Agent using a fresh session to avoid cookie carry-over.
        fresh_session = requests.Session()
        probe(fresh_session, endpoint, with_ua=False)

        time.sleep(0.5)

    print(f"\n{SEPARATOR}")
    print("RECONNAISSANCE COMPLETE")
    print(SEPARATOR)


if __name__ == "__main__":
    main()
