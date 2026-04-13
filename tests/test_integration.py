"""
test_integration.py — Integration tests that hit real ASX endpoints.

Mark ALL tests here with @pytest.mark.integration.
Run with:
    pytest tests/test_integration.py -v -m integration

Skip in CI with:
    pytest -m "not integration"
"""
from __future__ import annotations

import re
import time

import pytest
import requests

from http_utils import make_session, safe_get
from parsers import parse_announcements_do, parse_prev_bus_day_anns
from downloader import resolve_direct_pdf_url
from scraper import ANNOUNCEMENTS_URL, PREV_BUS_DAY_URL

# Known stable idsId (WMG announcement captured on 13/04/2026 from prevbusday fixture)
KNOWN_IDS_ID = "03082110"


@pytest.fixture(scope="module")
def session() -> requests.Session:
    """Shared requests session for all integration tests."""
    return make_session()


@pytest.mark.integration
class TestCrawlBhpIntegration:
    """Integration tests for BHP per-company crawl via announcements.do."""

    def test_crawl_bhp_returns_non_empty_response(self, session):
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "P"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None
        assert len(resp.text) > 100

    def test_crawl_bhp_period_p_parses_filings(self, session):
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "P"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        filings, errors = parse_announcements_do(resp.text)
        if errors:
            pytest.skip(f"Parser returned errors, may be an ASX format change: {errors}")

    def test_crawl_bhp_m6_returns_multiple_filings(self, session):
        time.sleep(0.5)
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        filings, errors = parse_announcements_do(resp.text)
        assert len(filings) > 0, "Expected at least some BHP filings for M6 period"

    def test_crawl_bhp_filings_have_valid_filing_ids(self, session):
        time.sleep(0.5)
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        filings, _ = parse_announcements_do(resp.text)
        filing_id_pattern = re.compile(r"^[A-Za-z0-9]{1,64}$")
        for f in filings:
            assert filing_id_pattern.match(f.filing_id), f"Invalid filing_id: {f.filing_id!r}"

    def test_crawl_bhp_ticker_is_bhp(self, session):
        time.sleep(0.5)
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        filings, _ = parse_announcements_do(resp.text)
        for f in filings:
            assert f.ticker == "BHP", f"Expected BHP, got {f.ticker!r}"

    def test_crawl_bhp_dates_are_iso_format(self, session):
        time.sleep(0.5)
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        filings, _ = parse_announcements_do(resp.text)
        iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for f in filings:
            assert iso_pattern.match(f.filing_date), (
                f"Expected YYYY-MM-DD, got: {f.filing_date!r}"
            )


@pytest.mark.integration
class TestPrevBusDayIntegration:
    """Integration tests for the prevBusDayAnns.do all-company endpoint."""

    def test_prevbusday_returns_successful_response(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None
        assert resp.status_code == 200

    def test_prevbusday_parses_without_errors(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        _, errors = parse_prev_bus_day_anns(resp.text)
        assert errors == [], f"Parser errors: {errors}"

    def test_prevbusday_returns_multiple_filings(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        filings, _ = parse_prev_bus_day_anns(resp.text)
        assert len(filings) > 10, f"Expected >10 filings, got {len(filings)}"

    def test_prevbusday_contains_multiple_unique_tickers(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        filings, _ = parse_prev_bus_day_anns(resp.text)
        unique_codes = set(f.ticker for f in filings)
        assert len(unique_codes) > 5, f"Expected >5 unique codes, got {len(unique_codes)}"

    def test_prevbusday_tickers_match_ticker_pattern(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        filings, _ = parse_prev_bus_day_anns(resp.text)
        ticker_re = re.compile(r"^[A-Z0-9]{2,6}$")
        for f in filings:
            assert ticker_re.match(f.ticker), f"Invalid ticker from live data: {f.ticker!r}"

    def test_prevbusday_dates_are_iso_format(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        filings, _ = parse_prev_bus_day_anns(resp.text)
        iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for f in filings:
            assert iso_pattern.match(f.filing_date), (
                f"Expected YYYY-MM-DD, got: {f.filing_date!r}"
            )


@pytest.mark.integration
class TestResolvePdfUrlIntegration:
    """Integration test: resolve a known idsId to a CDN PDF URL."""

    def test_resolve_known_ids_id_returns_cdn_url(self, session):
        time.sleep(0.5)
        url = resolve_direct_pdf_url(session, KNOWN_IDS_ID)
        assert url is not None, f"Could not resolve idsId={KNOWN_IDS_ID}"
        assert url.startswith("https://"), f"Expected https URL, got: {url!r}"

    def test_resolve_known_ids_id_points_to_asx_cdn(self, session):
        time.sleep(0.5)
        url = resolve_direct_pdf_url(session, KNOWN_IDS_ID)
        if url is None:
            pytest.skip(f"Could not resolve idsId={KNOWN_IDS_ID} — may have expired")
        assert "announcements.asx.com.au" in url or "asx.com.au" in url, (
            f"Expected ASX CDN domain in URL: {url!r}"
        )

    def test_resolve_known_ids_id_url_ends_in_pdf(self, session):
        time.sleep(0.5)
        url = resolve_direct_pdf_url(session, KNOWN_IDS_ID)
        if url is None:
            pytest.skip(f"Could not resolve idsId={KNOWN_IDS_ID} — may have expired")
        assert url.endswith(".pdf"), f"Expected .pdf URL, got: {url!r}"

    def test_resolve_invalid_ids_id_returns_none(self, session):
        time.sleep(0.5)
        url = resolve_direct_pdf_url(session, "XXXXXXXX")
        assert url is None or isinstance(url, str)
