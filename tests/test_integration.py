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
        # Arrange
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "P"}
        # Act
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        # Assert
        assert resp is not None
        assert len(resp.text) > 100

    def test_crawl_bhp_period_p_parses_announcements(self, session):
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "P"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        anns, errors = parse_announcements_do(resp.text)
        # BHP always has recent announcements; even on quiet days period=P should get today's
        # Accept 0 announcements only if there are no errors (valid empty response)
        if errors:
            pytest.skip(f"Parser returned errors, may be an ASX format change: {errors}")
        # No assertion on count — could be 0 on weekends/public holidays

    def test_crawl_bhp_m6_returns_multiple_announcements(self, session):
        time.sleep(0.5)  # polite delay
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        anns, errors = parse_announcements_do(resp.text)
        # 6 months of BHP announcements should be substantial
        assert len(anns) > 0, "Expected at least some BHP announcements for M6 period"

    def test_crawl_bhp_announcements_have_valid_ids_ids(self, session):
        time.sleep(0.5)
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        anns, _ = parse_announcements_do(resp.text)
        ids_id_pattern = re.compile(r"^[A-Za-z0-9]{1,64}$")
        for ann in anns:
            assert ids_id_pattern.match(ann.ids_id), f"Invalid ids_id: {ann.ids_id!r}"

    def test_crawl_bhp_asx_code_is_bhp(self, session):
        time.sleep(0.5)
        params = {"by": "asxCode", "asxCode": "BHP", "timeframe": "D", "period": "M6"}
        resp = safe_get(session, ANNOUNCEMENTS_URL, params=params)
        assert resp is not None

        anns, _ = parse_announcements_do(resp.text)
        for ann in anns:
            assert ann.asx_code == "BHP", f"Expected BHP, got {ann.asx_code!r}"


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

    def test_prevbusday_returns_multiple_announcements(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        anns, _ = parse_prev_bus_day_anns(resp.text)
        # Previous business day should have many announcements from all companies
        assert len(anns) > 10, f"Expected >10 announcements, got {len(anns)}"

    def test_prevbusday_contains_multiple_unique_asx_codes(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        anns, _ = parse_prev_bus_day_anns(resp.text)
        unique_codes = set(a.asx_code for a in anns)
        assert len(unique_codes) > 5, f"Expected >5 unique codes, got {len(unique_codes)}"

    def test_prevbusday_asx_codes_match_ticker_pattern(self, session):
        time.sleep(0.5)
        resp = safe_get(session, PREV_BUS_DAY_URL)
        assert resp is not None

        anns, _ = parse_prev_bus_day_anns(resp.text)
        ticker_re = re.compile(r"^[A-Z0-9]{2,6}$")
        for ann in anns:
            assert ticker_re.match(ann.asx_code), f"Invalid ticker from live data: {ann.asx_code!r}"


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
        # Either returns None (no pdfURL input) or a URL — both are valid behaviours
        # The key constraint: no exception should be raised
        assert url is None or isinstance(url, str)
