"""
test_http_utils.py — Unit tests for HTTP session and retry helper in http_utils.py.

Tests cover:
  - make_session()  : browser headers are configured
  - safe_get()      : happy path, retries, terminal error codes
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
import requests

from http_utils import BASE_HEADERS, BROWSER_UA, make_session, safe_get


# ---------------------------------------------------------------------------
# make_session() — session factory
# ---------------------------------------------------------------------------


class TestMakeSession:
    def test_returns_requests_session(self):
        sess = make_session()
        assert isinstance(sess, requests.Session)

    def test_user_agent_is_browser(self):
        sess = make_session()
        assert "Mozilla" in sess.headers.get("User-Agent", "")

    def test_user_agent_matches_browser_ua_constant(self):
        sess = make_session()
        assert sess.headers.get("User-Agent") == BROWSER_UA

    def test_accept_language_is_set(self):
        sess = make_session()
        assert sess.headers.get("Accept-Language") is not None

    def test_accept_encoding_is_set(self):
        sess = make_session()
        assert sess.headers.get("Accept-Encoding") is not None

    def test_all_base_headers_are_present(self):
        sess = make_session()
        for key in BASE_HEADERS:
            assert key in sess.headers


# ---------------------------------------------------------------------------
# safe_get() — retry logic
# ---------------------------------------------------------------------------


def _make_mock_session(side_effects: list) -> MagicMock:
    """Build a mock session whose .get() raises or returns the given sequence."""
    sess = MagicMock(spec=requests.Session)
    sess.get.side_effect = side_effects
    return sess


def _ok_response(text: str = "OK") -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


def _error_response(status_code: int) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    exc = requests.HTTPError(response=resp)
    resp.raise_for_status = MagicMock(side_effect=exc)
    return resp


class TestSafeGet:
    def test_returns_response_on_success(self):
        # Arrange
        ok = _ok_response("page content")
        ok.raise_for_status = MagicMock()
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = ok
        # Act
        result = safe_get(sess, "https://example.com", retries=1)
        # Assert
        assert result is ok

    def test_passes_params_to_get(self):
        # Arrange
        ok = _ok_response()
        ok.raise_for_status = MagicMock()
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = ok
        # Act
        safe_get(sess, "https://example.com", params={"key": "val"}, retries=1)
        # Assert
        sess.get.assert_called_once_with(
            "https://example.com", params={"key": "val"}, timeout=30, allow_redirects=True
        )

    def test_returns_none_on_404(self):
        # Arrange
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 404
        exc = requests.HTTPError(response=resp)
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = resp
        resp.raise_for_status = MagicMock(side_effect=exc)
        # Act
        result = safe_get(sess, "https://example.com", retries=3)
        # Assert
        assert result is None

    def test_returns_none_on_403(self):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 403
        exc = requests.HTTPError(response=resp)
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = resp
        resp.raise_for_status = MagicMock(side_effect=exc)
        result = safe_get(sess, "https://example.com", retries=3)
        assert result is None

    def test_returns_none_on_410(self):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 410
        exc = requests.HTTPError(response=resp)
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = resp
        resp.raise_for_status = MagicMock(side_effect=exc)
        result = safe_get(sess, "https://example.com", retries=3)
        assert result is None

    def test_returns_none_after_all_retries_exhausted(self):
        # Arrange — every attempt raises a network error
        sess = MagicMock(spec=requests.Session)
        sess.get.side_effect = requests.ConnectionError("Refused")
        # Act — patch sleep so test is instant
        with patch("http_utils.time.sleep"):
            result = safe_get(sess, "https://example.com", retries=3)
        # Assert
        assert result is None
        assert sess.get.call_count == 3

    def test_succeeds_on_second_attempt_after_transient_error(self):
        # Arrange — first call raises, second succeeds
        ok = _ok_response("data")
        ok.raise_for_status = MagicMock()
        sess = MagicMock(spec=requests.Session)
        sess.get.side_effect = [requests.ConnectionError("timeout"), ok]
        # Act
        with patch("http_utils.time.sleep"):
            result = safe_get(sess, "https://example.com", retries=2)
        # Assert
        assert result is ok
        assert sess.get.call_count == 2

    def test_terminal_404_does_not_retry(self):
        # 404 is a terminal error — no retry should happen
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 404
        exc = requests.HTTPError(response=resp)
        resp.raise_for_status = MagicMock(side_effect=exc)
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = resp

        with patch("http_utils.time.sleep") as mock_sleep:
            safe_get(sess, "https://example.com", retries=3)

        # Should only attempt once — no retry for 404
        assert sess.get.call_count == 1
        mock_sleep.assert_not_called()

    def test_uses_custom_timeout(self):
        ok = _ok_response()
        ok.raise_for_status = MagicMock()
        sess = MagicMock(spec=requests.Session)
        sess.get.return_value = ok
        safe_get(sess, "https://example.com", timeout=60, retries=1)
        sess.get.assert_called_once_with(
            "https://example.com", params=None, timeout=60, allow_redirects=True
        )
