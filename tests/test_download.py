"""
test_download.py — Unit tests for PDF resolution and download functions.

All HTTP calls are mocked with unittest.mock so no network is needed.

Tests cover:
  - resolve_direct_pdf_url()
  - download_pdf()
"""
from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import requests

from downloader import download_pdf, resolve_direct_pdf_url


# ---------------------------------------------------------------------------
# resolve_direct_pdf_url() — two-step PDF URL extraction
# ---------------------------------------------------------------------------


class TestResolveDirectPdfUrl:
    def _make_mock_session(self, html: str, status_code: int = 200) -> MagicMock:
        """Build a mock requests.Session that returns the given HTML."""
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        mock_session = MagicMock(spec=requests.Session)
        return mock_session, mock_resp

    def test_extracts_pdf_url_from_terms_page_html(self, terms_page_with_pdf_url):
        # Arrange
        mock_resp = MagicMock()
        mock_resp.text = terms_page_with_pdf_url
        mock_resp.raise_for_status = MagicMock()

        with patch("downloader.safe_get", return_value=mock_resp):
            session = MagicMock(spec=requests.Session)
            # Act
            url = resolve_direct_pdf_url(session, "ABCDEF12")
        # Assert
        assert url == "https://announcements.asx.com.au/asxpdf/20250101/pdf/abcdef123.pdf"

    def test_extracts_pdf_url_from_real_terms_fixture(self, terms_page_html):
        # Arrange
        mock_resp = MagicMock()
        mock_resp.text = terms_page_html
        mock_resp.raise_for_status = MagicMock()

        with patch("downloader.safe_get", return_value=mock_resp):
            session = MagicMock(spec=requests.Session)
            # Act
            url = resolve_direct_pdf_url(session, "03082110")
        # Assert
        assert url is not None
        assert url.startswith("https://announcements.asx.com.au/asxpdf/")
        assert url.endswith(".pdf")

    def test_returns_none_when_no_pdf_url_input(self, terms_page_without_pdf_url):
        # Arrange
        mock_resp = MagicMock()
        mock_resp.text = terms_page_without_pdf_url
        mock_resp.raise_for_status = MagicMock()

        with patch("downloader.safe_get", return_value=mock_resp):
            session = MagicMock(spec=requests.Session)
            # Act
            url = resolve_direct_pdf_url(session, "MISSING00")
        # Assert
        assert url is None

    def test_returns_none_when_safe_get_fails(self):
        # Arrange — safe_get returns None (network failure, 404, etc.)
        with patch("downloader.safe_get", return_value=None):
            session = MagicMock(spec=requests.Session)
            # Act
            url = resolve_direct_pdf_url(session, "NONET001")
        # Assert
        assert url is None

    def test_correct_url_is_constructed_for_ids_id(self):
        # Arrange — capture the URL passed to safe_get
        captured_urls: list[str] = []

        def fake_safe_get(session, url, **kwargs):
            captured_urls.append(url)
            return None  # simulate failure, but we only care about the URL

        with patch("downloader.safe_get", side_effect=fake_safe_get):
            session = MagicMock(spec=requests.Session)
            resolve_direct_pdf_url(session, "03082041")

        assert len(captured_urls) == 1
        assert "display=pdf" in captured_urls[0]
        assert "idsId=03082041" in captured_urls[0]

    def test_pdf_url_input_with_empty_value_returns_none(self):
        # A pdfURL input that exists but has no value
        html = '<html><body><input name="pdfURL" value=""></body></html>'
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        with patch("downloader.safe_get", return_value=mock_resp):
            session = MagicMock(spec=requests.Session)
            url = resolve_direct_pdf_url(session, "EMPTYVAL")

        assert url is None


# ---------------------------------------------------------------------------
# download_pdf() — streaming download with validation
# ---------------------------------------------------------------------------


class TestDownloadPdf:
    """Tests for download_pdf(). Uses tmp_path fixture for file isolation."""

    def _make_streaming_response(
        self, chunks: list[bytes], status_code: int = 200
    ) -> MagicMock:
        """Create a mock streaming response that yields the given chunks."""
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raise_for_status = MagicMock()
        mock_resp.iter_content = MagicMock(return_value=iter(chunks))
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    def test_successful_download_returns_url_and_path(self, tmp_path):
        # Arrange
        pdf_bytes = b"%PDF-1.4 some content"
        mock_resp = self._make_streaming_response([pdf_bytes])
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            # Act
            result = download_pdf(mock_session, "12345678", "BHP", "https://cdn.example.com/file.pdf")

        # Assert
        assert result is not None
        direct_url, local_path = result
        assert direct_url == "https://cdn.example.com/file.pdf"
        assert Path(local_path).exists()

    def test_downloaded_file_has_correct_name(self, tmp_path):
        # Arrange
        pdf_bytes = b"%PDF-1.4 test"
        mock_resp = self._make_streaming_response([pdf_bytes])
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "TESTID01", "CBA", "https://cdn.example.com/x.pdf")

        assert result is not None
        _, local_path = result
        assert Path(local_path).name == "TESTID01.pdf"

    def test_downloaded_file_is_in_asx_code_subdirectory(self, tmp_path):
        # Arrange
        pdf_bytes = b"%PDF-1.4 test"
        mock_resp = self._make_streaming_response([pdf_bytes])
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "DIRID001", "NAB", "https://cdn.example.com/x.pdf")

        assert result is not None
        _, local_path = result
        assert "NAB" in Path(local_path).parts

    def test_rejects_non_pdf_response_by_magic_bytes(self, tmp_path):
        # Arrange — response starts with HTML, not %PDF
        html_bytes = b"<html>This is not a PDF</html>"
        mock_resp = self._make_streaming_response([html_bytes])
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "NOTPDF01", "BHP", "https://cdn.example.com/x.pdf")

        assert result is None

    def test_rejects_suspicious_asx_code_with_path_traversal(self, tmp_path):
        # Arrange — asx_code contains slashes → should be rejected
        mock_session = MagicMock(spec=requests.Session)
        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(
                mock_session, "12345678", "../../../etc/passwd",
                "https://cdn.example.com/x.pdf"
            )
        assert result is None

    def test_rejects_ids_id_with_path_traversal(self, tmp_path):
        # Arrange — ids_id contains special chars → rejected
        mock_session = MagicMock(spec=requests.Session)
        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(
                mock_session, "../etc/passwd", "BHP",
                "https://cdn.example.com/x.pdf"
            )
        assert result is None

    def test_returns_none_on_http_error(self, tmp_path):
        # Arrange — session.get raises a RequestException
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.side_effect = requests.RequestException("Connection refused")

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "ERR00001", "BHP", "https://cdn.example.com/x.pdf")

        assert result is None

    def test_no_partial_file_left_after_non_pdf_rejection(self, tmp_path):
        # Arrange
        html_bytes = b"<html>Not a PDF at all</html>"
        mock_resp = self._make_streaming_response([html_bytes])
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            download_pdf(mock_session, "CLEAN001", "BHP", "https://cdn.example.com/x.pdf")

        dest_path = tmp_path / "BHP" / "CLEAN001.pdf"
        assert not dest_path.exists()

    def test_no_partial_file_left_after_http_error(self, tmp_path):
        # Arrange — get itself succeeds but iter_content raises
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.iter_content = MagicMock(
            side_effect=requests.RequestException("Broken pipe")
        )
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "BROKN001", "BHP", "https://cdn.example.com/x.pdf")

        assert result is None
        dest_path = tmp_path / "BHP" / "BROKN001.pdf"
        assert not dest_path.exists()

    def test_skips_download_when_file_already_exists(self, tmp_path):
        # Arrange — pre-create the file so download should be skipped
        dest_dir = tmp_path / "BHP"
        dest_dir.mkdir(parents=True)
        dest_file = dest_dir / "EXIST001.pdf"
        dest_file.write_bytes(b"%PDF-1.4 already here")

        mock_session = MagicMock(spec=requests.Session)

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(
                mock_session, "EXIST001", "BHP",
                "https://cdn.example.com/exist001.pdf"
            )

        # Should return success without calling session.get
        assert result is not None
        mock_session.get.assert_not_called()

    def test_asx_code_too_short_is_rejected(self, tmp_path):
        # TICKER_RE requires 2-6 chars: single char is invalid
        mock_session = MagicMock(spec=requests.Session)
        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "12345678", "X", "https://cdn.example.com/x.pdf")
        assert result is None

    def test_asx_code_too_long_is_rejected(self, tmp_path):
        # TICKER_RE requires 2-6 chars: 7 chars is invalid
        mock_session = MagicMock(spec=requests.Session)
        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "12345678", "TOOLONG7", "https://cdn.example.com/x.pdf")
        assert result is None

    def test_multiple_chunks_are_all_written(self, tmp_path):
        # Arrange — PDF split into two chunks
        chunk1 = b"%PDF-1.4 chunk one "
        chunk2 = b"chunk two end"
        mock_resp = self._make_streaming_response([chunk1, chunk2])
        mock_session = MagicMock(spec=requests.Session)
        mock_session.get.return_value = mock_resp

        with patch("downloader.DOCUMENTS_DIR", tmp_path):
            result = download_pdf(mock_session, "MULTI001", "BHP", "https://cdn.example.com/x.pdf")

        assert result is not None
        _, local_path = result
        content = Path(local_path).read_bytes()
        assert content == chunk1 + chunk2


# ---------------------------------------------------------------------------
# batch_download() — parallel download coordinator
# ---------------------------------------------------------------------------


class TestBatchDownload:
    """Tests for batch_download(): the main thread coordinator."""

    def _row(self, ids_id: str, asx_code: str = "BHP") -> dict:
        return {
            "ids_id": ids_id,
            "asx_code": asx_code,
            "pdf_url": f"https://example.com/{ids_id}.pdf",
        }

    def test_returns_zero_when_rows_is_empty(self, mem_db):
        from downloader import batch_download
        result = batch_download(mem_db, rows=[])
        assert result == 0

    def test_returns_count_of_successful_downloads(self, mem_db, tmp_path):
        from downloader import batch_download
        from db import upsert_announcement, Announcement

        # Insert the announcement row first so mark_downloaded has something to update
        ann = Announcement(
            ids_id="BATCH001", asx_code="BHP", date="01/01/2025", time=None,
            headline="Test", announcement_type="other",
            pdf_url="https://cdn.example.com/BATCH001.pdf",
            file_size=None, num_pages=None, price_sensitive=False,
        )
        upsert_announcement(mem_db, ann)

        def fake_worker(row: dict):
            # Simulate successful download
            ids_id = row["ids_id"]
            return ids_id, f"https://cdn.example.com/{ids_id}.pdf", f"/tmp/{ids_id}.pdf"

        with patch("downloader._resolve_and_download_worker", side_effect=fake_worker):
            count = batch_download(mem_db, rows=[self._row("BATCH001")], workers=1)

        assert count == 1

    def test_returns_zero_when_all_workers_fail(self, mem_db):
        from downloader import batch_download

        def failing_worker(row: dict):
            return None

        with patch("downloader._resolve_and_download_worker", side_effect=failing_worker):
            count = batch_download(mem_db, rows=[self._row("FAIL001")], workers=1)

        assert count == 0

    def test_marks_successful_downloads_in_db(self, mem_db, tmp_path):
        from downloader import batch_download
        from db import upsert_announcement, Announcement, fetch_undownloaded

        ann = Announcement(
            ids_id="MARK001", asx_code="BHP", date="01/01/2025", time=None,
            headline="Test", announcement_type="other",
            pdf_url="https://cdn.example.com/MARK001.pdf",
            file_size=None, num_pages=None, price_sensitive=False,
        )
        upsert_announcement(mem_db, ann)

        def fake_worker(row: dict):
            ids_id = row["ids_id"]
            return ids_id, f"https://cdn.example.com/{ids_id}.pdf", f"/tmp/{ids_id}.pdf"

        with patch("downloader._resolve_and_download_worker", side_effect=fake_worker):
            batch_download(mem_db, rows=[self._row("MARK001")], workers=1)

        # Should no longer be in undownloaded queue
        pending = fetch_undownloaded(mem_db)
        ids = [r["ids_id"] for r in pending]
        assert "MARK001" not in ids

    def test_handles_worker_exception_gracefully(self, mem_db):
        """An unexpected exception raised by a future should not crash batch_download."""
        from downloader import batch_download

        def exploding_worker(row: dict):
            raise RuntimeError("unexpected crash")

        with patch("downloader._resolve_and_download_worker", side_effect=exploding_worker):
            count = batch_download(mem_db, rows=[self._row("CRASH001")], workers=1)

        # Should return 0 (no successful downloads) without raising
        assert count == 0


# ---------------------------------------------------------------------------
# _resolve_and_download_worker() — thread worker end-to-end
# ---------------------------------------------------------------------------


class TestResolveAndDownloadWorker:
    def test_returns_tuple_on_success(self, tmp_path):
        from downloader import _resolve_and_download_worker

        pdf_bytes = b"%PDF-1.4 worker test"

        def fake_resolve(session, ids_id):
            return f"https://cdn.example.com/{ids_id}.pdf"

        def fake_download(session, ids_id, asx_code, direct_url):
            return (direct_url, f"/tmp/{ids_id}.pdf")

        with patch("downloader.resolve_direct_pdf_url", side_effect=fake_resolve), \
             patch("downloader.download_pdf", side_effect=fake_download), \
             patch("downloader.make_session", return_value=MagicMock()):
            result = _resolve_and_download_worker({"ids_id": "WRK00001", "asx_code": "BHP"})

        assert result is not None
        ids_id, url, path = result
        assert ids_id == "WRK00001"
        assert "WRK00001" in url

    def test_returns_none_when_resolve_fails(self, tmp_path):
        from downloader import _resolve_and_download_worker

        with patch("downloader.resolve_direct_pdf_url", return_value=None), \
             patch("downloader.make_session", return_value=MagicMock()):
            result = _resolve_and_download_worker({"ids_id": "NORES01", "asx_code": "BHP"})

        assert result is None

    def test_returns_none_when_download_fails(self, tmp_path):
        from downloader import _resolve_and_download_worker

        with patch("downloader.resolve_direct_pdf_url", return_value="https://cdn.example.com/x.pdf"), \
             patch("downloader.download_pdf", return_value=None), \
             patch("downloader.make_session", return_value=MagicMock()):
            result = _resolve_and_download_worker({"ids_id": "DLFAIL1", "asx_code": "CBA"})

        assert result is None
