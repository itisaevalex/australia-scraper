"""
conftest.py — shared pytest fixtures for the ASX scraper test suite.
"""
from __future__ import annotations

import sqlite3
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# HTML fixture loaders
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bhp_html() -> str:
    """Real BHP per-company announcement HTML captured from ASX."""
    path = FIXTURES_DIR / "announcements_bhp.html"
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def prevbusday_html() -> str:
    """Real previous-business-day all-companies HTML captured from ASX."""
    path = FIXTURES_DIR / "prevbusday.html"
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def terms_page_html() -> str:
    """Real terms/displayAnnouncement HTML that contains pdfURL input."""
    path = FIXTURES_DIR / "terms_page.html"
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def empty_html() -> str:
    """Real HTML returned for an invalid/unknown ticker (no announcement_data)."""
    path = FIXTURES_DIR / "announcements_empty.html"
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Synthetic minimal HTML fixtures (for unit tests that don't need real data)
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_announcements_html() -> str:
    """Minimal synthetic announcements.do HTML with two rows: one price-sensitive."""
    return textwrap.dedent("""\
        <html><body>
        <h2>Company announcements for ACME LTD (ACM)</h2>
        <announcement_data>
        <table>
          <thead><tr><th>Date</th><th>Price sens.</th><th>Headline</th></tr></thead>
          <tbody>
            <tr>
              <td>01/01/2025<br><span class="dates-time">10:00 am</span></td>
              <td class="pricesens"><img class="pricesens" alt="asterix" src="icon.svg"></td>
              <td>
                <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">
                  Annual Report 2024
                  <span class="page">10 pages</span>
                  <span class="filesize">120.0KB</span>
                </a>
              </td>
            </tr>
            <tr>
              <td>02/01/2025<br><span class="dates-time">2:30 pm</span></td>
              <td class="pricesens"></td>
              <td>
                <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000002">
                  Quarterly Report
                  <span class="page">4 pages</span>
                  <span class="filesize">45.3KB</span>
                </a>
              </td>
            </tr>
          </tbody>
        </table>
        </announcement_data>
        </body></html>
    """)


@pytest.fixture
def minimal_prevbusday_html() -> str:
    """Minimal synthetic prevBusDayAnns.do HTML with two rows."""
    return textwrap.dedent("""\
        <html><body>
        <announcement_data>
        <table>
          <tr>
            <th>ASX Code</th><th>Date</th><th>Price sens.</th><th>Headline</th>
          </tr>
          <tr>
            <td>BHP</td>
            <td>01/01/2025<br><span class="dates-time">9:00 am</span></td>
            <td class="pricesens"><img class="pricesens" alt="asterix" src="icon.svg"></td>
            <td>
              <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=AAA00001">
                BHP Full Year Results
                <span class="page">12 pages</span>
                <span class="filesize">250.0KB</span>
              </a>
            </td>
          </tr>
          <tr>
            <td>CBA</td>
            <td>01/01/2025<br><span class="dates-time">11:00 am</span></td>
            <td class="pricesens"></td>
            <td>
              <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=BBB00002">
                Half Year Report
                <span class="page">8 pages</span>
                <span class="filesize">180.5KB</span>
              </a>
            </td>
          </tr>
        </table>
        </announcement_data>
        </body></html>
    """)


@pytest.fixture
def terms_page_with_pdf_url() -> str:
    """Synthetic terms/display page containing a pdfURL hidden input."""
    return textwrap.dedent("""\
        <html><body>
        <form action="/asx/v2/statistics/displayAnnouncement.do" method="post">
          <input type="hidden" name="pdfURL"
            value="https://announcements.asx.com.au/asxpdf/20250101/pdf/abcdef123.pdf">
          <input type="submit" value="Accept and proceed">
        </form>
        </body></html>
    """)


@pytest.fixture
def terms_page_without_pdf_url() -> str:
    """Synthetic terms page that does NOT contain a pdfURL input."""
    return textwrap.dedent("""\
        <html><body>
        <p>No PDF available for this announcement.</p>
        </body></html>
    """)


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mem_db() -> sqlite3.Connection:
    """Fresh in-memory SQLite connection with the full schema applied."""
    # Import here to avoid import-time side-effects at collection time.
    from db import get_db

    conn = get_db(db_path=":memory:")  # type: ignore[arg-type]
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Sample Announcement object
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_announcement():
    """A valid Announcement dataclass instance for DB tests."""
    from db import Announcement

    return Announcement(
        ids_id="12345678",
        asx_code="BHP",
        date="13/04/2026",
        time="5:07 pm",
        headline="Annual Report 2025",
        announcement_type="annual_report",
        pdf_url="https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=12345678",
        file_size="120.0KB",
        num_pages=42,
        price_sensitive=False,
    )
