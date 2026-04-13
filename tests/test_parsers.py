"""
test_parsers.py — Unit tests for all HTML parser functions in parsers.py.

Tests cover:
  - parse_announcements_do()
  - parse_prev_bus_day_anns()
  - _parse_headline_td()
  - _is_price_sensitive()
  - _extract_ids_id()
  - _clean()
  - classify_announcement_type()
"""
from __future__ import annotations

import re
import textwrap

import pytest
from bs4 import BeautifulSoup

from parsers import (
    BS_PARSER,
    _clean,
    _extract_ids_id,
    _is_price_sensitive,
    _parse_headline_td,
    classify_announcement_type,
    parse_announcements_do,
    parse_prev_bus_day_anns,
)


# ---------------------------------------------------------------------------
# _clean() — whitespace normalisation
# ---------------------------------------------------------------------------


class TestClean:
    def test_clean_strips_leading_trailing_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_clean_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_clean_collapses_newlines_and_tabs(self):
        assert _clean("hello\n\t world") == "hello world"

    def test_clean_empty_string_returns_empty(self):
        assert _clean("") == ""

    def test_clean_none_returns_empty(self):
        assert _clean(None) == ""

    def test_clean_whitespace_only_returns_empty(self):
        assert _clean("   \n\t  ") == ""

    def test_clean_single_word_unchanged(self):
        assert _clean("BHP") == "BHP"

    def test_clean_unicode_text(self):
        assert _clean("  Réunion Holdings  ") == "Réunion Holdings"


# ---------------------------------------------------------------------------
# _extract_ids_id() — idsId extraction from displayAnnouncement URL
# ---------------------------------------------------------------------------


class TestExtractIdsId:
    def test_extracts_8_digit_ids_id(self):
        href = "/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=03082041"
        assert _extract_ids_id(href) == "03082041"

    def test_extracts_ids_id_from_full_url(self):
        href = "https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=ABCDE123"
        assert _extract_ids_id(href) == "ABCDE123"

    def test_returns_none_when_ids_id_missing(self):
        href = "/asx/v2/statistics/displayAnnouncement.do?display=pdf"
        assert _extract_ids_id(href) is None

    def test_returns_none_for_unrelated_url(self):
        href = "https://example.com/page?foo=bar"
        assert _extract_ids_id(href) is None

    def test_returns_none_for_empty_string(self):
        assert _extract_ids_id("") is None

    def test_extracts_alphanumeric_ids_id(self):
        href = "/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=ABC12345"
        assert _extract_ids_id(href) == "ABC12345"

    def test_handles_additional_query_params(self):
        href = "/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=03082041&extra=1"
        assert _extract_ids_id(href) == "03082041"


# ---------------------------------------------------------------------------
# _is_price_sensitive() — icon detection in pricesens cell
# ---------------------------------------------------------------------------


class TestIsPriceSensitive:
    def _td(self, inner_html: str) -> object:
        soup = BeautifulSoup(f"<td>{inner_html}</td>", BS_PARSER)
        return soup.find("td")

    def test_returns_true_when_pricesens_class_img_present(self):
        td = self._td('<img class="pricesens" alt="asterix" src="icon.svg">')
        assert _is_price_sensitive(td) is True

    def test_returns_true_when_alt_asterix(self):
        td = self._td('<img alt="asterix" src="icon.svg">')
        assert _is_price_sensitive(td) is True

    def test_returns_true_when_alt_price(self):
        td = self._td('<img alt="price sensitive" src="icon.svg">')
        assert _is_price_sensitive(td) is True

    def test_returns_false_when_no_img(self):
        td = self._td("")
        assert _is_price_sensitive(td) is False

    def test_returns_false_when_img_has_unrelated_alt(self):
        td = self._td('<img alt="pdf icon" src="pdf.png">')
        assert _is_price_sensitive(td) is False

    def test_returns_false_for_none(self):
        assert _is_price_sensitive(None) is False

    def test_case_insensitive_alt_matching(self):
        # alt="ASTERIX" should still match (re.I flag in scraper)
        td = self._td('<img alt="ASTERIX" src="icon.svg">')
        assert _is_price_sensitive(td) is True


# ---------------------------------------------------------------------------
# _parse_headline_td() — headline cell parsing
# ---------------------------------------------------------------------------


class TestParseHeadlineTd:
    def _td(self, inner_html: str) -> object:
        soup = BeautifulSoup(f"<td>{inner_html}</td>", BS_PARSER)
        return soup.find("td")

    def test_extracts_headline(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Annual Report 2024"
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["headline"] == "Annual Report 2024"

    def test_extracts_ids_id(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Some Headline"
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["ids_id"] == "00000001"

    def test_extracts_pdf_url_with_asx_base_prefix(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Report"
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["pdf_url"].startswith("https://www.asx.com.au")

    def test_extracts_num_pages(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Report"
            '<span class="page">10 pages</span>'
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["num_pages"] == 10

    def test_extracts_file_size(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Report"
            '<span class="filesize">120.0KB</span>'
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["file_size"] == "120.0KB"

    def test_returns_none_fields_when_no_anchor(self):
        td = self._td("<span>Some text without anchor</span>")
        result = _parse_headline_td(td)
        assert result["headline"] is None
        assert result["pdf_url"] is None
        assert result["ids_id"] is None
        assert result["file_size"] is None
        assert result["num_pages"] is None

    def test_num_pages_none_when_no_page_span(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Report"
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["num_pages"] is None

    def test_file_size_none_when_no_filesize_span(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=00000001">'
            "Report"
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["file_size"] is None

    def test_extracts_all_fields_together(self):
        td = self._td(
            '<a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=12345678">'
            "Full Year Results"
            '<span class="page">25 pages</span>'
            '<span class="filesize">500.0KB</span>'
            "</a>"
        )
        result = _parse_headline_td(td)
        assert result["headline"] == "Full Year Results"
        assert result["ids_id"] == "12345678"
        assert result["pdf_url"] == "https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=12345678"
        assert result["num_pages"] == 25
        assert result["file_size"] == "500.0KB"


# ---------------------------------------------------------------------------
# parse_announcements_do() — per-company page parser
# ---------------------------------------------------------------------------


class TestParseAnnouncementsDo:
    def test_returns_announcements_from_real_bhp_html(self, bhp_html):
        # Arrange
        # Act
        anns, errors = parse_announcements_do(bhp_html)
        # Assert
        assert len(anns) > 0

    def test_extracts_asx_code_bhp(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        assert all(a.asx_code == "BHP" for a in anns)

    def test_all_announcements_have_valid_ids_id(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        ids_id_pattern = re.compile(r"^[A-Za-z0-9]{1,64}$")
        for ann in anns:
            assert ann.ids_id, f"Missing ids_id: {ann}"
            assert ids_id_pattern.match(ann.ids_id), f"Invalid ids_id format: {ann.ids_id!r}"

    def test_dates_are_in_expected_format(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")
        for ann in anns:
            assert date_pattern.match(ann.date), f"Unexpected date format: {ann.date!r}"

    def test_headlines_are_non_empty_strings(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        for ann in anns:
            assert isinstance(ann.headline, str)
            assert len(ann.headline) > 0

    def test_pdf_url_contains_display_announcement(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        for ann in anns:
            if ann.pdf_url:
                assert "displayAnnouncement.do" in ann.pdf_url

    def test_pdf_url_is_absolute(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        for ann in anns:
            if ann.pdf_url:
                assert ann.pdf_url.startswith("https://"), f"Relative PDF URL: {ann.pdf_url!r}"

    def test_empty_results_html_returns_empty_list_and_error(self, empty_html):
        anns, errors = parse_announcements_do(empty_html)
        assert anns == []
        assert len(errors) > 0

    def test_empty_results_error_mentions_announcement_data_tag(self, empty_html):
        _, errors = parse_announcements_do(empty_html)
        assert any("announcement_data" in e for e in errors)

    def test_minimal_html_extracts_two_rows(self, minimal_announcements_html):
        anns, errors = parse_announcements_do(minimal_announcements_html)
        assert len(anns) == 2
        assert errors == []

    def test_minimal_html_extracts_asx_code(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        assert all(a.asx_code == "ACM" for a in anns)

    def test_minimal_html_first_row_is_price_sensitive(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        assert anns[0].price_sensitive is True

    def test_minimal_html_second_row_not_price_sensitive(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        assert anns[1].price_sensitive is False

    def test_minimal_html_extracts_time(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        assert anns[0].time == "10:00 am"

    def test_minimal_html_extracts_num_pages(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        assert anns[0].num_pages == 10

    def test_minimal_html_extracts_file_size(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        assert anns[0].file_size == "120.0KB"

    def test_no_announcement_data_tag_returns_error(self):
        html = "<html><body><p>Nothing here</p></body></html>"
        anns, errors = parse_announcements_do(html)
        assert anns == []
        assert any("announcement_data" in e for e in errors)

    def test_missing_table_inside_announcement_data_returns_error(self):
        html = "<html><body><announcement_data><p>No table</p></announcement_data></body></html>"
        anns, errors = parse_announcements_do(html)
        assert anns == []
        assert any("table" in e for e in errors)

    def test_row_without_ids_id_is_skipped(self):
        # Anchor with no idsId param → row should be skipped silently
        html = textwrap.dedent("""\
            <html><body>
            <h2>Announcements for SKIP (SKP)</h2>
            <announcement_data>
            <table>
              <tbody>
                <tr>
                  <td>01/01/2025</td>
                  <td class="pricesens"></td>
                  <td><a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf">No idsId</a></td>
                </tr>
              </tbody>
            </table>
            </announcement_data>
            </body></html>
        """)
        anns, _ = parse_announcements_do(html)
        assert anns == []

    def test_announcements_are_frozen_dataclasses(self, bhp_html):
        anns, _ = parse_announcements_do(bhp_html)
        for ann in anns:
            with pytest.raises((AttributeError, TypeError)):
                ann.headline = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# parse_prev_bus_day_anns() — all-company daily page parser
# ---------------------------------------------------------------------------


class TestParsePrevBusDayAnns:
    def test_returns_multiple_announcements_from_real_html(self, prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(prevbusday_html)
        assert len(anns) > 5

    def test_extracts_multiple_unique_asx_codes(self, prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(prevbusday_html)
        unique_codes = set(a.asx_code for a in anns)
        assert len(unique_codes) > 3

    def test_asx_codes_match_ticker_pattern(self, prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(prevbusday_html)
        ticker_re = re.compile(r"^[A-Z0-9]{2,6}$")
        for ann in anns:
            assert ticker_re.match(ann.asx_code), f"Invalid ticker: {ann.asx_code!r}"

    def test_no_errors_on_real_html(self, prevbusday_html):
        _, errors = parse_prev_bus_day_anns(prevbusday_html)
        assert errors == []

    def test_all_announcements_have_ids_id(self, prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(prevbusday_html)
        for ann in anns:
            assert ann.ids_id, f"Missing ids_id in {ann}"

    def test_minimal_html_extracts_two_rows(self, minimal_prevbusday_html):
        anns, errors = parse_prev_bus_day_anns(minimal_prevbusday_html)
        assert len(anns) == 2
        assert errors == []

    def test_minimal_html_extracts_correct_asx_codes(self, minimal_prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(minimal_prevbusday_html)
        codes = [a.asx_code for a in anns]
        assert "BHP" in codes
        assert "CBA" in codes

    def test_minimal_html_first_row_is_price_sensitive(self, minimal_prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(minimal_prevbusday_html)
        bhp_ann = next(a for a in anns if a.asx_code == "BHP")
        assert bhp_ann.price_sensitive is True

    def test_minimal_html_second_row_not_price_sensitive(self, minimal_prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(minimal_prevbusday_html)
        cba_ann = next(a for a in anns if a.asx_code == "CBA")
        assert cba_ann.price_sensitive is False

    def test_missing_announcement_data_tag_returns_error(self):
        html = "<html><body><p>No data here</p></body></html>"
        anns, errors = parse_prev_bus_day_anns(html)
        assert anns == []
        assert any("announcement_data" in e for e in errors)

    def test_missing_table_inside_announcement_data_returns_error(self):
        html = "<html><body><announcement_data><p>Text only</p></announcement_data></body></html>"
        anns, errors = parse_prev_bus_day_anns(html)
        assert anns == []
        assert any("table" in e for e in errors)

    def test_row_without_ids_id_is_skipped(self):
        html = textwrap.dedent("""\
            <html><body>
            <announcement_data>
            <table>
              <tr>
                <td>BHP</td>
                <td>01/01/2025</td>
                <td class="pricesens"></td>
                <td><a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf">No idsId</a></td>
              </tr>
            </table>
            </announcement_data>
            </body></html>
        """)
        anns, _ = parse_prev_bus_day_anns(html)
        assert anns == []

    def test_extracts_time_value(self, minimal_prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(minimal_prevbusday_html)
        bhp_ann = next(a for a in anns if a.asx_code == "BHP")
        assert bhp_ann.time == "9:00 am"

    def test_extracts_num_pages(self, minimal_prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(minimal_prevbusday_html)
        bhp_ann = next(a for a in anns if a.asx_code == "BHP")
        assert bhp_ann.num_pages == 12

    def test_extracts_file_size(self, minimal_prevbusday_html):
        anns, _ = parse_prev_bus_day_anns(minimal_prevbusday_html)
        bhp_ann = next(a for a in anns if a.asx_code == "BHP")
        assert bhp_ann.file_size == "250.0KB"

    def test_row_with_fewer_than_4_tds_is_skipped(self):
        html = textwrap.dedent("""\
            <html><body>
            <announcement_data>
            <table>
              <tr><td>SHORT</td><td>ROW</td></tr>
            </table>
            </announcement_data>
            </body></html>
        """)
        anns, _ = parse_prev_bus_day_anns(html)
        assert anns == []


# ---------------------------------------------------------------------------
# classify_announcement_type() — headline type classification
# ---------------------------------------------------------------------------


class TestClassifyAnnouncementType:
    def test_annual_report_classification(self):
        assert classify_announcement_type("Annual Report 2024") == "annual_report"

    def test_half_yearly_classification(self):
        assert classify_announcement_type("Half-Year Results FY25") == "half_yearly"

    def test_quarterly_classification(self):
        assert classify_announcement_type("Quarterly Activities Report") == "quarterly"

    def test_financial_results_classification(self):
        assert classify_announcement_type("Full Year Financial Results") == "financial_results"

    def test_dividend_classification(self):
        assert classify_announcement_type("Dividend/Distribution - BHP") == "dividend"

    def test_placement_classification(self):
        assert classify_announcement_type("Share Placement Announcement") == "placement"

    def test_prospectus_classification(self):
        assert classify_announcement_type("Prospectus filing") == "prospectus"

    def test_takeover_classification(self):
        assert classify_announcement_type("Takeover Bid Notice") == "takeover"

    def test_buyback_classification(self):
        assert classify_announcement_type("Buyback Programme Update") == "buyback"

    def test_trading_halt_classification(self):
        assert classify_announcement_type("Trading Halt") == "trading_halt"

    def test_cessation_classification(self):
        assert classify_announcement_type("Notification of cessation of securities") == "cessation"

    def test_substantial_holder_classification(self):
        assert classify_announcement_type("Substantial holder notice") == "substantial_holder"

    def test_agm_classification(self):
        assert classify_announcement_type("Notice of AGM") == "agm"

    def test_unrecognised_returns_other(self):
        assert classify_announcement_type("Miscellaneous Company Update") == "other"

    def test_empty_string_returns_other(self):
        assert classify_announcement_type("") == "other"

    def test_case_insensitive_matching(self):
        assert classify_announcement_type("ANNUAL REPORT") == "annual_report"
        assert classify_announcement_type("annual report") == "annual_report"

    def test_announcements_have_type_field(self, minimal_announcements_html):
        anns, _ = parse_announcements_do(minimal_announcements_html)
        for ann in anns:
            assert hasattr(ann, "announcement_type")
            assert isinstance(ann.announcement_type, str)
            assert len(ann.announcement_type) > 0
