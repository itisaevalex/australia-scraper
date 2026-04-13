# ASX API Endpoints — Reconnaissance Findings

Verified: 2026-04-13

---

## Infrastructure Overview

| Property | Finding |
|----------|---------|
| Backend | Java EE — `.do` URLs are Struts/Spring MVC servlets |
| Session cookie | `JSESSIONID` (Java EE standard) |
| CDN | Imperva (`X-CDN` response header, `incap_ses_*` cookies set) |
| Bot protection | None — Imperva passes requests through without challenge |
| Rate limiting | None detected — 40 rapid consecutive requests all returned 200 |
| Auth requirement | None — all endpoints are public |
| State model | Fully stateless — independent sessions return identical results |
| HTTP library needed | `requests` (plain) — `curl_cffi` not required |
| User-Agent | Optional but recommended (costs nothing, avoids trivial blocks) |

---

## ASX Announcement Endpoints

### Per-Company Announcements

**URL:** `https://www.asx.com.au/asx/v2/statistics/announcements.do`

**Method:** GET

**Parameters:**

| Parameter | Value | Notes |
|-----------|-------|-------|
| `by` | `asxCode` | Required to search by ticker |
| `asxCode` | e.g. `BHP` | 2–6 char ASX ticker (uppercase) |
| `timeframe` | `D` or `Y` | `D` = relative window, `Y` = calendar year |
| `period` | `T`, `P`, `W`, `M`, `M3`, `M6` | Only valid with `timeframe=D` |
| `year` | `1998`–`2026` | Only valid with `timeframe=Y` |

**Period values (IMPORTANT — other values silently return empty results):**

| `period` | Meaning |
|----------|---------|
| `T` | Today |
| `P` | Previous trading day |
| `W` | Past week |
| `M` | Past month |
| `M3` | Past 3 months |
| `M6` | Past 6 months |

**Calendar year examples:**
- `timeframe=Y&year=2025` — full calendar year 2025
- `timeframe=Y&year=1998` — earliest available year

**Returns:** `text/html` — a full HTML page containing a `<announcement_data>` custom tag wrapping an HTML `<table>`.

**Notes:**
- `by=companyName&companyName={PREFIX}` returns a company-chooser table listing matching `CODE - FULL NAME` entries, NOT announcement results. Must extract the ticker and re-query with `by=asxCode`.
- No server-side announcement type filtering exists. The `type`, `category`, `documentType`, `ann_type`, and `announcementType` query parameters are all ignored — they return the same result count as the baseline.
- The ASX code does NOT appear in individual table rows; it must be extracted from the page `<h2>` tag matching pattern `\(([A-Z0-9]{2,6})\)`.

---

### Previous Business Day (All Companies)

**URL:** `https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do`

**Method:** GET

**Parameters:** None

**Returns:** `text/html` — a single-page HTML response containing 488+ announcements from 331+ unique tickers. Approximate size: 1.1 MB. No pagination.

**Notes:**
- Same `<announcement_data>` + `<table>` structure as `announcements.do`, but with 4 columns instead of 3 (ASX code is the first column).
- Useful as a bulk daily snapshot — one request captures the entire previous trading day.

---

### PDF Download Flow (Two-Step)

**Step 1 — Resolve idsId to PDF URL:**

```
GET https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId={IDS_ID}
```

Returns an HTML terms-of-use interstitial page. The actual PDF URL is embedded in a hidden form field:

```html
<input name="pdfURL" value="https://announcements.asx.com.au/asxpdf/{YYYYMMDD}/pdf/{hash}.pdf">
```

**Step 2 — Download PDF:**

```
GET https://announcements.asx.com.au/asxpdf/{YYYYMMDD}/pdf/{hash}.pdf
```

Returns `application/pdf` directly.

**PDF URL pattern:** `https://announcements.asx.com.au/asxpdf/{YYYYMMDD}/pdf/{hash}.pdf`

**Key properties:**
- URLs are **permanent** — no session tokens, no expiry. The same URL returns the same bytes across independent sessions.
- Full parallel download is safe (verified with `ThreadPoolExecutor(max_workers=5)`).
- An invalid `idsId` returns an HTML page with no `<input name="pdfURL">` element; `resolve_pdf_url()` returns `None`.
- Old-style URLs (`www.asx.com.au/asxpdf/...`) issue a 302 redirect to `announcements.asx.com.au/asxpdf/...` — use the canonical domain directly.

---

### Company Directories

**Option A — ASX official CSV (recommended for full coverage):**

```
GET https://www.asx.com.au/asx/research/ASXListedCompanies.csv
```

| Property | Value |
|----------|-------|
| Format | CSV |
| Encoding | UTF-8 with BOM (`utf-8-sig`) |
| Header rows to skip | 2 prose rows; column names are on row index 2 |
| Columns | Company name, ASX code, GICS industry group |
| Company count | ~1,979 |

**Option B — MarkitDigital CSV (richer metadata):**

```
GET https://asx.api.markitdigital.com/asx-research/1.0/companies/directory/file
```

Requires `Referer: https://www.asx.com.au/` and `Origin: https://www.asx.com.au` headers.

| Property | Value |
|----------|-------|
| Format | CSV |
| Encoding | UTF-8 with BOM (`utf-8-sig`) |
| Header rows to skip | 0 (column names on row 0) |
| Columns | ASX code, Company name, GICS industry group, Listing date, Market Cap |
| Company count | ~1,845 |

The MarkitDigital CSV has fewer entries because it excludes certain non-equity instruments that appear in the ASX official CSV. Use the ASX official CSV for complete ticker coverage.

---

### Deprecated Endpoints — DO NOT USE

These were documented by the `pyasx` library (last functional circa 2022) and now return 404:

- `https://www.asx.com.au/asx/1/company/{TICKER}`
- `https://www.asx.com.au/asx/1/share/{TICKER}`
- `https://www.asx.com.au/asx/1/company/{TICKER}/announcements`

---

## HTML Parsing Structure

### Per-Company Announcements (`announcements.do`)

```
<announcement_data>               ← custom tag wrapping the entire table
  <table>
    <tbody>
      <tr class="" | "altrow">    ← one row per announcement
        <td>                      ← col 0: Date + Time
          {date_text}             ← first NavigableString, e.g. "14/04/2026"
          <span class="dates-time">09:30</span>
        </td>
        <td class="pricesens">    ← col 1: Price sensitivity flag
          <img class="pricesens"> ← present when price-sensitive; absent otherwise
        </td>
        <td>                      ← col 2: Headline + download link
          <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=XXXXXXXX">
            {headline_text}       ← first NavigableString inside the <a>
            <span class="page">4 pages</span>
            <span class="filesize">123 kB</span>
          </a>
        </td>
      </tr>
    </tbody>
  </table>
</announcement_data>
```

The ASX code is NOT in the table rows. Extract from the page `<h2>` tag:
```
<h2>BHP GROUP LIMITED (BHP)</h2>
```
Pattern: `\(([A-Z0-9]{2,6})\)`

### Previous Business Day (`prevBusDayAnns.do`)

Same structure but with 4 `<td>` columns:

| Index | Content |
|-------|---------|
| 0 | ASX code (plain text) |
| 1 | Date + Time (same structure as above) |
| 2 | Price sensitivity (same structure as above) |
| 3 | Headline + download link (same structure as above) |

Header row contains `<th>` elements; data rows contain `<td>` elements. Filter with `row.find("td")` to skip the header.

### Price Sensitivity Detection

```python
def _is_price_sensitive(td) -> bool:
    img = td.find("img", class_="pricesens")
    if img:
        return True
    # Fallback
    img_fallback = td.find("img", alt=re.compile(r"asterix|price", re.I))
    return img_fallback is not None
```

An empty or whitespace-only `<td class="pricesens">` means NOT price-sensitive.

### Date Range Span

The page contains a span with the search window description:

```html
<span class="searchperiod">Released between 14/01/2026 and 14/04/2026</span>
```

---

## ASIC Bulk Data (Company Metadata Enrichment)

**URL:** `https://data.gov.au/data/dataset/asic-companies`

| Property | Value |
|----------|-------|
| Format | TSV-delimited `.csv` files (tab separator, despite `.csv` extension) |
| Encoding | ISO-8859-1 (Latin-1) |
| Columns | 14 (ACN, company name, type, class, subclass, status, ABN, and more) |
| Company count | ~3.4 million |
| File size | ~77 MB ZIP |
| Update frequency | Every Tuesday |
| License | Creative Commons Attribution 3.0 Australia |
| Auth | None — completely free |

Use for enriching ASX company records with ACN, ABN, company status, and registration dates. Not a source of filing documents.

---

## ASIC Connect — Deprioritised

**URL:** `https://connectonline.asic.gov.au/RegistrySearch/faces/landing/SearchRegisters.jspx`

| Finding | Detail |
|---------|--------|
| Auth wall | Oracle Access Manager — login required for most operations |
| Framework | Oracle ADF / JSF — stateful, `.jspx` URLs |
| Public API | None — SOAP API only, requires DSP (Data Service Provider) approval |
| GitHub prior art | Zero repos found that successfully scrape ASIC Connect |
| Verdict | NOT worth pursuing for this scraper |

ASIC Connect is the only path to prospectuses and AFSL-related regulatory documents, but the auth and framework complexity far exceed the value for this project's scope. Deprioritise unless prospectus coverage becomes a hard requirement.

---

## Announcement Type Classification

There is no server-side announcement type parameter. All type filtering must be done client-side by parsing the headline text.

Common headline prefixes observed in `prevBusDayAnns.do` output (ordered by frequency, approximate):

- Quarterly Activities Report
- Change in substantial holding
- Appendix 4C
- Change of Director's Interest Notice
- Half Yearly Report
- Appendix 3B
- Notice of Annual General Meeting
- Annual Report
- Investor Presentation
- Becoming a substantial holder
- Ceasing to be a substantial holder
- Appendix 4E
- Results of Meeting
- Market Update
- Other

Classification approach: split headline on ` - `, take the first segment, strip trailing digits.

---

## Scraper Architecture Implications

1. **No session init required** — start crawling immediately with a plain `requests.Session`.
2. **Random-access pagination is safe** — URLs are permanent and stateless; collect all `idsId` values first, then batch-download PDFs in parallel.
3. **Full parallelism is safe** — no stateful server, no download-before-paginate constraint.
4. **prevBusDayAnns.do is the daily firehose** — one request per day captures all announcements; use for monitoring mode.
5. **announcements.do with `timeframe=Y`** — use for historical backfill per ticker, year by year from 1998.
6. **PDF resolve adds one extra HTTP round-trip per document** — cache resolved `pdfURL` values in SQLite to avoid redundant requests on re-runs.
7. **No announcement type filtering at fetch time** — classify by headline text after parsing.
