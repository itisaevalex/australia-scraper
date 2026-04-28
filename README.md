<div align="left">

```
  █████╗ ██╗   ██╗███████╗████████╗██████╗  █████╗ ██╗     ██╗ █████╗
 ██╔══██╗██║   ██║██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██║     ██║██╔══██╗
 ███████║██║   ██║███████╗   ██║   ██████╔╝███████║██║     ██║███████║
 ██╔══██║██║   ██║╚════██║   ██║   ██╔══██╗██╔══██║██║     ██║██╔══██║
 ██║  ██║╚██████╔╝███████║   ██║   ██║  ██║██║  ██║███████╗██║██║  ██║
 ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝╚═╝  ╚═╝
    ███████╗██╗██╗     ██╗███╗   ██╗ ██████╗ ███████╗
    ██╔════╝██║██║     ██║████╗  ██║██╔════╝ ██╔════╝
    █████╗  ██║██║     ██║██╔██╗ ██║██║  ███╗███████╗
    ██╔══╝  ██║██║     ██║██║╚██╗██║██║   ██║╚════██║
    ██║     ██║███████╗██║██║ ╚████║╚██████╔╝███████║
    ╚═╝     ╚═╝╚══════╝╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚══════╝
     ███████╗ ██████╗██████╗  █████╗ ██████╗ ███████╗██████╗
     ██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
     ███████╗██║     ██████╔╝███████║██████╔╝█████╗  ██████╔╝
     ╚════██║██║     ██╔══██╗██╔══██║██╔═══╝ ██╔══╝  ██╔══██╗
     ███████║╚██████╗██║  ██║██║  ██║██║     ███████╗██║  ██║
     ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝  ╚═╝
```

**Australia's ASX announcements — reverse-engineered from scratch.**

*488+ filings per day. 324 companies. Two-step PDF resolution. Pure HTTP.*

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Proprietary](https://img.shields.io/badge/License-Proprietary-red.svg)](#license)

**Created by Alexander Isaev | [Data Alchemy Labs](https://github.com/itisaevalex)**

</div>

---

Production scraper for Australia's ASX (Australian Securities Exchange) announcements system. Extracts corporate announcements, financial reports, and market-sensitive disclosures into structured JSON + downloaded PDF documents.

Part of a multi-country financial filings scraper project (siblings: [Canada SEDAR+](https://github.com/itisaevalex/SedarPlusScraper), [Mexico CNBV](https://github.com/itisaevalex/MexicanReportsScraperExtended), [China CNINFO](https://github.com/itisaevalex/china-scraper), [India BSE+NSE+SEBI](https://github.com/itisaevalex/india-scraper)).

## Quick Start

```bash
pip install -r requirements.txt

# Crawl all companies from previous business day (488+ announcements)
python scraper.py crawl --all-day --download

# Crawl specific tickers
python scraper.py crawl --tickers BHP,CBA,NAB --period M6 --download

# Crawl a full year of BHP announcements
python scraper.py crawl --tickers BHP --year 2025

# Crawl top 20 companies by market cap
python scraper.py crawl --max-companies 20 --period M --download

# Monitor for new filings every 5 minutes
python scraper.py monitor --interval 300 --download

# Export to JSON
python scraper.py export --output filings.json

# Show statistics
python scraper.py stats
```

## Architecture

```
Python requests (plain HTTP, no browser)
  → announcements.do (per-company HTML tables)
  → prevBusDayAnns.do (all-company daily firehose)
    → Parse <announcement_data> tables with BeautifulSoup
      → Two-step PDF resolution (terms page → hidden pdfURL)
        → Parallel download from announcements.asx.com.au CDN
          → Cache everything in SQLite (dedup + download tracking)
```

**Zero bot protection.** No TLS fingerprinting, no JavaScript challenges, no cookie validation. Plain `requests` with a browser User-Agent is all that's needed.

## Data Source

### ASX Announcements (Australian Securities Exchange)

| Property | Value |
|----------|-------|
| Endpoint | `asx.com.au/asx/v2/statistics/announcements.do` |
| Format | Server-rendered HTML tables (not JSON) |
| Auth | None — browser User-Agent recommended but optional |
| Pagination | Time-period based (T, P, W, M, M3, M6) + year-based (1998-2026) |
| Bot Protection | **None** — Imperva CDN passes all requests through |
| Coverage | All 1,845+ ASX-listed companies |
| Documents | Two-step: terms page → `announcements.asx.com.au/asxpdf/` CDN |

### Endpoints Used

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `announcements.do?by=asxCode&asxCode=BHP&timeframe=D&period=M6` | Per-company announcements | HTML table |
| `prevBusDayAnns.do` | All companies, previous trading day | HTML table (~488 items) |
| `displayAnnouncement.do?display=pdf&idsId=XXXXXXXX` | PDF terms page | HTML with hidden `pdfURL` |
| `announcements.asx.com.au/asxpdf/{date}/pdf/{hash}.pdf` | Actual PDF download | `application/pdf` |
| `asx.api.markitdigital.com/.../companies/directory/file` | Company directory | CSV (1,845 companies) |

### Time Period Parameters

| Parameter | Meaning |
|-----------|---------|
| `period=T` | Today |
| `period=P` | Previous trading day |
| `period=W` | Past week |
| `period=M` | Past month |
| `period=M3` | Past 3 months |
| `period=M6` | Past 6 months |
| `timeframe=Y&year=2025` | Full calendar year |

**Warning:** Invalid period values (D, M1, Y1) silently return the search form instead of results.

## Reverse-Engineering Journey

### Phase 1: Parallel Reconnaissance

Five parallel agents probed all Australian financial portals simultaneously:

1. **GitHub research** — Discovered `pyasx` (Python ASX API client) which mapped `/asx/1/` JSON endpoints. **Critical finding:** these endpoints are all **dead** (404) as of 2026. The library is obsolete. Also found MarkitDigital API domain (`asx.api.markitdigital.com`) from a fork.

2. **ASX endpoint probing** — The `/asx/v2/statistics/` HTML endpoints are alive and unprotected. Backend is Java EE (JSESSIONID cookie, `.do` URL pattern = Struts/Spring MVC). Imperva CDN present but not challenging requests.

3. **ASIC Connect** — Oracle Access Manager auth wall on `asicconnect.asic.gov.au`. Registry search uses Oracle ADF/JSF (heavier than standard JSF). No public API (SOAP only, requires Digital Service Provider approval). **Verdict: deprioritized.**

4. **ASIC bulk data** — `data.gov.au/data/dataset/asic-companies` has 3.4M companies in TSV format (despite `.csv` extension), updated weekly. Free, CC BY 3.0. Useful for enrichment but contains no filing documents.

5. **ASX API deep-dive** — Tested the MarkitDigital company directory CSV (richer than ASX's own, includes market cap and listing date). Confirmed all endpoints are stateless and fully parallelizable.

### Phase 2: The PDF Discovery

The announcement table links point to `displayAnnouncement.do?display=pdf&idsId=XXXXXXXX`. This does NOT return a PDF — it returns an HTML terms-of-use page. The actual PDF URL is hidden in a form field:

```html
<form name="showAnnouncementPDFForm" method="post" action="announcementTerms.do">
    <input name="pdfURL" value="https://announcements.asx.com.au/asxpdf/20260413/pdf/06yfpn3qy3wkwr.pdf" type="hidden">
</form>
```

The scraper extracts this hidden URL and downloads the PDF directly from the CDN. URLs are permanent — no session tokens, no expiry.

### Phase 3: Validation

| Test | Result |
|------|--------|
| Per-company crawl (BHP, 1 month) | 6 announcements, 0 parse errors |
| All-day crawl (prevBusDayAnns) | 488 announcements, 331 unique tickers, 0 errors |
| Year crawl (BHP 2025) | 64 announcements |
| PDF download (parallel, 5 workers) | 461/461 succeeded |
| Rate limiting (40 rapid requests) | 0 blocked, no 429s |
| Stateless verification | Independent sessions get identical results |

### What Made This Easy (vs. Siblings)

| Country | Site | Bot Protection | State Machine | Difficulty |
|---------|------|---------------|---------------|------------|
| Mexico | CNBV | Azure WAF | ASP.NET ViewState + DevExpress | Hard |
| Canada | SEDAR+ | Radware Bot Manager | Oracle Catalyst state machine | Very Hard |
| China | CNINFO | **None** | Stateless JSON API | Easy |
| India | BSE/NSE/SEBI | Akamai (light) | Stateless REST APIs | Easy-Medium |
| **Australia** | **ASX** | **None (Imperva passthrough)** | **Stateless HTML** | **Easy** |

## SQLite Cache

All data is cached in `filings_cache.db`:

```sql
announcements (
    ids_id           TEXT PRIMARY KEY,    -- ASX document identifier
    asx_code         TEXT NOT NULL,       -- Ticker (e.g. BHP)
    date             TEXT NOT NULL,       -- Announcement date
    time             TEXT,                -- Announcement time
    headline         TEXT NOT NULL,       -- Announcement title
    pdf_url          TEXT,                -- Terms page URL
    direct_pdf_url   TEXT,                -- Actual CDN PDF URL
    file_size        TEXT,                -- File size string
    num_pages        INTEGER,             -- Page count
    price_sensitive  BOOLEAN,             -- Market-sensitive flag
    downloaded       BOOLEAN,             -- PDF downloaded?
    download_path    TEXT,                -- Local file path
    created_at       TEXT                 -- Cache timestamp
)
```

## Output Structure

```
australia-scraper/
  scraper.py              # Main scraper (crawl, monitor, export, stats)
  requirements.txt        # Python dependencies
  filings_cache.db        # SQLite cache (auto-generated)
  documents/              # Downloaded PDFs (auto-generated)
    BHP/
      03082041.pdf
      03073262.pdf
    CBA/
      03081981.pdf
  filings.json            # Exported data (via export command)
  _investigation/         # Reverse-engineering artifacts
    API_ENDPOINTS.md       # Complete endpoint documentation
    exp_asx_*.py           # Experiment scripts (rerunnable)
```

## Not Targeted

- **ASIC Connect** (`asicconnect.asic.gov.au`) — Oracle Access Manager auth wall, Oracle ADF/JSF stateful framework, SOAP-only API requiring DSP approval. No GitHub prior art. Not worth the effort when ASX covers all listed company filings.
- **ASIC Company Dataset** (`data.gov.au`) — Available as free enrichment (3.4M companies, TSV, CC BY 3.0) but contains company metadata only, not filing documents.

## License

Copyright (c) 2026 Alexander Isaev / Data Alchemy Labs. All rights reserved.

This software is proprietary. See [LICENSE](LICENSE) for details. Commercial use, redistribution, or derivative works require explicit written authorization.
</div>
