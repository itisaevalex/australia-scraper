# CLAUDE.md — Australia Securities Filing Scraper

## Mission

Reverse-engineer and scrape Australia's securities filing systems. Extract filings (annual reports, prospectuses, financial statements, market announcements) from Australian regulatory portals into structured JSON + downloaded documents.

Part of a multi-country financial filings scraper project. Sibling scrapers exist for Canada (SEDAR+, task2/), Mexico (CNBV STIV-2, task1/), China (CNINFO, china-scraper/), India (BSE+NSE+SEBI, india-scraper/).

## Target Portals

### Primary Target: ASX Announcements
- **Public search:** https://www.asx.com.au/markets/trade-our-cash-market/announcements.leg
- **Historical:** https://www.asx.com.au/markets/trade-our-cash-market/historical-announcements
- **Today's:** https://www.asx.com.au/markets/trade-our-cash-market/todays-announcements
- **Search endpoint:** https://www.asx.com.au/asx/v2/statistics/announcements.do
- **Previous day:** https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do
- **Operator:** ASX (Australian Securities Exchange)
- **Access:** Free public search, no login required for viewing announcements
- **Coverage:** All ASX-listed companies (continuous disclosure under Listing Rule 3.1)
- **Tech stack:** .NET backend, specific frontend unknown
- **Prior art:** GitHub repo `pyasx` uses undocumented ASX APIs — check this first

### Secondary Target: ASIC Connect (Regulatory filings)
- **Registry search:** https://connectonline.asic.gov.au/RegistrySearch/faces/landing/SearchRegisters.jspx
- **Portal:** https://asicconnect.asic.gov.au/public/
- **Operator:** ASIC (Australian Securities and Investments Commission)
- **Access:** Free search, some document downloads may cost money
- **Coverage:** All Australian companies (not just listed), prospectuses, financial reports
- **Note:** URL contains `.jspx` — likely Java/JSF backend

### Bulk Data: ASIC Company Dataset
- **URL:** https://data.gov.au/data/dataset/asic-companies
- **Format:** CSV, TSV, JSON, XML
- **Coverage:** 3.4 million companies, updated weekly (Tuesdays)
- **Access:** Completely free, no auth
- **Note:** This is company metadata only (name, ACN, status, dates), not the actual filing documents

### Company Directory
- **URL:** https://www.asx.com.au/markets/trade-our-cash-market/directory
- **Purpose:** List of all ASX-listed companies with ticker codes

## Recommended Approach

1. **Start with ASX announcements** — this is the primary filing disclosure system for listed companies, free public access, and likely has an undocumented JSON API behind the search pages
2. **Check `pyasx` GitHub repo** — may have already mapped the API endpoints
3. **ASIC Connect second** — for prospectuses and regulatory filings if needed
4. **Bulk dataset third** — for company metadata enrichment

## Methodology — Lessons from 4 Previous Scrapers

### What We've Learned So Far

| Country | Site | Bot Protection | HTTP Library | State Management | Difficulty |
|---------|------|---------------|-------------|-----------------|------------|
| Mexico | CNBV | Azure WAF (light) | `requests` | ASP.NET ViewState + DevExpress callbacks | Hard |
| Canada | SEDAR+ | Radware Bot Manager (heavy) | `curl_cffi` | Oracle Catalyst state machine | Very Hard |
| China | CNINFO | **None** | `requests` | Stateless JSON API | Easy |
| India | BSE/NSE/SEBI | Akamai (light on API) | `requests` | Stateless REST APIs | Easy-Medium |

### Phase 1: Reconnaissance (DO THIS FIRST)

1. **Identify the tech stack** — check response headers (`Server`, `X-Powered-By`, cookie names)
2. **Inspect page source** — look for framework patterns:
   - ASP.NET: `__VIEWSTATE`, `__EVENTVALIDATION`, `ScriptManager`
   - Java/JSF: `.jspx` URLs, `javax.faces` parameters (ASIC Connect uses this)
   - React/Vue SPA: API calls in Network tab
   - .NET: ASX likely uses this
3. **Test plain curl/requests** — does it return data or redirect to challenge?
4. **Check robots.txt**
5. **Look for JSON API endpoints** — modern sites often have REST APIs behind the HTML pages

### Phase 2: GitHub Research (BEFORE writing any code)

1. `gh search repos` and `gh search code` for the target site
2. Specifically check `pyasx` repo for ASX API documentation
3. **This saved days on SEDAR+, CNBV, and CNINFO** — prior art existed for all three

### Phase 3: HTTP Library Selection

| Library | When it works | When it fails |
|---------|--------------|---------------|
| `requests` | No TLS fingerprinting (worked for Mexico, China, India) | Radware, aggressive Cloudflare |
| `curl_cffi` | TLS-sensitive WAFs (needed for Canada SEDAR+) | Overkill when requests works |

**Start with `requests`.** Only escalate to `curl_cffi` if you get 403s or challenge redirects.

### Phase 4: Use Playwright as a DEBUGGING TOOL, not the scraper

Capture real browser traffic, then replicate with raw HTTP:
- **Network requests** — exact headers, POST bodies, cookie values
- **XHR/Fetch calls** — reveal the actual API endpoints
- **JavaScript callbacks** — frameworks often transform data before sending

Key discoveries from previous projects:
- **Mexico:** DevExpress silently prepends `c0:` to callback params — invisible without Playwright capture
- **Canada:** Real Chrome CDP cookies enable pure HTTP; headless Playwright cookies were rejected by Radware
- **India:** Recon agents' WebFetch had different header behavior than Python `requests` — always verify with actual library

### Phase 5: Understand the State Machine

**Stateful servers (Mexico, Canada):**
- ViewState/session tokens must be sent with every request
- Pagination may be sequential only
- State invalidation can destroy previous page's URLs

**Stateless APIs (China, India):**
- Random-access pagination (jump to any page)
- URLs are permanent
- No download-before-paginate constraint

**Determine which pattern ASX uses early** — it fundamentally shapes the scraper architecture.

### Phase 6: Download Pattern

**Download-before-paginate** (if state machine detected):
- Download documents from current page BEFORE paginating
- Within a page, parallelize freely (ThreadPoolExecutor)
- Cross-page parallelism does NOT work with stateful servers

**Permanent URLs** (if stateless API):
- Collect all URLs first, then batch download
- Full parallelism possible
- Cache URLs in SQLite for incremental re-downloads

**Enc/token caching** (from Mexico):
- If download URLs use encrypted/session tokens, cache them in SQLite
- Tokens may be deterministic and permanent

### Phase 7: Rate Limiting & Bot Protection

| Protection | Detection | Bypass |
|-----------|-----------|--------|
| TLS fingerprinting | 403 on plain requests | `curl_cffi` with `impersonate="chrome120"` |
| JavaScript challenge | Redirect to challenge page | Real browser cookies via CDP |
| IP reputation | Datacenter IPs blocked | Residential IP |
| Cookie validation | Missing session cookies | Harvest from browser |
| Rate limiting | 429 / connection pool exhaustion | Add delays, limit concurrency |

**From India experience:** Always send a browser User-Agent. It costs nothing and prevents the most common 403 blocks.

### Phase 8: Production Architecture

```
Session init (one-time, <10s)
  → Pure HTTP crawl (requests or curl_cffi)
    → Parse response (JSON preferred, HTML fallback with BeautifulSoup)
      → Download documents (parallel within page or fully parallel if stateless)
        → Cache to SQLite (dedup + tracking)
```

## Output Format

Match the existing project structure:
```
scraper.py              # Main scraper (crawl, monitor, export, stats)
requirements.txt        # Python dependencies
filings_cache.db        # SQLite cache (auto-generated)
documents/              # Downloaded files (auto-generated)
filings.json            # Exported filings (via export command)
_investigation/         # Reverse-engineering artifacts
README.md               # Documentation with full RE journey
```

## Commands to Support

```bash
python scraper.py crawl --max-pages 10 --download
python scraper.py monitor --interval 300 --download
python scraper.py export --output filings.json
python scraper.py stats
```

## Investigation Artifacts

Save ALL reverse-engineering work in `_investigation/`:
- Network captures, decoded responses
- Hypothesis test scripts (`exp_*.py`)
- API endpoint documentation
- Header/cookie analysis

This evidence is invaluable for debugging when things break later.
