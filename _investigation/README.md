# _investigation/

This directory stores all reverse-engineering artifacts produced during reconnaissance of the Australian securities filing systems. Keeping this evidence here makes it possible to diagnose regressions when ASX changes its endpoints and to onboard new contributors without repeating the discovery work.

## Contents

### Documentation

| File | Purpose |
|------|---------|
| `API_ENDPOINTS.md` | Complete reference for every confirmed API endpoint, parameter, response format, HTML parsing structure, and scraper architecture decision. Read this first. |
| `README.md` | This file. |

### Experiment Scripts (`exp_*.py`)

Each script is a standalone investigation that was run once to answer a specific question. They are preserved as executable evidence — run them again if endpoint behaviour needs to be reverified.

| Script | What it tested |
|--------|---------------|
| `exp_asx_endpoints.py` | Probed all candidate endpoints with and without a User-Agent to confirm access model, response format, and server headers. Established that plain `requests` works and that Imperva CDN passes through without challenge. |
| `exp_asx_pagination.py` | Confirmed correct `timeframe`/`period` parameter values, showed that `by=companyName` returns a company-chooser (not announcements), proved no server-side type filtering exists, ran a 40-request rate-limit burst test (all 200 OK), and verified stateless behaviour across independent sessions. |
| `exp_asx_parse_announcements.py` | Mapped the complete HTML structure of both `announcements.do` and `prevBusDayAnns.do` responses, including the `<announcement_data>` container, column layouts, date/time extraction, price sensitivity detection, and `idsId` parsing. |
| `exp_asx_downloads.py` | Verified the two-step PDF download flow (displayAnnouncement.do → terms page → pdfURL → actual PDF), confirmed URL permanence across independent sessions, tested parallel downloads with `ThreadPoolExecutor`, and characterised both company directory CSVs (ASXListedCompanies.csv and MarkitDigital). |
| `exp_asx_pdf_download.py` | Focused retest of the PDF download flow after discovering the terms-of-use interstitial page. Confirmed `<input name="pdfURL">` extraction pattern, parallel download behaviour, and `None` return for invalid `idsId` values. |

## How to Re-run an Experiment

All scripts are self-contained and require only the packages in `requirements.txt`:

```bash
cd /path/to/australia-scraper
pip install -r requirements.txt
python _investigation/exp_asx_endpoints.py
```

They make live HTTP requests to ASX servers and print structured output to stdout. No files are written to disk (except `exp_asx_downloads.py`, which briefly writes a test PDF to `/tmp` and deletes it on exit).
