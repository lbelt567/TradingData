# TradingData Pipeline v2 -- Work Requirements

---

## Goal

Replace the current file-based pipeline (CSV -> Pickle -> Parquet -> GCS) with a database-first architecture using **BigQuery** as the single source of truth. All scrapers perform **upserts** (only writing changed rows). AI agents query BigQuery directly for 24/7 financial monitoring.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   DATA INGESTION                     │
│                                                      │
│  Real-time (every 5 min):                            │
│    IBKR TWS API ──► Python worker ──► BigQuery MERGE │
│                                                      │
│  Scheduled:                                          │
│    NASDAQ earnings  (daily)  ─┐                      │
│    AlphaVantage earnings (daily) ─┤                  │
│    FINRA ATS/OTC   (weekly)  ─┼──► BigQuery MERGE    │
│    SEC EDGAR       (quarterly)─┤                     │
│    FOMC meetings   (weekly)  ─┘                      │
│                                                      │
├──────────────────────────────────────────────────────┤
│                   STORAGE (GCP)                      │
│                                                      │
│  BigQuery Dataset: trading_data                      │
│    ├── stock_loan         (upserted every 5 min)     │
│    ├── earnings_calendar  (upserted daily)           │
│    ├── nasdaq_earnings    (upserted daily)           │
│    ├── ats_otc            (upserted weekly)          │
│    ├── edgar_filings      (upserted quarterly)       │
│    └── fomc_meetings      (upserted weekly)          │
│                                                      │
│  GCS Bucket (optional cold archive)                  │
│    └── weekly Parquet snapshots                       │
│                                                      │
├──────────────────────────────────────────────────────┤
│                   AI AGENTS                          │
│                                                      │
│  Query BigQuery directly via SQL                     │
│  No file reads, no local storage needed              │
│                                                      │
└──────────────────────────────────────────────────────┘
```

---

## Work Breakdown

### Phase 1: GCP Infrastructure Setup

**1.1 -- BigQuery Dataset and Tables**
- Create a BigQuery dataset called `trading_data` in your GCP project.
- Create 6 tables with defined schemas:

| Table | Primary Key (for MERGE) | Key Columns |
|---|---|---|
| `stock_loan` | `SYM` + `CURRENCY` | SYM, NAME, FEERATE, REBATERATE, AVAILABLE, COUNTRY, TIMESTAMP |
| `earnings_calendar` | `symbol` + `reportDate` | symbol, name, reportDate, fiscalDateEnding, estimate, currency |
| `nasdaq_earnings` | `Symbol` + `Date` | Symbol, Company Name, Date, EPS Forecast, Market Cap, Fiscal Quarter |
| `ats_otc` | `issueSymbolIdentifier` + `initialPublishedDate` | issueSymbolIdentifier, totalWeeklyShareQuantity, totalWeeklyTradeCount, lastUpdateDate |
| `edgar_filings` | `ticker` + `accessionNumber` | ticker, filingType, filedAt, accessionNumber, filing_link |
| `fomc_meetings` | `Year` + `Month` + `Meeting Dates` | Year, Month, Meeting Dates, Summary of Economic Projections |

- Add a `_last_updated` timestamp column to every table for tracking when each row was last modified.

**1.2 -- GCP Service Account**
- Create a GCP service account with roles: `BigQuery Data Editor`, `BigQuery Job User`.
- If using GCS for archival: add `Storage Object Admin` on the archive bucket.
- Generate a JSON key file. This is the only credential the pipeline needs.

**1.3 -- Secrets Management**
- Store the service account JSON key as a GitHub Actions secret (`GCP_SA_KEY`).
- Store any API keys (AlphaVantage, FINRA) as GitHub Actions secrets.
- **Zero hardcoded credentials in source code.** All secrets come from environment variables.

---

### Phase 2: Core Python Package

**2.1 -- Project Structure**

```
TradingDataV2/
├── scrapers/
│   ├── __init__.py
│   ├── base.py                  # BaseScraper class
│   ├── alpha_earnings.py        # AlphaVantage earnings
│   ├── nasdaq_earnings.py       # NASDAQ earnings calendar
│   ├── fomc.py                  # Federal Reserve FOMC
│   ├── edgar.py                 # SEC EDGAR filings
│   ├── finra_ats.py             # FINRA OTC/ATS
│   └── ibkr_short.py           # Interactive Brokers (TWS API)
├── pipeline/
│   ├── __init__.py
│   ├── config.py                # Single config module (env vars)
│   ├── bigquery.py              # BigQuery client (upsert logic)
│   ├── orchestrator.py          # Scrape -> upsert coordination
│   └── archive.py               # Optional GCS Parquet snapshots
├── tests/
│   ├── test_scrapers.py
│   ├── test_bigquery.py
│   └── test_data_quality.py
├── .github/workflows/
│   ├── scheduled_scrapers.yml   # All scheduled scrapers
│   ├── realtime_ibkr.yml        # IBKR high-frequency worker
│   └── tests.yml                # Test suite
├── requirements.txt
├── .env.example
└── README.md
```

**2.2 -- Config Module (`pipeline/config.py`)**
- Single dataclass loading all config from environment variables via `python-dotenv`.
- Required env vars:
  - `GCP_PROJECT_ID`
  - `GCP_SA_KEY` (service account JSON or path to key file)
  - `BQ_DATASET` (default: `trading_data`)
  - `ALPHA_API_KEY`
  - `FINRA_CLIENT_ID`, `FINRA_CLIENT_SECRET`
  - `GCS_ARCHIVE_BUCKET` (optional, for Parquet snapshots)
- No fallbacks to hardcoded values. Missing vars raise clear errors.

**2.3 -- BigQuery Client (`pipeline/bigquery.py`)**
- Wrapper around `google-cloud-bigquery` Python client.
- Core method: `upsert(table, dataframe, key_columns)` that:
  1. Loads the DataFrame into a BigQuery temp/staging table.
  2. Runs a `MERGE` statement joining staging to the target table on the key columns.
  3. Updates rows where non-key columns differ.
  4. Inserts rows that don't exist in the target.
  5. Sets `_last_updated` on every touched row.
- Additional methods:
  - `query(sql)` -- run a query, return DataFrame.
  - `table_info(table)` -- row count, last updated, schema.
  - `export_parquet(table, gcs_path)` -- export table to Parquet on GCS (for archival).

**2.4 -- Base Scraper (`scrapers/base.py`)**
- Abstract base class that every scraper inherits from.
- Interface:
  - `scrape() -> pd.DataFrame` -- fetch data from source, return clean DataFrame.
  - `key_columns() -> list[str]` -- columns that define a unique row (used by MERGE).
  - `table_name() -> str` -- BigQuery target table.
- Built-in:
  - Logging (standardized format).
  - Error handling with retries for HTTP failures.
  - Timestamp injection (`_scraped_at` column).

---

### Phase 3: Scraper Rewrites

Rewrite each scraper to inherit from `BaseScraper`, return a clean DataFrame, and let the orchestrator handle the BigQuery upsert.

**3.1 -- AlphaVantage Earnings (`scrapers/alpha_earnings.py`)**
- Source: AlphaVantage API
- Schedule: Daily
- Key columns: `symbol` + `reportDate`
- Changes from v1: Read API key from env var, return DataFrame instead of writing CSV.

**3.2 -- NASDAQ Earnings (`scrapers/nasdaq_earnings.py`)**
- Source: NASDAQ API (`api.nasdaq.com`)
- Schedule: Daily
- Key columns: `Symbol` + `Date`
- Changes from v1: Minimal -- already the cleanest scraper. Just return DataFrame.

**3.3 -- FOMC Meetings (`scrapers/fomc.py`)**
- Source: Federal Reserve website (HTML scrape)
- Schedule: Weekly (not hourly -- data changes ~8x/year)
- Key columns: `Year` + `Month` + `Meeting Dates`
- Changes from v1: Fix `raise_for_status` bug (missing parentheses), reduce schedule from hourly to weekly, read config from env vars.

**3.4 -- SEC EDGAR Filings (`scrapers/edgar.py`)**
- Source: SEC EDGAR API + `edgar` library
- Schedule: Quarterly (or monthly)
- Key columns: `ticker` + `accessionNumber`
- Changes from v1: Add `edgar` to requirements.txt, remove hardcoded email, add workflow automation (v1 had none).

**3.5 -- FINRA ATS/OTC (`scrapers/finra_ats.py`)**
- Source: FINRA EWS API (OAuth2)
- Schedule: Weekly (Monday after data publish)
- Key columns: `issueSymbolIdentifier` + `initialPublishedDate`
- Changes from v1: Read OAuth credentials from env vars (v1 hardcodes them), return DataFrame.

**3.6 -- IBKR Short Borrow (`scrapers/ibkr_short.py`)**
- Source: **IBKR TWS API** (replaces FTP approach entirely)
- Schedule: Every 5 minutes (real-time worker)
- Key columns: `SYM` + `CURRENCY`
- Changes from v1: **Complete rewrite.** Replace the 5-step FTP pipeline (download -> process -> resample -> compile -> delete) with a direct TWS API connection that streams updates. Each cycle fetches current data, and the orchestrator MERGEs only changed rows into BigQuery.
- Note: Requires IBKR TWS Gateway or IB Gateway running. If TWS API is not feasible, fall back to FTP but upsert into BigQuery instead of building pickle files.

---

### Phase 4: Orchestrator and Automation

**4.1 -- Orchestrator (`pipeline/orchestrator.py`)**
- Accepts a scraper instance, calls `scrape()`, passes result to `bigquery.upsert()`.
- Logs: rows scraped, rows inserted, rows updated, rows unchanged, duration.
- Error handling: if scrape fails, log and exit cleanly (no partial writes).

**4.2 -- GitHub Actions: Scheduled Scrapers (`scheduled_scrapers.yml`)**
- Single workflow using a **matrix strategy** for all scheduled scrapers:

```yaml
strategy:
  matrix:
    include:
      - scraper: alpha_earnings
        cron: '0 4 * * *'        # Daily 4 AM UTC
      - scraper: nasdaq_earnings
        cron: '0 5 * * *'        # Daily 5 AM UTC
      - scraper: fomc
        cron: '0 6 * * 1'        # Weekly Monday 6 AM UTC
      - scraper: finra_ats
        cron: '0 18 * * 1'       # Weekly Monday 6 PM UTC
      - scraper: edgar
        cron: '0 8 1 */3 *'      # Quarterly
```

- Each job: checkout -> install deps -> run `python -m pipeline run <scraper>`.
- Secrets passed as env vars: `GCP_SA_KEY`, `ALPHA_API_KEY`, `FINRA_CLIENT_ID`, `FINRA_CLIENT_SECRET`.

**4.3 -- GitHub Actions: IBKR Real-time Worker (`realtime_ibkr.yml`)**
- Runs on a self-hosted runner or Cloud Run (GitHub Actions free tier won't support continuous 5-min polling).
- Options:
  - **Google Cloud Run Job** with a 5-minute Cloud Scheduler trigger.
  - **Compute Engine micro VM** running a cron loop.
  - **Cloud Functions** triggered by Cloud Scheduler.
- This is the one scraper that can't run on GitHub Actions free tier due to frequency.

**4.4 -- No More Git Commits of Data**
- v1 commits CSV files to the git repo, inflating repo size.
- v2: **Data never touches the git repo.** Scrapers write directly to BigQuery. The repo contains only code.

---

### Phase 5: AI Agent Integration

**5.1 -- Agent Query Interface**
- AI agents authenticate to BigQuery with the same service account.
- Agents run SQL queries directly:

```sql
-- "Any fee rate spikes in the last hour?"
SELECT SYM, NAME, FEERATE, AVAILABLE, _last_updated
FROM trading_data.stock_loan
WHERE _last_updated > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND FEERATE > 50
ORDER BY FEERATE DESC

-- "Earnings announcements tomorrow?"
SELECT Symbol, `Company Name`, `EPS Forecast`, `Market Cap`
FROM trading_data.nasdaq_earnings
WHERE Date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)

-- "FINRA ATS volume changes week over week?"
SELECT issueSymbolIdentifier,
       totalWeeklyShareQuantity,
       LAG(totalWeeklyShareQuantity) OVER (
         PARTITION BY issueSymbolIdentifier ORDER BY initialPublishedDate
       ) AS prev_week,
FROM trading_data.ats_otc
ORDER BY initialPublishedDate DESC
```

**5.2 -- Optional: DuckDB Local Querying**
- DuckDB can query BigQuery directly via its `bigquery` extension if you want local tooling.
- Also useful for joining BigQuery data with local files during development.

---

### Phase 6: Optional Enhancements

**6.1 -- GCS Parquet Archive (`pipeline/archive.py`)**
- Weekly export of each BigQuery table to Parquet on GCS.
- Purpose: cold backup, cost-free long-term storage, portability.
- Not required for the pipeline to function.

**6.2 -- Data Quality Tests (`tests/test_data_quality.py`)**
- After each scrape, validate:
  - Row count is non-zero.
  - Key columns have no nulls.
  - Numeric values are within expected ranges (market cap, EPS, fee rates).
  - No duplicate keys.
- Run as part of the orchestrator (fail loudly if data is bad, don't upsert garbage).

**6.3 -- Pipeline Monitoring**
- Log every scrape run to a `pipeline_runs` table in BigQuery:
  - scraper name, status, rows_scraped, rows_inserted, rows_updated, duration, timestamp.
- AI agents can monitor pipeline health alongside financial data.

---

## What Gets Deleted from v1

| v1 File/Concept | v2 Status |
|---|---|
| `data_upload/` directory (all CSVs, pickles) | **Eliminated** -- data goes straight to BigQuery |
| `scrape_logic/ftp_download.py` | **Deleted** -- was already redundant |
| `data_upload/stock_loan_data.csv` | **Deleted** -- was already unused |
| `configuration.py` | **Replaced** by single `pipeline/config.py` |
| `ml_pipeline/convert.py` (Parquet conversion) | **Eliminated** -- no file conversion needed |
| `ml_pipeline/storage.py` (GCS upload) | **Eliminated** -- BigQuery is the store |
| `ml_pipeline/metadata.py` (Supabase tracking) | **Replaced** by BigQuery `pipeline_runs` table |
| `ml_pipeline/query.py` (DuckDB engine) | **Optional** -- agents query BigQuery directly |
| `ml_pipeline/dataset.py` (ML loader) | **Keep if doing ML** -- adapt to read from BigQuery |
| Pickle files (.pkl, .pkl.gz) | **Eliminated** -- no intermediate formats |
| Git-committed data files | **Eliminated** -- repo is code only |
| Supabase dependency | **Eliminated** -- BigQuery handles metadata too |
| 6 separate workflow files | **Consolidated** into 2-3 workflows |

---

## Dependencies (requirements.txt)

```
# Core
pandas
requests
beautifulsoup4

# GCP
google-cloud-bigquery>=3.14.0
google-cloud-storage>=2.14.0    # only if using GCS archive

# Scrapers
holidays                         # FINRA business day logic
edgar                            # SEC EDGAR filings

# Optional
duckdb>=0.9.0                    # local querying / development
python-dotenv>=1.0.0             # local .env file support
pytest>=7.0.0                    # testing
```

Removed from v1: `pyarrow`, `boto3`, `supabase`, `numpy` (add back if doing ML).

---

## Estimated GCP Cost

| Service | Usage | Monthly Cost |
|---|---|---|
| BigQuery Storage | ~5-10 GB | $0.10 - $0.20 |
| BigQuery Queries | Well under 1 TB/month | Free (1 TB free tier) |
| GCS Archive (optional) | ~5-10 GB | $0.10 - $0.20 |
| Cloud Scheduler (IBKR) | 288 invocations/day | Free (3 jobs free tier) |
| Cloud Run (IBKR worker) | ~1 min execution, 288x/day | ~$1-3 |
| **Total** | | **~$1-5/month** |

---

## Order of Execution

1. **Phase 1** -- Set up GCP project, BigQuery dataset, tables, service account.
2. **Phase 2** -- Build `pipeline/config.py`, `pipeline/bigquery.py`, `scrapers/base.py`.
3. **Phase 3** -- Rewrite scrapers one at a time (start with the simplest: NASDAQ earnings).
4. **Phase 4** -- Build orchestrator, set up GitHub Actions, deploy IBKR worker.
5. **Phase 5** -- Connect AI agents to BigQuery.
6. **Phase 6** -- Add archival, data quality tests, monitoring.

Each phase is independently deployable. You can run v1 scrapers alongside v2 scrapers during migration.
