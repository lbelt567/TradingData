# TradingData Pipeline - Full Codebase Audit

**Date**: 2026-02-04
**Purpose**: Identify every file's role, what is actively used, what is redundant/unused, and what is essential for rebuilding an efficient version of this pipeline from scratch.

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Complete File Inventory](#2-complete-file-inventory)
3. [Data Flow: How the Pipeline Actually Works](#3-data-flow-how-the-pipeline-actually-works)
4. [Detailed File-by-File Analysis](#4-detailed-file-by-file-analysis)
   - [Scraper Scripts](#41-scraper-scripts-scrape_logic)
   - [ML Pipeline Module](#42-ml-pipeline-module-ml_pipeline)
   - [Configuration Files](#43-configuration-files)
   - [Test Files](#44-test-files)
   - [GitHub Actions Workflows](#45-github-actions-workflows)
   - [Data Files](#46-data-files-data_upload)
   - [Documentation & Repo Files](#47-documentation--repo-files)
5. [Dependency / Import Map](#5-dependency--import-map)
6. [Redundant and Unused Files](#6-redundant-and-unused-files)
7. [Security Issues Found](#7-security-issues-found)
8. [Files Required for an Efficient Rebuild](#8-files-required-for-an-efficient-rebuild)
9. [Architectural Recommendations for v2](#9-architectural-recommendations-for-v2)

---

## 1. Pipeline Overview

This repo is a **financial data scraping and ML data pipeline** with two major subsystems:

### Subsystem A: Data Scrapers (6 data sources)
Scheduled via GitHub Actions, these scripts pull raw financial data from external APIs/FTP servers and save CSV/pickle files into `data_upload/`.

### Subsystem B: ML Pipeline (post-processing)
A Python package (`ml_pipeline/`) that converts the raw CSV/pickle files into Parquet, uploads them to cloud storage (GCS or S3), tracks metadata in Supabase (PostgreSQL), and exposes a DuckDB-powered SQL query engine for analytics and ML dataset loading.

### End-to-End Data Flow

```
External APIs / FTP Servers
    |
    v
[6 Scraper Scripts] -- GitHub Actions (cron) --> Raw CSV/pickle in data_upload/
    |
    v
[ParquetConverter] -- convert_all() --> Local .parquet files in data_upload/parquet/
    |
    v
[CloudStorage] -- upload_file() --> GCS or S3 bucket
    |
    v
[MetadataTracker] -- log to Supabase --> pipeline_runs, dataset_files, ticker_index tables
    |
    v
[QueryEngine / TradingDataset] -- DuckDB SQL on Parquet --> pandas/numpy/PyTorch output
```

---

## 2. Complete File Inventory

```
TradingData/
├── .github/
│   └── workflows/
│       ├── main.yml                    # FOMC scraper schedule (hourly)
│       ├── earnings_scrape.yml         # AlphaVantage earnings schedule (daily 4 AM UTC)
│       ├── nasdaq_schedule.yml         # NASDAQ earnings schedule (daily 5 AM UTC)
│       ├── ats_schedule.yml            # FINRA OTC schedule (weekly Monday 6 PM UTC)
│       ├── ml_pipeline.yml             # ML pipeline schedule (daily 10 AM UTC + Monday 7 PM UTC)
│       └── unit_tests.yml              # Data validation (daily 9 AM UTC)
│
├── scrape_logic/
│   ├── earnings_scrape.py              # AlphaVantage earnings calendar
│   ├── nasdaq_earnings_calendar.py     # NASDAQ earnings calendar
│   ├── fomc_script.py                  # Federal Reserve FOMC meetings
│   ├── edgar_scraper.py                # SEC EDGAR filings (10-K, 10-Q, 8-K)
│   ├── ats_scraper.py                  # FINRA OTC/ATS market data
│   ├── scrape_ibkr_short_borrow.py     # Interactive Brokers short borrow (5-step pipeline)
│   └── ftp_download.py                 # REDUNDANT: standalone FTP downloader
│
├── ml_pipeline/
│   ├── __init__.py                     # Package exports
│   ├── __main__.py                     # `python -m ml_pipeline` entry point
│   ├── cli.py                          # CLI argument parser (convert, run, query, status, etc.)
│   ├── config.py                       # PipelineConfig dataclass (env vars)
│   ├── convert.py                      # ParquetConverter (CSV/pickle -> Parquet)
│   ├── storage.py                      # CloudStorage (GCS/S3 upload/download)
│   ├── metadata.py                     # MetadataTracker (Supabase logging)
│   ├── query.py                        # QueryEngine (DuckDB SQL on Parquet)
│   ├── dataset.py                      # TradingDataset (ML-ready data loader)
│   └── pipeline.py                     # MLPipeline orchestrator (convert -> upload -> track)
│
├── tests/
│   └── test_scraper.py                 # pytest integration tests for scrapers
│
├── data_upload/                        # Output data directory
│   ├── earnings_calendar.csv           # AlphaVantage output
│   ├── fomc_meeting_times.csv          # FOMC output
│   ├── stock_loan_data.csv             # UNUSED: legacy CSV, not consumed by anything
│   ├── nasdaq_earnings_upload/         # Daily NASDAQ CSVs
│   ├── ats_upload/                     # Weekly FINRA OTC CSVs
│   └── financial_filings/              # SEC EDGAR CSVs (10-K, 10-Q, 8-K)
│
├── configuration.py                    # Global config (DATA_FOLDER, API keys, URLs)
├── data_unit_test.py                   # unittest data validation for NASDAQ earnings
├── requirements.txt                    # Python dependencies
├── schema.sql                          # Supabase PostgreSQL schema
├── .env.example                        # Environment variable template
├── .gitignore                          # Git ignore rules
└── README.md                           # Project documentation (outdated)
```

---

## 3. Data Flow: How the Pipeline Actually Works

### Phase 1: Scraping (GitHub Actions cron jobs)

| Schedule | Workflow File | Script Executed | Output Location | Data Source |
|---|---|---|---|---|
| Hourly | `main.yml` | `scrape_logic/fomc_script.py` | `data_upload/fomc_meeting_times.csv` | Federal Reserve website (HTML scrape) |
| Daily 4AM UTC | `earnings_scrape.yml` | `scrape_logic/earnings_scrape.py` | `data_upload/earnings_calendar.csv` | AlphaVantage API |
| Daily 5AM UTC | `nasdaq_schedule.yml` | `scrape_logic/nasdaq_earnings_calendar.py` | `data_upload/nasdaq_earnings_upload/nasdaq_earnings_YYYY-MM-DD.csv` | NASDAQ API |
| Monday 6PM UTC | `ats_schedule.yml` | `scrape_logic/ats_scraper.py` | `data_upload/ats_upload/filtered_weekly_otc_data_YYYY-MM-DD.csv` | FINRA API (OAuth2) |
| **No workflow** | N/A | `scrape_logic/edgar_scraper.py` | `data_upload/financial_filings/` | SEC EDGAR |
| **No workflow** | N/A | `scrape_logic/scrape_ibkr_short_borrow.py` | `data_upload/compiled/`, `data_upload/resampled/` | Interactive Brokers FTP |

**Key observations**:
- `edgar_scraper.py` has **no GitHub Actions workflow** -- it must be run manually.
- `scrape_ibkr_short_borrow.py` has **no GitHub Actions workflow** -- it must be run manually. This is the most complex scraper (5-step pipeline: download -> process -> resample -> compile -> delete).
- `ftp_download.py` is a **redundant simplified copy** of the download step in `scrape_ibkr_short_borrow.py`.

### Phase 2: Conversion & Upload (ML Pipeline)

Triggered by `ml_pipeline.yml` (daily 10AM UTC + Monday 7PM UTC):

1. **Convert**: `ParquetConverter.convert_all()` reads raw files from `data_upload/` and writes `.parquet` files to `data_upload/parquet/`:
   - `master.pkl.gz` -> `stock_loan.parquet` (requires IBKR pipeline to have run first -- but there is no workflow for it)
   - `earnings_calendar.csv` -> `earnings.parquet`
   - `nasdaq_earnings_upload/*.csv` -> `nasdaq_earnings.parquet` (merges all daily files)
   - `fomc_meeting_times.csv` -> `fomc.parquet`
   - `ats_upload/*.csv` -> `ats_otc.parquet` (merges all weekly files)
   - `financial_filings/*.csv` -> `edgar_filings.parquet` (merges all SEC filings)

2. **Upload**: `CloudStorage.upload_file()` pushes each `.parquet` file to GCS or S3 under the `trading_data/` prefix.

3. **Track**: `MetadataTracker` logs the pipeline run, file details, and ticker index to Supabase.

### Phase 3: Querying & ML Consumption

Not automated -- used interactively or by downstream code:
- `QueryEngine`: DuckDB SQL interface over local or cloud Parquet files
- `TradingDataset`: ML-ready data loader with feature engineering helpers (log returns, z-scores, etc.)

---

## 4. Detailed File-by-File Analysis

### 4.1 Scraper Scripts (`scrape_logic/`)

#### `earnings_scrape.py` -- ACTIVE, USED
- **What it does**: Fetches 3-month earnings calendar from AlphaVantage API, saves as CSV.
- **Triggered by**: `earnings_scrape.yml` (daily 4AM UTC)
- **Imports**: `requests`, `csv`, `os`, `configuration` (parent dir)
- **Output**: `data_upload/earnings_calendar.csv`
- **External dependency**: AlphaVantage API (requires API key)
- **Issue**: Uses `sys.path.append` hack to import `configuration.py` from parent directory. API key is hardcoded in `configuration.py`.
- **Consumed by**: `ParquetConverter.convert_earnings()`

#### `nasdaq_earnings_calendar.py` -- ACTIVE, USED
- **What it does**: Scrapes NASDAQ earnings calendar API for today's date, saves daily CSV.
- **Triggered by**: `nasdaq_schedule.yml` (daily 5AM UTC)
- **Imports**: `requests`, `datetime`, `csv`, `os` (no local imports)
- **Output**: `data_upload/nasdaq_earnings_upload/nasdaq_earnings_YYYY-MM-DD.csv`
- **External dependency**: NASDAQ API (no authentication needed, uses browser-like User-Agent)
- **Consumed by**: `ParquetConverter.convert_nasdaq_earnings()`

#### `fomc_script.py` -- ACTIVE, USED
- **What it does**: Scrapes Federal Reserve FOMC calendar page (HTML), parses meeting dates and metadata.
- **Triggered by**: `main.yml` (hourly)
- **Imports**: `requests`, `beautifulsoup4`, `pandas`, `configuration` (parent dir)
- **Output**: `data_upload/fomc_meeting_times.csv`
- **External dependency**: Federal Reserve website (HTML scrape)
- **Issue**: Uses `sys.path.append` hack. Running hourly is excessive for data that changes maybe 8 times a year. Also `response.raise_for_status` is called without parentheses (line 14) -- it does nothing.
- **Consumed by**: `ParquetConverter.convert_fomc()`

#### `edgar_scraper.py` -- ACTIVE but NO AUTOMATION
- **What it does**: Fetches SEC EDGAR filings (10-K, 10-Q, 8-K) using the `edgar` Python library, maps CIK to tickers, generates filing links.
- **Triggered by**: **Nothing** -- no GitHub Actions workflow exists. Must be run manually.
- **Imports**: `requests`, `json`, `os`, `datetime`, `pandas`, `edgar` (third-party)
- **Output**: `data_upload/financial_filings/10-K_filings_YYYY_QN.csv`, etc.
- **External dependency**: SEC EDGAR API, `edgar` Python library
- **Issue**: The `edgar` library is not listed in `requirements.txt`. Personal email is hardcoded as User-Agent.
- **Consumed by**: `ParquetConverter.convert_edgar()`

#### `ats_scraper.py` -- ACTIVE, USED
- **What it does**: Fetches FINRA OTC weekly summary data via OAuth2-authenticated API, paginates through all results.
- **Triggered by**: `ats_schedule.yml` (Monday 6PM UTC)
- **Imports**: `datetime`, `holidays`, `requests`, `base64`, `json`, `pandas`, `os`
- **Output**: `data_upload/ats_upload/filtered_weekly_otc_data_YYYY-MM-DD.csv`
- **External dependency**: FINRA EWS API (OAuth2)
- **Critical issue**: Client ID and Client Secret are **hardcoded in the source file** (lines 12-13). The workflow passes secrets via env vars, but the script doesn't read them -- it uses the hardcoded values.
- **Consumed by**: `ParquetConverter.convert_ats()`

#### `scrape_ibkr_short_borrow.py` -- ACTIVE but NO AUTOMATION
- **What it does**: 5-step pipeline for Interactive Brokers short stock borrow data:
  1. `download`: FTP recursive download of .txt files
  2. `process`: Parse pipe-delimited files, track timestamp changes
  3. `resample`: Deduplicate, compress to .pkl.gz
  4. `compile`: Merge all resampled files into `master.pkl.gz`
  5. `delete`: Archive old snapshots
- **Triggered by**: **Nothing** -- no GitHub Actions workflow. Must be run manually with `--action` argument.
- **Imports**: `os`, `re`, `argparse`, `ftplib`, `tarfile`, `shutil`, `pandas`, `pathlib`, `datetime`, `logging`
- **Output**: `data_upload/compiled/`, `data_upload/resampled/`, `master.pkl.gz`
- **External dependency**: Interactive Brokers FTP server (`ftp2.interactivebrokers.com`)
- **Consumed by**: `ParquetConverter.convert_stock_loan()` reads `master.pkl.gz`
- **Issue**: The `stock_loan` conversion in the ML pipeline depends on `master.pkl.gz` existing, but there is no workflow to produce it. This means the `stock_loan.parquet` conversion always returns `None` in CI unless the file was manually placed there.

#### `ftp_download.py` -- REDUNDANT, UNUSED
- **What it does**: Simplified standalone FTP downloader for Interactive Brokers. Does the same thing as `scrape_ibkr_short_borrow.py --action download` but with less functionality (no recursive scanning, less error handling).
- **Triggered by**: Nothing.
- **Not imported by any other file.**
- **Not referenced in any workflow.**
- **Verdict**: This file is 100% redundant. Its functionality is a subset of `scrape_ibkr_short_borrow.py`'s download action.

---

### 4.2 ML Pipeline Module (`ml_pipeline/`)

#### `__init__.py` -- USED (package definition)
- Exports all 7 public classes. Required for `python -m ml_pipeline` to work.
- Imports from: `config`, `convert`, `storage`, `metadata`, `query`, `dataset`, `pipeline`

#### `__main__.py` -- USED (entry point)
- Two lines: imports and calls `main()` from `cli.py`.
- Required for `python -m ml_pipeline` command.

#### `cli.py` -- USED (CLI interface)
- Parses CLI arguments: `convert`, `run`, `query`, `status`, `top-shorts`, `ticker`.
- Used directly by `ml_pipeline.yml` workflow (`python -m ml_pipeline run`).
- Imports from: `ml_pipeline.pipeline`, `ml_pipeline.query`

#### `config.py` -- USED (configuration hub)
- `PipelineConfig` dataclass: loads all settings from environment variables via `python-dotenv`.
- Covers: paths, GCS/S3 credentials, Supabase, DuckDB settings, cloud prefix.
- **Every other ml_pipeline module imports this.**

#### `convert.py` -- USED (core conversion logic)
- `ParquetConverter` class: 6 converter methods (one per data source) + `convert_all()`.
- Reads raw files from `data_upload/`, writes `.parquet` to `data_upload/parquet/`.
- Imports: `pandas`, `pyarrow`, `ml_pipeline.config`

#### `storage.py` -- USED (cloud upload/download)
- `CloudStorage` class: GCS or S3 abstraction.
- Methods: `upload_file()`, `upload_bytes()`, `download_bytes()`, `list_keys()`, `exists()`.
- Imports: `google.cloud.storage` (lazy), `boto3` (lazy), `ml_pipeline.config`

#### `metadata.py` -- USED (Supabase tracking)
- `MetadataTracker` class: logs pipeline runs, file uploads, ticker index to Supabase.
- Gracefully degrades when Supabase credentials are not set.
- Imports: `supabase` (lazy), `ml_pipeline.config`

#### `query.py` -- USED (DuckDB query engine)
- `QueryEngine` class: configures DuckDB with httpfs for cloud reads, provides SQL interface.
- Pre-built queries: `top_short_borrow()`, `ticker_history()`, `availability_changes()`.
- Imports: `duckdb`, `pandas`, `ml_pipeline.config`

#### `dataset.py` -- USED but NOT CRITICAL for core pipeline
- `TradingDataset` class: ML-ready data loader.
- Feature engineering: `add_log_returns()`, `add_z_score()`, `add_pct_rank()`, `add_lag()`, `add_diff()`.
- Output formats: pandas, numpy, PyTorch.
- Train/test splits: `time_split()`, `ticker_split()`.
- This is a **consumer** of the pipeline output, not part of the core scrape/convert/upload flow.
- **Not used by any automated workflow.** Only useful for interactive ML work.

#### `pipeline.py` -- USED (orchestrator)
- `MLPipeline` class: `run()` method ties convert -> upload -> metadata into one call.
- Used by `cli.py` for `run`, `convert`, and `status` commands.
- Imports: `ml_pipeline.config`, `ml_pipeline.convert`, `ml_pipeline.storage`, `ml_pipeline.metadata`

---

### 4.3 Configuration Files

#### `configuration.py` -- USED (by scrapers only)
- Provides: `DATA_FOLDER`, `FOMC_URL`, `FOMC_FILENAME`, `ALPHA_API_KEY`, `ALPHA_URL`, `ALPHA_FILENAME`
- **Only imported by**: `earnings_scrape.py` and `fomc_script.py`
- **Not imported by the ml_pipeline module** (which uses its own `config.py` with env vars)
- **Issue**: Contains a hardcoded API key in plain text (line 9).

#### `.env.example` -- REFERENCE ONLY
- Template showing all environment variables needed by the ml_pipeline module.
- Not executed by anything.

#### `requirements.txt` -- USED (by all workflows)
- Lists all Python dependencies.
- **Missing**: The `edgar` library used by `edgar_scraper.py` is not listed.

#### `schema.sql` -- REFERENCE ONLY
- SQL DDL for creating Supabase tables (`pipeline_runs`, `dataset_files`, `ticker_index`).
- Must be run manually in Supabase SQL Editor. Not automated.

#### `.gitignore` -- USED (repo config)
- Ignores: `.env`, `__pycache__/`, `data_upload/parquet/`, `.DS_Store`, `gcp_key.json`

#### `README.md` -- OUTDATED
- Describes the original simple scraper framework. Does not mention the ml_pipeline module, cloud storage, Supabase, DuckDB, or most data sources.
- References `.github/workflows/main.yaml` (wrong extension -- actual file is `main.yml`).

---

### 4.4 Test Files

#### `data_unit_test.py` -- USED (limited scope)
- unittest-based validation of NASDAQ earnings CSV data quality.
- Checks Market Cap and EPS Forecast ranges.
- **Triggered by**: `unit_tests.yml` (daily 9AM UTC)
- **Scope**: Only validates NASDAQ earnings data. Does not test any other data source.

#### `tests/test_scraper.py` -- PARTIALLY USED
- pytest-based integration tests.
- Tests: CSV existence, NASDAQ save logic, API error handling, earnings_scrape.py execution, API response structure, data loading.
- **Not triggered by any workflow.** The `unit_tests.yml` workflow runs `data_unit_test.py`, not pytest.
- **References non-existent file**: `data_upload/stock_loan_data.csv` is checked in `test_csv_files_exist` but the ml_pipeline uses `master.pkl.gz`, not this CSV.
- **Verdict**: Partially useful but not automated and partially broken.

---

### 4.5 GitHub Actions Workflows

| Workflow | Schedule | Script | Status |
|---|---|---|---|
| `main.yml` | Hourly | `fomc_script.py` | **Wasteful** -- FOMC data changes ~8x/year. Hourly is excessive. |
| `earnings_scrape.yml` | Daily 4AM UTC | `earnings_scrape.py` | Active |
| `nasdaq_schedule.yml` | Daily 5AM UTC | `nasdaq_earnings_calendar.py` | Active |
| `ats_schedule.yml` | Monday 6PM UTC | `ats_scraper.py` | Active |
| `ml_pipeline.yml` | Daily 10AM + Monday 7PM UTC | `python -m ml_pipeline run` | Active |
| `unit_tests.yml` | Daily 9AM UTC | `data_unit_test.py` | Active (limited scope) |

**Missing workflows**:
- `edgar_scraper.py` -- no automation
- `scrape_ibkr_short_borrow.py` -- no automation (this is arguably the most important data source)

**Issues**:
- `main.yml` and `earnings_scrape.yml` use `git push --force`, which can overwrite concurrent changes.
- Workflows use different Python versions (3.10 vs 3.12) and different setup steps inconsistently.
- Some workflows redundantly uninstall/reinstall numpy/pandas before installing requirements.txt.

---

### 4.6 Data Files (`data_upload/`)

| File/Directory | Source | Consumed By | Status |
|---|---|---|---|
| `earnings_calendar.csv` | `earnings_scrape.py` | `ParquetConverter.convert_earnings()` | Active |
| `fomc_meeting_times.csv` | `fomc_script.py` | `ParquetConverter.convert_fomc()` | Active |
| `stock_loan_data.csv` | Unknown origin | **Nothing** | **UNUSED** -- referenced in `test_scraper.py` but not consumed by any converter. The ml_pipeline expects `master.pkl.gz`, not this file. |
| `nasdaq_earnings_upload/` | `nasdaq_earnings_calendar.py` | `ParquetConverter.convert_nasdaq_earnings()` | Active |
| `ats_upload/` | `ats_scraper.py` | `ParquetConverter.convert_ats()` | Active |
| `financial_filings/` | `edgar_scraper.py` | `ParquetConverter.convert_edgar()` | Active |

---

### 4.7 Documentation & Repo Files

| File | Status |
|---|---|
| `README.md` | **Outdated** -- does not reflect current architecture |
| `schema.sql` | Reference file, must be run manually in Supabase |
| `.env.example` | Useful template |
| `.gitignore` | Active |

---

## 5. Dependency / Import Map

### Who imports what (within the repo):

```
configuration.py
  <- scrape_logic/earnings_scrape.py  (DATA_FOLDER, ALPHA_API_KEY, ALPHA_URL, ALPHA_FILENAME)
  <- scrape_logic/fomc_script.py      (DATA_FOLDER, FOMC_URL, FOMC_FILENAME)

ml_pipeline/config.py
  <- ml_pipeline/convert.py
  <- ml_pipeline/storage.py
  <- ml_pipeline/metadata.py
  <- ml_pipeline/query.py
  <- ml_pipeline/dataset.py
  <- ml_pipeline/pipeline.py

ml_pipeline/convert.py
  <- ml_pipeline/pipeline.py

ml_pipeline/storage.py
  <- ml_pipeline/pipeline.py

ml_pipeline/metadata.py
  <- ml_pipeline/pipeline.py

ml_pipeline/query.py
  <- ml_pipeline/cli.py
  <- ml_pipeline/dataset.py

ml_pipeline/pipeline.py
  <- ml_pipeline/cli.py

ml_pipeline/cli.py
  <- ml_pipeline/__main__.py
```

### Files with ZERO inbound imports (not imported by anything):
- `scrape_logic/ftp_download.py` -- standalone, redundant
- `scrape_logic/nasdaq_earnings_calendar.py` -- standalone entry point (via workflow)
- `scrape_logic/ats_scraper.py` -- standalone entry point (via workflow)
- `scrape_logic/edgar_scraper.py` -- standalone entry point (no workflow)
- `scrape_logic/scrape_ibkr_short_borrow.py` -- standalone entry point (no workflow)
- `data_unit_test.py` -- standalone test (via workflow)
- `tests/test_scraper.py` -- standalone test (NOT triggered by any workflow)

### External Python Dependencies:

| Package | Used By | Listed in requirements.txt? |
|---|---|---|
| `requests` | All scrapers | Yes |
| `beautifulsoup4` | `fomc_script.py` | Yes |
| `pandas` | Everything | Yes |
| `numpy` | `dataset.py`, various | Yes |
| `holidays` | `ats_scraper.py` | Yes |
| `pyarrow` | `convert.py` | Yes |
| `duckdb` | `query.py` | Yes |
| `google-cloud-storage` | `storage.py` | Yes |
| `boto3` | `storage.py` | Yes |
| `supabase` | `metadata.py` | Yes |
| `python-dotenv` | `config.py` | Yes |
| `pytest` | `tests/test_scraper.py` | Yes |
| `edgar` | `edgar_scraper.py` | **NO -- MISSING** |
| `google-auth` | `query.py` (for GCS DuckDB auth) | **NO -- MISSING** |

---

## 6. Redundant and Unused Files

### 100% Redundant (can be deleted with zero impact):

| File | Reason |
|---|---|
| `scrape_logic/ftp_download.py` | Duplicate of `scrape_ibkr_short_borrow.py --action download`. Less functional subset. Not imported or referenced anywhere. |
| `data_upload/stock_loan_data.csv` | Legacy CSV file. Not consumed by any converter (the ml_pipeline expects `master.pkl.gz`). Only referenced in `test_scraper.py` line 14, which checks if it exists. |

### Effectively Unused (present but not integrated):

| File | Reason |
|---|---|
| `tests/test_scraper.py` | Not triggered by any workflow. The `unit_tests.yml` runs `data_unit_test.py` instead. Some tests reference non-existent data. Would need fixes to be useful. |
| `README.md` | Describes a different, simpler version of the project. Does not mention ml_pipeline, cloud storage, Supabase, DuckDB, or 4 of the 6 data sources. |

### Partially Redundant:

| File | Reason |
|---|---|
| `configuration.py` | Only used by 2 scrapers (`earnings_scrape.py`, `fomc_script.py`). The ml_pipeline module has its own `config.py` using env vars, creating two competing config systems. In a rebuild, consolidate into one. |
| `data_unit_test.py` | Only validates NASDAQ earnings. In a rebuild, expand to cover all data sources or merge with `tests/test_scraper.py`. |

---

## 7. Security Issues Found

### Critical: Hardcoded Credentials

1. **`configuration.py:9`** -- AlphaVantage API key hardcoded:
   ```python
   ALPHA_API_KEY = 'C7DEHKVVQH4SDSTX'
   ```

2. **`scrape_logic/ats_scraper.py:12-13`** -- FINRA OAuth2 credentials hardcoded:
   ```python
   CLIENT_ID = "f8fcc6142ec9417ba441"
   CLIENT_SECRET = "Harm0708#Harm0708#"
   ```
   The workflow defines `FINRA_CLIENT_ID`/`FINRA_CLIENT_SECRET` as secrets, but the script ignores them and uses the hardcoded values.

3. **`scrape_logic/edgar_scraper.py:17`** -- Personal email hardcoded as SEC User-Agent:
   ```python
   headers = {"User-Agent": "Nico Monsalve (nmonsalve@g.hmc.edu)"}
   ```

### Medium: Dangerous Git Operations

4. **`main.yml` and `earnings_scrape.yml`** use `git push --force`, which can overwrite concurrent changes from other workflows.

### Low: Bug in fomc_script.py

5. **`scrape_logic/fomc_script.py:14`** -- `response.raise_for_status` is referenced but **not called** (missing parentheses). HTTP errors are silently ignored.

---

## 8. Files Required for an Efficient Rebuild

### Tier 1: ESSENTIAL -- Core Pipeline Logic (must rebuild/keep)

These files contain the actual business logic that makes the pipeline work:

| File | Why It's Essential |
|---|---|
| `scrape_logic/earnings_scrape.py` | Scraper for AlphaVantage earnings data. Needs refactoring (use env vars for API key) but the logic is needed. |
| `scrape_logic/nasdaq_earnings_calendar.py` | Scraper for NASDAQ earnings. Cleanest scraper in the repo -- good reference for rebuild. |
| `scrape_logic/fomc_script.py` | Scraper for FOMC meetings. HTML parsing logic is the valuable part. |
| `scrape_logic/edgar_scraper.py` | Scraper for SEC filings. Needs a workflow added. |
| `scrape_logic/ats_scraper.py` | Scraper for FINRA OTC data. Fix credential handling. |
| `scrape_logic/scrape_ibkr_short_borrow.py` | Most complex scraper (5-step pipeline). Needs a workflow added. The FTP download, parsing, timestamp tracking, deduplication, and compilation logic is all critical. |
| `ml_pipeline/config.py` | Centralized env-var-based configuration. Good pattern to keep. |
| `ml_pipeline/convert.py` | Parquet conversion logic for all 6 sources. Schema definitions and sanitization logic are valuable. |
| `ml_pipeline/storage.py` | Cloud storage abstraction (GCS/S3). Clean, reusable. |
| `ml_pipeline/pipeline.py` | Orchestrator that ties convert -> upload -> track. |
| `ml_pipeline/cli.py` | CLI interface. Useful pattern for v2. |
| `ml_pipeline/__main__.py` | Entry point for `python -m ml_pipeline`. |
| `ml_pipeline/__init__.py` | Package definition. |
| `requirements.txt` | Dependency list (needs `edgar` and `google-auth` added). |
| `.env.example` | Environment variable template. |
| `schema.sql` | Database schema for Supabase -- needed to set up metadata tracking. |
| `.github/workflows/earnings_scrape.yml` | Reference for scraper workflow automation. |
| `.github/workflows/nasdaq_schedule.yml` | Reference for scraper workflow automation. |
| `.github/workflows/ats_schedule.yml` | Reference for scraper workflow automation. |
| `.github/workflows/ml_pipeline.yml` | Reference for pipeline workflow automation. |

### Tier 2: USEFUL -- Supporting but replaceable

| File | Why It's Useful |
|---|---|
| `ml_pipeline/metadata.py` | Supabase metadata tracking. Useful if you want pipeline observability, but not strictly required for data to flow. |
| `ml_pipeline/query.py` | DuckDB query engine. Very useful for analytics but could be replaced with direct DuckDB calls. |
| `ml_pipeline/dataset.py` | ML dataset loader. Only needed if you're doing ML with this data. |
| `data_unit_test.py` | Data validation concept is good. Needs expansion to all sources. |
| `.github/workflows/unit_tests.yml` | Test automation reference. |

### Tier 3: NOT NEEDED -- Delete or replace

| File | Reason |
|---|---|
| `scrape_logic/ftp_download.py` | 100% redundant with `scrape_ibkr_short_borrow.py`. |
| `configuration.py` | Consolidate into `ml_pipeline/config.py` or a single env-var-based config. Two config systems creates confusion. |
| `data_upload/stock_loan_data.csv` | Legacy file, not consumed by anything. |
| `tests/test_scraper.py` | Not automated, partially broken. Rewrite from scratch for v2. |
| `README.md` | Completely outdated. Write a new one. |
| `.github/workflows/main.yml` | FOMC scraper running hourly is wasteful. Rebuild with sensible schedule (daily or weekly). |

---

## 9. Architectural Recommendations for v2

### 9.1 Consolidate Configuration
- **Current**: Two config systems (`configuration.py` with hardcoded values, `ml_pipeline/config.py` with env vars).
- **Recommended**: Single config module using environment variables for everything. All secrets via `.env` file locally and GitHub Secrets in CI.

### 9.2 Add Missing Workflows
- `edgar_scraper.py` needs a GitHub Actions workflow (quarterly cadence makes sense).
- `scrape_ibkr_short_borrow.py` needs a workflow. This is a multi-step pipeline -- consider running all 5 steps in sequence in a single workflow, or breaking into separate triggered jobs.

### 9.3 Fix Credential Management
- Remove all hardcoded credentials from source code.
- Make scrapers read from environment variables, matching what workflows already pass.

### 9.4 Standardize Scraper Interface
- All scrapers should follow the same pattern: read config from env vars, accept CLI arguments, output to a consistent location.
- Consider a base scraper class or shared utilities.

### 9.5 Reduce Unnecessary Runs
- FOMC hourly schedule is wasteful. Change to daily or weekly.
- The daily unit test only validates NASDAQ data. Either expand coverage or run it less frequently.

### 9.6 Fix Git Push Strategy
- Replace `--force` push with regular push. Force push in automated workflows risks data loss.
- Consider using GitHub Actions artifacts instead of committing data files directly to the repo.

### 9.7 Clean Up Data Storage
- Raw CSV data files are committed to the git repo. This inflates repo size over time.
- Consider: scrape -> convert to parquet -> upload to cloud storage -> done. No need to persist raw CSVs in git.

### 9.8 Missing Dependencies
- Add `edgar` to `requirements.txt`.
- Add `google-auth` to `requirements.txt` (used by `query.py` for GCS DuckDB authentication).

### 9.9 File Structure for v2

```
TradingDataV2/
├── scrapers/
│   ├── __init__.py
│   ├── base.py              # Shared scraper interface
│   ├── alpha_earnings.py     # AlphaVantage
│   ├── nasdaq_earnings.py    # NASDAQ
│   ├── fomc.py               # Federal Reserve
│   ├── edgar.py              # SEC filings
│   ├── finra_ats.py          # FINRA OTC
│   └── ibkr_short.py         # Interactive Brokers
├── pipeline/
│   ├── __init__.py
│   ├── config.py             # Single config (env vars)
│   ├── convert.py            # Parquet conversion
│   ├── storage.py            # Cloud upload (GCS/S3)
│   ├── metadata.py           # Supabase tracking
│   ├── query.py              # DuckDB engine
│   └── orchestrator.py       # End-to-end pipeline
├── tests/
│   ├── test_scrapers.py
│   ├── test_pipeline.py
│   └── test_data_quality.py
├── .github/workflows/
│   ├── scrape.yml            # All scrapers (with matrix strategy)
│   ├── pipeline.yml          # Convert + upload
│   └── tests.yml             # All tests
├── requirements.txt
├── .env.example
├── schema.sql
└── README.md
```

---

## Summary

| Category | Count | Details |
|---|---|---|
| Total Python files | 17 | 7 scrapers, 8 ml_pipeline, 2 test files |
| Actively used files | 14 | Everything except `ftp_download.py`, `stock_loan_data.csv`, and `tests/test_scraper.py` |
| Redundant/unused files | 3 | `ftp_download.py`, `stock_loan_data.csv`, `tests/test_scraper.py` |
| Missing workflows | 2 | `edgar_scraper.py`, `scrape_ibkr_short_borrow.py` |
| Hardcoded credentials | 3 | AlphaVantage key, FINRA OAuth, SEC email |
| Missing dependencies | 2 | `edgar`, `google-auth` |
| Outdated docs | 1 | `README.md` |
