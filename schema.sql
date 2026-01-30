-- ============================================================
-- Supabase schema for ML pipeline metadata tracking
-- Run this in the Supabase SQL Editor to create the tables.
-- ============================================================

-- 1. Pipeline runs — one row per execution
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT UNIQUE NOT NULL,
    source      TEXT NOT NULL,             -- e.g. "stock_loan", "all"
    action      TEXT NOT NULL,             -- e.g. "convert_upload"
    status      TEXT NOT NULL DEFAULT 'running',  -- running | success | failed
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    rows_processed  INTEGER DEFAULT 0,
    files_uploaded  INTEGER DEFAULT 0,
    error       TEXT,
    extra       JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_runs_source ON pipeline_runs(source);
CREATE INDEX idx_runs_status ON pipeline_runs(status);
CREATE INDEX idx_runs_started ON pipeline_runs(started_at DESC);

-- 2. Dataset files — one row per uploaded Parquet file
CREATE TABLE IF NOT EXISTS dataset_files (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    cloud_uri   TEXT NOT NULL,             -- gs://bucket/path or s3://bucket/path
    source      TEXT NOT NULL,
    row_count   INTEGER DEFAULT 0,
    size_bytes  BIGINT DEFAULT 0,
    schema      JSONB DEFAULT '{}'::jsonb, -- column_name → dtype
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_files_source ON dataset_files(source);
CREATE INDEX idx_files_run    ON dataset_files(run_id);

-- 3. Ticker index — fast lookup of which tickers exist in which sources
CREATE TABLE IF NOT EXISTS ticker_index (
    ticker      TEXT NOT NULL,
    source      TEXT NOT NULL,
    last_run_id TEXT,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, source)
);

CREATE INDEX idx_ticker_sym ON ticker_index(ticker);

-- 4. Helpful views
CREATE OR REPLACE VIEW v_latest_run AS
SELECT DISTINCT ON (source)
    run_id, source, status, started_at, finished_at,
    rows_processed, files_uploaded
FROM pipeline_runs
ORDER BY source, started_at DESC;

CREATE OR REPLACE VIEW v_data_catalog AS
SELECT
    df.source,
    df.cloud_uri,
    df.row_count,
    pg_size_pretty(df.size_bytes) AS size,
    df.uploaded_at,
    pr.status AS run_status
FROM dataset_files df
JOIN pipeline_runs pr ON pr.run_id = df.run_id
ORDER BY df.uploaded_at DESC;
