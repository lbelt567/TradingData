"""
Metadata tracking via Supabase (PostgreSQL).

Logs every pipeline run, dataset version, and per-file upload so you have
a single source of truth for what data exists, when it was refreshed, and
where it lives in cloud storage.
"""

from __future__ import annotations

import datetime as dt
import logging
import uuid

from ml_pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


class MetadataTracker:
    """Thin Supabase client that writes to three tables:
    - pipeline_runs   — one row per pipeline execution
    - dataset_files   — one row per Parquet file uploaded
    - ticker_index    — lookup of tickers ↔ datasets for fast filtering
    """

    def __init__(self, config: PipelineConfig | None = None):
        self.cfg = config or PipelineConfig()
        self._client = None

    # ── connection ─────────────────────────────────────────────────────

    @property
    def client(self):
        if self._client is None:
            if not self.cfg.supabase_url or not self.cfg.supabase_key:
                log.warning(
                    "SUPABASE_URL / SUPABASE_KEY not set — metadata tracking disabled"
                )
                return None
            from supabase import create_client

            self._client = create_client(self.cfg.supabase_url, self.cfg.supabase_key)
        return self._client

    @property
    def enabled(self) -> bool:
        return self.client is not None

    # ── pipeline runs ──────────────────────────────────────────────────

    def start_run(
        self, source: str, action: str, *, extra: dict | None = None
    ) -> str:
        """Record the start of a pipeline run.  Returns run_id."""
        run_id = uuid.uuid4().hex[:12]
        row = {
            "run_id": run_id,
            "source": source,
            "action": action,
            "status": "running",
            "started_at": dt.datetime.utcnow().isoformat(),
            "extra": extra or {},
        }
        if self.enabled:
            self.client.table("pipeline_runs").insert(row).execute()
        log.info("run %s started  [%s/%s]", run_id, source, action)
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: str = "success",
        rows_processed: int = 0,
        files_uploaded: int = 0,
        error: str | None = None,
    ):
        """Mark a pipeline run as finished."""
        update = {
            "status": status,
            "finished_at": dt.datetime.utcnow().isoformat(),
            "rows_processed": rows_processed,
            "files_uploaded": files_uploaded,
        }
        if error:
            update["error"] = error
        if self.enabled:
            (
                self.client.table("pipeline_runs")
                .update(update)
                .eq("run_id", run_id)
                .execute()
            )
        log.info("run %s finished  [%s, %d rows, %d files]", run_id, status, rows_processed, files_uploaded)

    # ── dataset files ──────────────────────────────────────────────────

    def log_file(
        self,
        run_id: str,
        cloud_uri: str,
        source: str,
        *,
        row_count: int = 0,
        size_bytes: int = 0,
        schema: dict | None = None,
    ):
        """Log a single uploaded Parquet file."""
        row = {
            "run_id": run_id,
            "cloud_uri": cloud_uri,
            "source": source,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "schema": schema or {},
            "uploaded_at": dt.datetime.utcnow().isoformat(),
        }
        if self.enabled:
            self.client.table("dataset_files").insert(row).execute()

    # ── ticker index ───────────────────────────────────────────────────

    def index_tickers(self, run_id: str, source: str, tickers: list[str]):
        """Upsert tickers seen in a dataset for fast lookups."""
        if not self.enabled or not tickers:
            return
        rows = [
            {
                "ticker": t,
                "source": source,
                "last_run_id": run_id,
                "last_seen": dt.datetime.utcnow().isoformat(),
            }
            for t in tickers
        ]
        self.client.table("ticker_index").upsert(
            rows, on_conflict="ticker,source"
        ).execute()

    # ── queries ────────────────────────────────────────────────────────

    def latest_files(self, source: str, limit: int = 20) -> list[dict]:
        """Return the most recent uploaded files for a source."""
        if not self.enabled:
            return []
        resp = (
            self.client.table("dataset_files")
            .select("*")
            .eq("source", source)
            .order("uploaded_at", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data

    def run_history(self, source: str | None = None, limit: int = 50) -> list[dict]:
        """Return recent pipeline runs, optionally filtered by source."""
        if not self.enabled:
            return []
        q = self.client.table("pipeline_runs").select("*")
        if source:
            q = q.eq("source", source)
        resp = q.order("started_at", desc=True).limit(limit).execute()
        return resp.data
