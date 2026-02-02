"""
Pipeline orchestrator — ties convert → upload → metadata into a single call.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ml_pipeline.config import PipelineConfig
from ml_pipeline.convert import ParquetConverter
from ml_pipeline.storage import CloudStorage
from ml_pipeline.metadata import MetadataTracker

log = logging.getLogger(__name__)


class MLPipeline:
    """End-to-end: raw files → Parquet → cloud → metadata."""

    def __init__(self, config: PipelineConfig | None = None):
        self.cfg = config or PipelineConfig()
        self.converter = ParquetConverter(self.cfg)
        self.storage = CloudStorage(self.cfg)
        self.metadata = MetadataTracker(self.cfg)

    def run(
        self,
        sources: list[str] | None = None,
        upload: bool = True,
        track: bool = True,
    ) -> dict:
        """Execute the full pipeline.

        Parameters
        ----------
        sources : list of source names to convert.  None = all.
        upload  : whether to push Parquet to cloud storage.
        track   : whether to log metadata to Supabase.

        Returns
        -------
        dict with keys: converted, uploaded, run_id
        """
        run_id = ""
        if track:
            try:
                run_id = self.metadata.start_run(
                    source="all" if sources is None else ",".join(sources),
                    action="convert_upload",
                )
            except Exception as e:
                log.warning("metadata start_run failed, continuing without tracking: %s", e)
                track = False

        # ── convert ────────────────────────────────────────────────────
        all_results = self.converter.convert_all()
        if sources is not None:
            all_results = {k: v for k, v in all_results.items() if k in sources}

        converted = {k: v for k, v in all_results.items() if v is not None}
        log.info("converted %d / %d sources", len(converted), len(all_results))

        # ── upload ─────────────────────────────────────────────────────
        uploaded = {}
        total_rows = 0

        for source, local_path in converted.items():
            if not upload:
                continue

            cloud_key = f"{self.cfg.cloud_prefix}/{local_path.name}"
            try:
                uri = self.storage.upload_file(local_path, cloud_key)
                uploaded[source] = uri

                # gather stats for metadata
                import pandas as pd

                df = pd.read_parquet(local_path)
                row_count = len(df)
                total_rows += row_count
                size_bytes = local_path.stat().st_size

                if track:
                    self.metadata.log_file(
                        run_id,
                        uri,
                        source,
                        row_count=row_count,
                        size_bytes=size_bytes,
                        schema={c: str(t) for c, t in zip(df.columns, df.dtypes)},
                    )

                    # index tickers if a symbol column exists
                    sym_col = next(
                        (c for c in df.columns if c.upper() in ("SYM", "SYMBOL")),
                        None,
                    )
                    if sym_col:
                        tickers = df[sym_col].dropna().unique().tolist()
                        self.metadata.index_tickers(run_id, source, tickers[:5000])

            except Exception as e:
                log.error("upload failed for %s: %s", source, e)

        # ── finish ─────────────────────────────────────────────────────
        if track:
            try:
                self.metadata.finish_run(
                    run_id,
                    status="success",
                    rows_processed=total_rows,
                    files_uploaded=len(uploaded),
                )
            except Exception as e:
                log.warning("metadata finish_run failed: %s", e)

        return {
            "run_id": run_id,
            "converted": converted,
            "uploaded": uploaded,
        }

    def convert_only(self, sources: list[str] | None = None) -> dict[str, Path]:
        """Just convert to Parquet locally — no cloud upload."""
        result = self.run(sources=sources, upload=False, track=False)
        return result["converted"]

    def status(self) -> dict:
        """Check what local Parquet files exist and cloud connectivity."""
        local_files = {}
        if self.cfg.parquet_dir.exists():
            for f in sorted(self.cfg.parquet_dir.glob("*.parquet")):
                import pandas as pd

                df = pd.read_parquet(f)
                local_files[f.stem] = {
                    "path": str(f),
                    "rows": len(df),
                    "columns": list(df.columns),
                    "size_mb": round(f.stat().st_size / 1e6, 2),
                }

        return {
            "storage_backend": self.cfg.storage_backend,
            "bucket": self.cfg.bucket,
            "cloud_base_uri": self.cfg.cloud_base_uri,
            "supabase_connected": self.metadata.enabled,
            "local_parquet_files": local_files,
            "config_issues": self.cfg.validate(),
        }
