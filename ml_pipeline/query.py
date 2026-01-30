"""
DuckDB query engine — run SQL directly on cloud-hosted Parquet files.

No downloads needed.  DuckDB's httpfs extension reads S3/GCS Parquet
in-place, making ad-hoc analytics over TB-scale data instant.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from ml_pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


class QueryEngine:
    """SQL interface over local or cloud-hosted Parquet files."""

    def __init__(self, config: PipelineConfig | None = None):
        self.cfg = config or PipelineConfig()
        self.con = duckdb.connect(":memory:")
        self._configure()

    def _configure(self):
        """Set memory limits, threads, and cloud credentials."""
        self.con.execute(f"SET memory_limit = '{self.cfg.duckdb_memory_limit}'")
        self.con.execute(f"SET threads = {self.cfg.duckdb_threads}")

        # Cloud filesystem extensions
        self.con.execute("INSTALL httpfs; LOAD httpfs;")

        if self.cfg.storage_backend == "s3":
            if self.cfg.aws_access_key_id:
                self.con.execute(
                    f"SET s3_access_key_id = '{self.cfg.aws_access_key_id}'"
                )
                self.con.execute(
                    f"SET s3_secret_access_key = '{self.cfg.aws_secret_access_key}'"
                )
            self.con.execute(f"SET s3_region = '{self.cfg.aws_region}'")

    # ── core query ─────────────────────────────────────────────────────

    def sql(self, query: str, params: list | None = None) -> pd.DataFrame:
        """Run arbitrary SQL and return a DataFrame."""
        log.debug("SQL: %s", query[:200])
        if params:
            return self.con.execute(query, params).fetchdf()
        return self.con.execute(query).fetchdf()

    # ── helpers for common patterns ────────────────────────────────────

    def _parquet_uri(self, source: str) -> str:
        """Resolve a source name to its Parquet URI (cloud or local)."""
        # prefer cloud
        cloud = f"{self.cfg.cloud_base_uri}/{source}.parquet"
        # fallback: local
        local = self.cfg.parquet_dir / f"{source}.parquet"
        if local.exists():
            return str(local)
        return cloud

    def describe(self, source: str) -> pd.DataFrame:
        """Show schema of a Parquet source."""
        uri = self._parquet_uri(source)
        return self.sql(f"DESCRIBE SELECT * FROM '{uri}'")

    def head(self, source: str, n: int = 10) -> pd.DataFrame:
        """Preview first N rows."""
        uri = self._parquet_uri(source)
        return self.sql(f"SELECT * FROM '{uri}' LIMIT {n}")

    def count(self, source: str) -> int:
        """Total row count."""
        uri = self._parquet_uri(source)
        return int(self.sql(f"SELECT count(*) AS n FROM '{uri}'").iloc[0, 0])

    def query_source(
        self,
        source: str,
        columns: str = "*",
        where: str = "",
        order_by: str = "",
        limit: int = 0,
    ) -> pd.DataFrame:
        """Structured query builder for a single source."""
        uri = self._parquet_uri(source)
        q = f"SELECT {columns} FROM '{uri}'"
        if where:
            q += f" WHERE {where}"
        if order_by:
            q += f" ORDER BY {order_by}"
        if limit:
            q += f" LIMIT {limit}"
        return self.sql(q)

    # ── cross-source joins ─────────────────────────────────────────────

    def register_sources(self, sources: list[str] | None = None):
        """Register Parquet files as named views for easy JOIN syntax.

        After calling this you can write:
            engine.sql("SELECT * FROM stock_loan JOIN earnings ON ...")
        """
        if sources is None:
            sources = [
                "stock_loan",
                "earnings",
                "nasdaq_earnings",
                "fomc",
                "ats_otc",
                "edgar_filings",
            ]
        for src in sources:
            uri = self._parquet_uri(src)
            try:
                self.con.execute(
                    f"CREATE OR REPLACE VIEW {src} AS SELECT * FROM '{uri}'"
                )
                log.info("registered view: %s → %s", src, uri)
            except Exception as e:
                log.warning("could not register %s: %s", src, e)

    # ── stock-loan specific queries ────────────────────────────────────

    def top_short_borrow(self, n: int = 50) -> pd.DataFrame:
        """Highest fee-rate stocks currently available to short."""
        uri = self._parquet_uri("stock_loan")
        return self.sql(
            f"""
            SELECT SYM, NAME, FEERATE, REBATERATE, AVAILABLE, CURRENCY, COUNTRY
            FROM '{uri}'
            WHERE AVAILABLE > 0
            ORDER BY FEERATE DESC
            LIMIT {n}
            """
        )

    def ticker_history(self, symbol: str) -> pd.DataFrame:
        """Full timestamp history for a single ticker."""
        uri = self._parquet_uri("stock_loan")
        return self.sql(
            f"""
            SELECT *
            FROM '{uri}'
            WHERE UPPER(SYM) = UPPER('{symbol}')
            ORDER BY TIMESTAMP
            """
        )

    def availability_changes(self, min_pct_change: float = 50.0) -> pd.DataFrame:
        """Find tickers whose available shares changed significantly."""
        uri = self._parquet_uri("stock_loan")
        return self.sql(
            f"""
            WITH ranked AS (
                SELECT *,
                       LAG(AVAILABLE) OVER (PARTITION BY SYM ORDER BY TIMESTAMP) AS prev_avail
                FROM '{uri}'
            )
            SELECT SYM, NAME, TIMESTAMP,
                   prev_avail, AVAILABLE,
                   ROUND(100.0 * (AVAILABLE - prev_avail) / NULLIF(prev_avail, 0), 2) AS pct_change
            FROM ranked
            WHERE prev_avail IS NOT NULL
              AND ABS(100.0 * (AVAILABLE - prev_avail) / NULLIF(prev_avail, 0)) >= {min_pct_change}
            ORDER BY ABS(pct_change) DESC
            """
        )
