"""
Centralized configuration — all secrets from environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()


@dataclass
class PipelineConfig:
    """All pipeline settings, loaded from env vars with sensible defaults."""

    # ── Paths ──────────────────────────────────────────────────────────
    project_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent
    )

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data_upload"

    @property
    def parquet_dir(self) -> Path:
        return self.project_root / "data_upload" / "parquet"

    # ── Cloud storage ──────────────────────────────────────────────────
    storage_backend: str = field(
        default_factory=lambda: os.getenv("STORAGE_BACKEND", "gcs")  # "gcs" | "s3"
    )
    gcs_bucket: str = field(
        default_factory=lambda: os.getenv("GCS_BUCKET", "")
    )
    gcs_credentials_json: str = field(
        default_factory=lambda: os.getenv("GCS_CREDENTIALS_JSON", "")
    )
    s3_bucket: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET", "")
    )
    aws_access_key_id: str = field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID", "")
    )
    aws_secret_access_key: str = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY", "")
    )
    aws_region: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )

    # ── Supabase / PostgreSQL ──────────────────────────────────────────
    supabase_url: str = field(
        default_factory=lambda: os.getenv("SUPABASE_URL", "")
    )
    supabase_key: str = field(
        default_factory=lambda: os.getenv("SUPABASE_KEY", "")
    )
    supabase_db_url: str = field(
        default_factory=lambda: os.getenv("SUPABASE_DB_URL", "")
    )  # postgresql://... direct connection string

    # ── DuckDB ─────────────────────────────────────────────────────────
    duckdb_memory_limit: str = field(
        default_factory=lambda: os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
    )
    duckdb_threads: int = field(
        default_factory=lambda: int(os.getenv("DUCKDB_THREADS", "4"))
    )

    # ── Data source prefixes (cloud paths) ─────────────────────────────
    cloud_prefix: str = field(
        default_factory=lambda: os.getenv("CLOUD_PREFIX", "trading_data")
    )

    # ── Convenience helpers ────────────────────────────────────────────

    @property
    def bucket(self) -> str:
        """Return the active bucket name."""
        if self.storage_backend == "gcs":
            return self.gcs_bucket
        return self.s3_bucket

    @property
    def cloud_base_uri(self) -> str:
        """Full URI root for DuckDB / cloud reads."""
        if self.storage_backend == "gcs":
            return f"gs://{self.gcs_bucket}/{self.cloud_prefix}"
        return f"s3://{self.s3_bucket}/{self.cloud_prefix}"

    def validate(self) -> list[str]:
        """Return a list of missing-but-required env vars."""
        issues = []
        if self.storage_backend == "gcs":
            if not self.gcs_bucket:
                issues.append("GCS_BUCKET is not set")
        elif self.storage_backend == "s3":
            if not self.s3_bucket:
                issues.append("S3_BUCKET is not set")
            if not self.aws_access_key_id:
                issues.append("AWS_ACCESS_KEY_ID is not set")
        if not self.supabase_url:
            issues.append("SUPABASE_URL is not set (metadata tracking disabled)")
        return issues
