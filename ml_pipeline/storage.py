"""
Cloud storage abstraction — upload / download / list Parquet files on GCS or S3.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

from ml_pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


class CloudStorage:
    """Thin wrapper around GCS or S3, selected by config.storage_backend."""

    def __init__(self, config: PipelineConfig | None = None):
        self.cfg = config or PipelineConfig()
        self._client = None

    # ── lazy init ──────────────────────────────────────────────────────

    def _gcs_client(self):
        if self._client is None:
            from google.cloud import storage as gcs

            if self.cfg.gcs_credentials_json:
                import json, tempfile

                creds = json.loads(self.cfg.gcs_credentials_json)
                tmp = tempfile.NamedTemporaryFile(
                    suffix=".json", delete=False, mode="w"
                )
                json.dump(creds, tmp)
                tmp.close()
                self._client = gcs.Client.from_service_account_json(tmp.name)
            else:
                # falls back to GOOGLE_APPLICATION_CREDENTIALS env var
                self._client = gcs.Client()
        return self._client

    def _s3_client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                region_name=self.cfg.aws_region,
                aws_access_key_id=self.cfg.aws_access_key_id or None,
                aws_secret_access_key=self.cfg.aws_secret_access_key or None,
            )
        return self._client

    # ── public API ─────────────────────────────────────────────────────

    def upload_file(self, local_path: Path | str, cloud_key: str) -> str:
        """Upload a local file.  Returns the full cloud URI."""
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(local_path)

        if self.cfg.storage_backend == "gcs":
            bucket = self._gcs_client().bucket(self.cfg.gcs_bucket)
            blob = bucket.blob(cloud_key)
            blob.upload_from_filename(str(local_path))
            uri = f"gs://{self.cfg.gcs_bucket}/{cloud_key}"
        else:
            self._s3_client().upload_file(
                str(local_path), self.cfg.s3_bucket, cloud_key
            )
            uri = f"s3://{self.cfg.s3_bucket}/{cloud_key}"

        log.info("uploaded %s → %s", local_path.name, uri)
        return uri

    def upload_bytes(self, data: bytes, cloud_key: str) -> str:
        """Upload raw bytes (useful for in-memory Parquet buffers)."""
        if self.cfg.storage_backend == "gcs":
            bucket = self._gcs_client().bucket(self.cfg.gcs_bucket)
            blob = bucket.blob(cloud_key)
            blob.upload_from_string(data)
            uri = f"gs://{self.cfg.gcs_bucket}/{cloud_key}"
        else:
            self._s3_client().put_object(
                Bucket=self.cfg.s3_bucket, Key=cloud_key, Body=data
            )
            uri = f"s3://{self.cfg.s3_bucket}/{cloud_key}"

        log.info("uploaded %d bytes → %s", len(data), uri)
        return uri

    def download_bytes(self, cloud_key: str) -> bytes:
        """Download a file as bytes."""
        if self.cfg.storage_backend == "gcs":
            bucket = self._gcs_client().bucket(self.cfg.gcs_bucket)
            blob = bucket.blob(cloud_key)
            return blob.download_as_bytes()
        else:
            buf = io.BytesIO()
            self._s3_client().download_fileobj(self.cfg.s3_bucket, cloud_key, buf)
            return buf.getvalue()

    def list_keys(self, prefix: str) -> list[str]:
        """List object keys under a prefix."""
        if self.cfg.storage_backend == "gcs":
            bucket = self._gcs_client().bucket(self.cfg.gcs_bucket)
            return [b.name for b in bucket.list_blobs(prefix=prefix)]
        else:
            resp = self._s3_client().list_objects_v2(
                Bucket=self.cfg.s3_bucket, Prefix=prefix
            )
            return [obj["Key"] for obj in resp.get("Contents", [])]

    def exists(self, cloud_key: str) -> bool:
        """Check if an object exists."""
        if self.cfg.storage_backend == "gcs":
            bucket = self._gcs_client().bucket(self.cfg.gcs_bucket)
            return bucket.blob(cloud_key).exists()
        else:
            try:
                self._s3_client().head_object(
                    Bucket=self.cfg.s3_bucket, Key=cloud_key
                )
                return True
            except self._s3_client().exceptions.ClientError:
                return False

    def cloud_uri(self, cloud_key: str) -> str:
        """Return the full URI for a key."""
        if self.cfg.storage_backend == "gcs":
            return f"gs://{self.cfg.gcs_bucket}/{cloud_key}"
        return f"s3://{self.cfg.s3_bucket}/{cloud_key}"
