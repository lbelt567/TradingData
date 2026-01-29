"""
ML Data Pipeline for TradingData.

Connects scraped financial data to ML/trading models via:
- Cloud storage (GCS / S3) for Parquet files
- Supabase (PostgreSQL) for metadata tracking
- DuckDB for fast SQL queries on cloud-hosted Parquet
- Dataset loaders for pandas, numpy, and PyTorch
"""

from ml_pipeline.config import PipelineConfig
from ml_pipeline.convert import ParquetConverter
from ml_pipeline.storage import CloudStorage
from ml_pipeline.metadata import MetadataTracker
from ml_pipeline.query import QueryEngine
from ml_pipeline.dataset import TradingDataset
from ml_pipeline.pipeline import MLPipeline

__all__ = [
    "PipelineConfig",
    "ParquetConverter",
    "CloudStorage",
    "MetadataTracker",
    "QueryEngine",
    "TradingDataset",
    "MLPipeline",
]
