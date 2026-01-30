"""
ML-ready dataset loader.

Reads Parquet data (local or cloud) and exposes it as:
  - pandas DataFrame  (default)
  - numpy arrays      (.to_numpy())
  - PyTorch Dataset   (.to_torch())

Includes common feature-engineering helpers for trading models.
"""

from __future__ import annotations

import logging
from typing import Sequence

import numpy as np
import pandas as pd

from ml_pipeline.config import PipelineConfig
from ml_pipeline.query import QueryEngine

log = logging.getLogger(__name__)


class TradingDataset:
    """Load, merge, and transform trading data for ML consumption."""

    def __init__(
        self,
        sources: Sequence[str] | None = None,
        config: PipelineConfig | None = None,
    ):
        self.cfg = config or PipelineConfig()
        self.engine = QueryEngine(self.cfg)
        self.sources = list(sources or ["stock_loan"])
        self._df: pd.DataFrame | None = None

    # ── loading ────────────────────────────────────────────────────────

    def load(
        self,
        where: str = "",
        columns: str = "*",
        limit: int = 0,
    ) -> "TradingDataset":
        """Load data from configured sources into memory."""
        frames = []
        for src in self.sources:
            df = self.engine.query_source(
                src, columns=columns, where=where, limit=limit
            )
            df["_source"] = src
            frames.append(df)

        self._df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        log.info("loaded %d rows from %s", len(self._df), self.sources)
        return self

    def load_sql(self, query: str) -> "TradingDataset":
        """Load via raw SQL for full flexibility (joins, CTEs, etc.)."""
        self.engine.register_sources(self.sources)
        self._df = self.engine.sql(query)
        log.info("loaded %d rows from SQL query", len(self._df))
        return self

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            raise RuntimeError("Call .load() or .load_sql() first")
        return self._df

    # ── feature engineering helpers ─────────────────────────────────────

    def add_log_returns(self, col: str = "FEERATE", output: str = "log_return"):
        """Compute log-return of a numeric column (grouped by ticker)."""
        self.df[output] = self.df.groupby("SYM")[col].transform(
            lambda s: np.log(s / s.shift(1))
        )
        return self

    def add_z_score(self, col: str, window: int = 20, output: str | None = None):
        """Rolling z-score within each ticker group."""
        out = output or f"{col}_zscore"
        grouped = self.df.groupby("SYM")[col]
        roll_mean = grouped.transform(lambda s: s.rolling(window).mean())
        roll_std = grouped.transform(lambda s: s.rolling(window).std())
        self.df[out] = (self.df[col] - roll_mean) / roll_std.replace(0, np.nan)
        return self

    def add_pct_rank(self, col: str, output: str | None = None):
        """Cross-sectional percentile rank at each timestamp."""
        out = output or f"{col}_pctrank"
        if "TIMESTAMP" in self.df.columns:
            self.df[out] = self.df.groupby("TIMESTAMP")[col].rank(pct=True)
        else:
            self.df[out] = self.df[col].rank(pct=True)
        return self

    def add_lag(self, col: str, periods: int = 1, output: str | None = None):
        """Lagged value of a column within each ticker."""
        out = output or f"{col}_lag{periods}"
        self.df[out] = self.df.groupby("SYM")[col].shift(periods)
        return self

    def add_diff(self, col: str, periods: int = 1, output: str | None = None):
        """First difference within each ticker."""
        out = output or f"{col}_diff{periods}"
        self.df[out] = self.df.groupby("SYM")[col].diff(periods)
        return self

    # ── output formats ─────────────────────────────────────────────────

    def to_pandas(self) -> pd.DataFrame:
        """Return the internal DataFrame."""
        return self.df.copy()

    def to_numpy(
        self, feature_cols: list[str] | None = None, target_col: str | None = None
    ) -> tuple[np.ndarray, np.ndarray | None]:
        """Return (X, y) numpy arrays.

        Parameters
        ----------
        feature_cols : columns to use as features (default: all numeric)
        target_col   : column to use as target (default: None → y is None)
        """
        df = self.df.copy()
        if feature_cols is None:
            feature_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if target_col and target_col in feature_cols:
                feature_cols.remove(target_col)

        X = df[feature_cols].values.astype(np.float32)
        y = df[target_col].values.astype(np.float32) if target_col else None
        return X, y

    def to_torch(
        self,
        feature_cols: list[str] | None = None,
        target_col: str | None = None,
    ):
        """Return a PyTorch TensorDataset.

        Import is deferred so torch is optional.
        """
        import torch
        from torch.utils.data import TensorDataset

        X, y = self.to_numpy(feature_cols, target_col)
        tensors = [torch.from_numpy(X)]
        if y is not None:
            tensors.append(torch.from_numpy(y))
        return TensorDataset(*tensors)

    # ── train / test split ─────────────────────────────────────────────

    def time_split(
        self,
        time_col: str = "TIMESTAMP",
        train_frac: float = 0.8,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Chronological train/test split (no leakage)."""
        df = self.df.sort_values(time_col)
        n = int(len(df) * train_frac)
        return df.iloc[:n].copy(), df.iloc[n:].copy()

    def ticker_split(
        self,
        test_tickers: list[str],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split by ticker — useful for out-of-sample generalization tests."""
        mask = self.df["SYM"].isin([t.upper() for t in test_tickers])
        return self.df[~mask].copy(), self.df[mask].copy()

    # ── convenience ────────────────────────────────────────────────────

    def dropna(self, subset: list[str] | None = None) -> "TradingDataset":
        """Drop rows with NaN (in-place)."""
        self._df = self.df.dropna(subset=subset).reset_index(drop=True)
        return self

    def filter_tickers(self, tickers: list[str]) -> "TradingDataset":
        """Keep only specific tickers."""
        upper = [t.upper() for t in tickers]
        self._df = self.df[self.df["SYM"].isin(upper)].reset_index(drop=True)
        return self

    def __len__(self):
        return len(self.df) if self._df is not None else 0

    def __repr__(self):
        n = len(self) if self._df is not None else 0
        return f"TradingDataset(sources={self.sources}, rows={n})"
