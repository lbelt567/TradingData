"""
Convert raw CSV / pickle / pkl.gz data to columnar Parquet files.

Each data source gets its own conversion routine with appropriate
schema coercion, partitioning, and compression.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ml_pipeline.config import PipelineConfig

log = logging.getLogger(__name__)

# ── Column schemas per source ──────────────────────────────────────────

STOCK_LOAN_DTYPES = {
    "SYM": "string",
    "CURRENCY": "string",
    "NAME": "string",
    "CON": "string",           # IB CONTRACT ID
    "ISIN": "string",
    "REBATERATE": "float64",
    "FEERATE": "float64",
    "AVAILABLE": "float64",
    "COUNTRY": "string",
}

EARNINGS_DTYPES = {
    "symbol": "string",
    "name": "string",
    "reportDate": "string",
    "fiscalDateEnding": "string",
    "estimate": "float64",
    "currency": "string",
}

NASDAQ_EARNINGS_DTYPES = {
    "Company Name": "string",
    "Symbol": "string",
    "Market Cap": "string",
    "EPS Forecast": "string",
    "Time": "string",
    "Fiscal Quarter Ending": "string",
    "No. of Estimates": "string",
    "Last Year Report Date": "string",
    "Last Year EPS": "string",
}

FOMC_DTYPES = {
    "Year": "string",
    "Month": "string",
    "Dates": "string",
}

ATS_DTYPES = {
    "issueSymbolIdentifier": "string",
    "totalWeeklyShareQuantity": "float64",
    "totalWeeklyTradeCount": "float64",
    "lastUpdateDate": "string",
}


def _sanitize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce object columns to pandas StringDtype so PyArrow sees
    proper nulls instead of mixed str/float(NaN) types."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")
    return df


class ParquetConverter:
    """Reads local raw files and writes Parquet with proper schemas."""

    def __init__(self, config: PipelineConfig | None = None):
        self.cfg = config or PipelineConfig()
        self.cfg.parquet_dir.mkdir(parents=True, exist_ok=True)

    # ── public per-source converters ───────────────────────────────────

    def convert_stock_loan(self) -> Path | None:
        """Convert master.pkl.gz → stock_loan.parquet"""
        # find the most recent master.pkl.gz or master.pkl
        candidates = sorted(
            self.cfg.data_dir.rglob("master.pkl*"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        if not candidates:
            log.warning("No master.pkl* found — skipping stock_loan conversion")
            return None

        src = candidates[0]
        log.info("reading %s", src)
        df = pd.read_pickle(src)

        # normalise column names
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        rename_map = {
            "IB_CONTRACT_ID": "CON",
            "REBATE_RATE": "REBATERATE",
            "FEE_RATE": "FEERATE",
            "AVAILABLE_SHARES": "AVAILABLE",
        }
        df.rename(columns=rename_map, inplace=True)

        for col, dtype in STOCK_LOAN_DTYPES.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype, errors="ignore")

        # keep TIMESTAMP if present
        if "TIMESTAMP" in df.columns:
            df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")

        out = self.cfg.parquet_dir / "stock_loan.parquet"
        table = pa.Table.from_pandas(_sanitize_for_arrow(df))
        pq.write_table(table, out, compression="snappy")
        log.info("wrote %s  (%d rows, %.1f MB)", out.name, len(df), out.stat().st_size / 1e6)
        return out

    def convert_earnings(self) -> Path | None:
        """Convert earnings_calendar.csv → earnings.parquet"""
        src = self.cfg.data_dir / "earnings_calendar.csv"
        if not src.exists():
            log.warning("earnings_calendar.csv not found")
            return None

        df = pd.read_csv(src)
        for col, dtype in EARNINGS_DTYPES.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype, errors="ignore")

        out = self.cfg.parquet_dir / "earnings.parquet"
        pq.write_table(pa.Table.from_pandas(_sanitize_for_arrow(df)), out, compression="snappy")
        log.info("wrote %s  (%d rows)", out.name, len(df))
        return out

    def convert_nasdaq_earnings(self) -> Path | None:
        """Merge all daily NASDAQ earnings CSVs → nasdaq_earnings.parquet"""
        src_dir = self.cfg.data_dir / "nasdaq_earnings_upload"
        if not src_dir.exists():
            log.warning("nasdaq_earnings_upload/ not found")
            return None

        csvs = sorted(src_dir.glob("*.csv"))
        if not csvs:
            log.warning("No NASDAQ earnings CSVs found")
            return None

        frames = []
        for f in csvs:
            try:
                frames.append(pd.read_csv(f))
            except Exception as e:
                log.warning("skipping %s: %s", f.name, e)
        if not frames:
            return None

        df = pd.concat(frames, ignore_index=True)
        out = self.cfg.parquet_dir / "nasdaq_earnings.parquet"
        pq.write_table(pa.Table.from_pandas(_sanitize_for_arrow(df)), out, compression="snappy")
        log.info("wrote %s  (%d rows from %d files)", out.name, len(df), len(csvs))
        return out

    def convert_fomc(self) -> Path | None:
        """Convert fomc_meeting_times.csv → fomc.parquet"""
        src = self.cfg.data_dir / "fomc_meeting_times.csv"
        if not src.exists():
            log.warning("fomc_meeting_times.csv not found")
            return None

        df = pd.read_csv(src)
        out = self.cfg.parquet_dir / "fomc.parquet"
        pq.write_table(pa.Table.from_pandas(_sanitize_for_arrow(df)), out, compression="snappy")
        log.info("wrote %s  (%d rows)", out.name, len(df))
        return out

    def convert_ats(self) -> Path | None:
        """Merge all ATS weekly CSVs → ats_otc.parquet"""
        src_dir = self.cfg.data_dir / "ats_upload"
        if not src_dir.exists():
            log.warning("ats_upload/ not found")
            return None

        csvs = sorted(src_dir.glob("*.csv"))
        if not csvs:
            log.warning("No ATS CSVs found")
            return None

        frames = []
        for f in csvs:
            try:
                frames.append(pd.read_csv(f))
            except Exception as e:
                log.warning("skipping %s: %s", f.name, e)
        if not frames:
            return None

        df = pd.concat(frames, ignore_index=True)
        out = self.cfg.parquet_dir / "ats_otc.parquet"
        pq.write_table(pa.Table.from_pandas(_sanitize_for_arrow(df)), out, compression="snappy")
        log.info("wrote %s  (%d rows from %d files)", out.name, len(df), len(csvs))
        return out

    def convert_edgar(self) -> Path | None:
        """Merge SEC filings CSVs → edgar_filings.parquet"""
        src_dir = self.cfg.data_dir / "financial_filings"
        if not src_dir.exists():
            log.warning("financial_filings/ not found")
            return None

        csvs = sorted(src_dir.glob("*.csv"))
        if not csvs:
            return None

        frames = []
        for f in csvs:
            try:
                tmp = pd.read_csv(f)
                tmp["filing_type"] = f.stem.split("_")[0]  # 10-K, 10-Q, 8-K
                frames.append(tmp)
            except Exception as e:
                log.warning("skipping %s: %s", f.name, e)
        if not frames:
            return None

        df = pd.concat(frames, ignore_index=True)
        out = self.cfg.parquet_dir / "edgar_filings.parquet"
        pq.write_table(pa.Table.from_pandas(_sanitize_for_arrow(df)), out, compression="snappy")
        log.info("wrote %s  (%d rows from %d files)", out.name, len(df), len(csvs))
        return out

    # ── batch convert all ──────────────────────────────────────────────

    def convert_all(self) -> dict[str, Path | None]:
        """Run every converter and return {source: output_path}."""
        return {
            "stock_loan": self.convert_stock_loan(),
            "earnings": self.convert_earnings(),
            "nasdaq_earnings": self.convert_nasdaq_earnings(),
            "fomc": self.convert_fomc(),
            "ats_otc": self.convert_ats(),
            "edgar_filings": self.convert_edgar(),
        }
