#!/usr/bin/env python3
"""
Stock Loan Data Pipeline
========================

Scrapes stock loan data from Interactive Brokers' FTP server, processes raw
.txt files, tracks changes with timestamps, and stores clean compressed
snapshots.

Usage:
    python scrape_ibkr_short_borrow.py --action download
    python scrape_ibkr_short_borrow.py --action process
    python scrape_ibkr_short_borrow.py --action resample
    python scrape_ibkr_short_borrow.py --action compile
    python scrape_ibkr_short_borrow.py --action delete
"""

import os
import re
import argparse
import ftplib
import tarfile
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FTP_SERVER = "ftp2.interactivebrokers.com"
FTP_USERNAME = "shortstock"
FTP_PASSWORD = ""

BASE_DIR = Path("data_upload")
COMPILED_DIR = BASE_DIR / "compiled"
RESAMPLED_DIR = BASE_DIR / "resampled"

# Columns whose changes trigger a new timestamp
CHANGE_COLS = ["AVAILABLE", "FEERATE", "REBATERATE"]
SYMBOL_COL = "SYM"

# Regex that matches our timestamped folder names
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$")


# ===================================================================== helpers
def _timestamped_folders(parent: Path):
    """Return all YYYY-MM-DD_HH-MM-SS sub-directories of *parent*, sorted."""
    if not parent.exists():
        return []
    return sorted(
        [d for d in parent.iterdir() if d.is_dir() and _TS_RE.match(d.name)],
        key=lambda d: d.name,
    )


def _latest_timestamped_folder(parent: Path):
    folders = _timestamped_folders(parent)
    return folders[-1] if folders else None


def _parse_bof_timestamp(line: str):
    """Parse ``#BOF|2025.04.14|19:06:47`` into a datetime."""
    try:
        parts = line.split("|")
        if len(parts) >= 3:
            return datetime.strptime(
                f"{parts[1].strip()} {parts[2].strip()}", "%Y.%m.%d %H:%M:%S"
            )
    except (ValueError, IndexError) as exc:
        logger.warning("Could not parse BOF timestamp from %r: %s", line, exc)
    return None


# ======================================================= STEP 1 – DOWNLOAD ==
def _scan_ftp_recursive(ftp: ftplib.FTP, path: str = "/"):
    """Walk *path* on the FTP server and return all file paths."""
    files: list[str] = []
    try:
        entries = list(ftp.mlsd(path))
    except ftplib.all_errors:
        # Fallback for servers that don't support MLSD
        try:
            names = ftp.nlst(path)
            return [n for n in names if n not in (".", "..")]
        except ftplib.all_errors:
            return files

    for name, facts in entries:
        if name in (".", ".."):
            continue
        full = f"{path.rstrip('/')}/{name}"
        entry_type = facts.get("type", "").lower()
        if entry_type == "dir":
            files.extend(_scan_ftp_recursive(ftp, full))
        elif entry_type == "file":
            files.append(full)
    return files


def action_download():
    """Connect to IB's FTP, download all .txt files to a timestamped folder."""
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_dir = BASE_DIR / timestamp
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    errors = 0

    try:
        with ftplib.FTP(FTP_SERVER) as ftp:
            ftp.login(FTP_USERNAME, FTP_PASSWORD)
            logger.info("Connected to FTP: %s", FTP_SERVER)

            all_files = _scan_ftp_recursive(ftp)
            txt_files = [f for f in all_files if f.lower().endswith(".txt")]
            logger.info("Found %d .txt files on FTP server", len(txt_files))

            for remote_path in txt_files:
                filename = os.path.basename(remote_path)
                local_path = snapshot_dir / filename
                try:
                    with open(local_path, "wb") as fh:
                        ftp.retrbinary(f"RETR {remote_path}", fh.write)
                    downloaded += 1
                    logger.info("Downloaded: %s", filename)
                except ftplib.all_errors as exc:
                    errors += 1
                    logger.error("Failed to download %s: %s", filename, exc)

        logger.info(
            "Download complete – %d files downloaded, %d errors", downloaded, errors
        )
        logger.info("Saved to: %s", snapshot_dir)
        return snapshot_dir

    except ftplib.all_errors as exc:
        logger.error("FTP connection error: %s", exc)
        return None


# ======================================================== STEP 2 – PROCESS ==
def parse_ibkr_txt_file(file_path: Path):
    """
    Parse a pipe-delimited IBKR .txt file.

    Returns
    -------
    (DataFrame, datetime | None)
        The parsed data and the BOF timestamp (if present).
    """
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            lines = fh.readlines()

        header_line = None
        data_lines: list[str] = []
        bof_timestamp = None

        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#BOF"):
                bof_timestamp = _parse_bof_timestamp(line)
                continue
            if line.startswith("#EOF"):
                continue
            if line.upper().startswith("#SYM|") or line.upper().startswith("#SYM\t"):
                header_line = line[1:]  # strip leading '#'
                continue
            if line.startswith("#"):
                continue
            data_lines.append(line)

        if not header_line:
            logger.warning("No header found in %s", file_path.name)
            return None, None

        columns = [c.strip().upper() for c in header_line.split("|") if c.strip()]

        parsed_rows: list[list[str]] = []
        for line in data_lines:
            row = [c.strip() for c in line.split("|")]
            while len(row) < len(columns):
                row.append("")
            parsed_rows.append(row[: len(columns)])

        if not parsed_rows:
            logger.warning("No data rows in %s", file_path.name)
            return None, None

        df = pd.DataFrame(parsed_rows, columns=columns)

        # Uppercase symbols, drop empties
        if SYMBOL_COL in df.columns:
            df[SYMBOL_COL] = df[SYMBOL_COL].str.upper().str.strip()
            df = df[df[SYMBOL_COL].str.len() > 0]

        # Replace placeholder strings with real NA
        df = df.replace({"NA": pd.NA, "": pd.NA})

        # Coerce numeric columns
        for col in CHANGE_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Country metadata from filename (e.g. usa.txt -> USA)
        df["COUNTRY"] = file_path.stem.upper()

        logger.info("Parsed %s: %d rows", file_path.name, len(df))
        return df, bof_timestamp

    except Exception as exc:
        logger.error("Error parsing %s: %s", file_path.name, exc)
        return None, None


def _merge_with_timestamp_tracking(prev_df, new_df, new_timestamp):
    """
    For each symbol in *new_df*, check if AVAILABLE / FEERATE / REBATERATE
    changed vs *prev_df*.  Changed rows get *new_timestamp*; unchanged rows
    keep their old TIMESTAMP.
    """
    if SYMBOL_COL not in prev_df.columns or SYMBOL_COL not in new_df.columns:
        return new_df

    compare_cols = [
        c for c in CHANGE_COLS if c in new_df.columns and c in prev_df.columns
    ]
    if not compare_cols:
        return new_df

    # Build a lookup from the previous data keyed on symbol
    prev_indexed = prev_df.set_index(SYMBOL_COL)

    timestamps = []
    for _, row in new_df.iterrows():
        sym = row[SYMBOL_COL]

        if sym not in prev_indexed.index:
            # Brand-new symbol
            timestamps.append(new_timestamp)
            continue

        prev_row = prev_indexed.loc[sym]
        if isinstance(prev_row, pd.DataFrame):
            prev_row = prev_row.iloc[-1]

        changed = False
        for col in compare_cols:
            nv, ov = row[col], prev_row[col]
            if pd.isna(nv) and pd.isna(ov):
                continue
            if pd.isna(nv) or pd.isna(ov) or nv != ov:
                changed = True
                break

        if changed:
            timestamps.append(new_timestamp)
        else:
            timestamps.append(prev_row.get("TIMESTAMP", new_timestamp))

    out = new_df.copy()
    out["TIMESTAMP"] = timestamps
    return out


def action_process():
    """Parse raw .txt files, compare to previous scrape, track timestamps."""
    latest_folder = _latest_timestamped_folder(BASE_DIR)
    if not latest_folder:
        logger.error("No download folders found in %s", BASE_DIR)
        return

    COMPILED_DIR.mkdir(parents=True, exist_ok=True)

    txt_files = list(latest_folder.glob("*.txt"))
    if not txt_files:
        logger.error("No .txt files in %s", latest_folder)
        return

    logger.info("Processing %d files from %s", len(txt_files), latest_folder.name)

    for txt_file in txt_files:
        logger.info("--- Processing: %s ---", txt_file.name)

        df, bof_ts = parse_ibkr_txt_file(txt_file)
        if df is None or df.empty:
            logger.warning("Skipping %s – no data", txt_file.name)
            continue

        # Determine timestamp: prefer BOF header, fall back to folder name
        if bof_ts is None:
            bof_ts = datetime.strptime(latest_folder.name, "%Y-%m-%d_%H-%M-%S")

        df["TIMESTAMP"] = bof_ts

        # Compare with previous processed version
        processed_name = f"{txt_file.stem}_processed.pkl"
        prev_path = COMPILED_DIR / processed_name

        if prev_path.exists():
            prev_df = pd.read_pickle(prev_path)
            logger.info(
                "Loaded previous %s: %d rows", processed_name, len(prev_df)
            )
            df = _merge_with_timestamp_tracking(prev_df, df, bof_ts)

        df.to_pickle(prev_path)
        logger.info("Saved %s (%d rows)", processed_name, len(df))

    logger.info("Processing complete.")


# ====================================================== STEP 3 – RESAMPLE ==
def action_resample():
    """Deduplicate processed .pkl files and save compressed .pkl.gz."""
    if not COMPILED_DIR.exists():
        logger.error("Compiled directory not found: %s", COMPILED_DIR)
        return

    pkl_files = list(COMPILED_DIR.glob("*_processed.pkl"))
    if not pkl_files:
        logger.error("No *_processed.pkl files in %s", COMPILED_DIR)
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = RESAMPLED_DIR / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Resampling %d files -> %s", len(pkl_files), output_dir)

    for pkl_file in pkl_files:
        df = pd.read_pickle(pkl_file)

        # Clean symbols
        if SYMBOL_COL in df.columns:
            df[SYMBOL_COL] = df[SYMBOL_COL].str.upper().str.strip()
            df = df[df[SYMBOL_COL].str.len() > 0]

        # Deduplicate on everything except TIMESTAMP
        dedup_cols = [c for c in df.columns if c != "TIMESTAMP"]
        df = df.drop_duplicates(subset=dedup_cols, keep="last")

        out_name = pkl_file.stem + ".pkl.gz"
        df.to_pickle(output_dir / out_name, compression="gzip")
        logger.info("Resampled: %s (%d rows)", out_name, len(df))

    logger.info("Resampling complete.")


# ======================================================= STEP 4 – COMPILE ==
def action_compile():
    """Merge all resampled .pkl.gz into a single master.pkl.gz."""
    latest = _latest_timestamped_folder(RESAMPLED_DIR)
    if not latest:
        logger.error("No resampled folders found in %s", RESAMPLED_DIR)
        return

    pkl_files = [f for f in latest.glob("*.pkl.gz") if f.name != "master.pkl.gz"]
    if not pkl_files:
        logger.error("No .pkl.gz files in %s", latest)
        return

    logger.info("Compiling %d files from %s", len(pkl_files), latest.name)

    frames = []
    for f in pkl_files:
        df = pd.read_pickle(f)
        frames.append(df)
        logger.info("Loaded: %s (%d rows)", f.name, len(df))

    master = pd.concat(frames, ignore_index=True)

    dedup_cols = [c for c in master.columns if c != "TIMESTAMP"]
    master = master.drop_duplicates(subset=dedup_cols, keep="last")

    master_path = latest / "master.pkl.gz"
    master.to_pickle(master_path, compression="gzip")
    logger.info("Master compiled: %d rows -> %s", len(master), master_path)


# ======================================================== STEP 5 – DELETE ==
def action_delete():
    """Archive raw download folders and old resampled folders."""
    # --- archive raw download folders ---
    raw_folders = _timestamped_folders(BASE_DIR)
    if raw_folders:
        logger.info("Archiving %d raw download folder(s)", len(raw_folders))
        for folder in raw_folders:
            archive_path = BASE_DIR / f"{folder.name}.tar.gz"
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(folder, arcname=folder.name)
            shutil.rmtree(folder)
            logger.info("Archived & removed: %s", folder.name)

    # --- keep only the latest resampled folder ---
    if RESAMPLED_DIR.exists():
        resampled = sorted(
            [d for d in RESAMPLED_DIR.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )
        if len(resampled) > 1:
            to_archive = resampled[:-1]
            logger.info("Archiving %d old resampled folder(s)", len(to_archive))
            for folder in to_archive:
                archive_path = RESAMPLED_DIR / f"{folder.name}.tar.gz"
                with tarfile.open(archive_path, "w:gz") as tar:
                    tar.add(folder, arcname=folder.name)
                shutil.rmtree(folder)
                logger.info("Archived & removed: %s", folder.name)
            logger.info("Kept latest: %s", resampled[-1].name)

    logger.info("Cleanup complete.")


# ================================================================= CLI =====
def main():
    parser = argparse.ArgumentParser(
        description="Stock Loan Data Pipeline – IBKR short borrow data"
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["download", "process", "resample", "compile", "delete"],
        help="Pipeline step to execute",
    )
    args = parser.parse_args()

    dispatch = {
        "download": action_download,
        "process": action_process,
        "resample": action_resample,
        "compile": action_compile,
        "delete": action_delete,
    }

    logger.info("=== Pipeline action: %s ===", args.action)
    dispatch[args.action]()


if __name__ == "__main__":
    main()
