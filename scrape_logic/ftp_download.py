#!/usr/bin/env python3
"""
Standalone FTP downloader for Interactive Brokers short stock borrow data.

Downloads all .txt files from ftp2.interactivebrokers.com into a timestamped
folder under ``data_upload/YYYY-MM-DD_HH-MM-SS/``.

This is also available as ``--action download`` in the main pipeline
(scrape_ibkr_short_borrow.py).
"""

import os
import ftplib
from datetime import datetime
from pathlib import Path

FTP_SERVER = "ftp2.interactivebrokers.com"
FTP_USERNAME = "shortstock"
FTP_PASSWORD = ""

BASE_DIR = Path("data_upload")


def download_ftp_snapshot():
    """
    Connect to the FTP server, download all .txt files into a timestamped
    folder, and return the path to that folder.
    """
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_dir = BASE_DIR / timestamp
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    errors = 0

    try:
        with ftplib.FTP(FTP_SERVER) as ftp:
            ftp.login(FTP_USERNAME, FTP_PASSWORD)
            print(f"Connected to FTP: {FTP_SERVER}")

            # Try MLSD first (returns type info), fall back to NLST
            try:
                entries = list(ftp.mlsd("/"))
                all_files = [
                    name
                    for name, facts in entries
                    if facts.get("type", "").lower() == "file"
                ]
            except ftplib.all_errors:
                all_files = [
                    f for f in ftp.nlst() if f not in (".", "..")
                ]

            txt_files = [f for f in all_files if f.lower().endswith(".txt")]
            print(f"Found {len(txt_files)} .txt files on FTP server")

            for filename in txt_files:
                local_path = snapshot_dir / os.path.basename(filename)
                try:
                    with open(local_path, "wb") as fh:
                        ftp.retrbinary(f"RETR {filename}", fh.write)
                    downloaded += 1
                    print(f"Downloaded: {filename}")
                except ftplib.all_errors as exc:
                    errors += 1
                    print(f"Failed to download {filename}: {exc}")

        print(f"Download complete: {downloaded} files, {errors} errors")
        print(f"Saved to: {snapshot_dir}")
        return snapshot_dir

    except ftplib.all_errors as exc:
        print(f"FTP connection error: {exc}")
        return None


if __name__ == "__main__":
    download_ftp_snapshot()
