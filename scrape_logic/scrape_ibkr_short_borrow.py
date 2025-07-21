import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_latest_download_folder():
    """Find the most recent FTP download folder"""
    base_dir = Path("data/ftp_raw_downloads")
    
    if not base_dir.exists():
        logger.error("❌ No download directory found. Run the downloader first.")
        return None
    
    folders = [f for f in base_dir.iterdir() if f.is_dir()]
    if not folders:
        logger.error("❌ No download folders found.")
        return None
    
    latest_folder = max(folders, key=lambda x: x.name)
    logger.info(f"🔍 Processing latest download: {latest_folder.name}")
    return latest_folder

def parse_ibkr_txt_file(file_path, folder_timestamp):
    """
    Parse an IBKR .txt file (pipe-delimited)
    Returns a cleaned DataFrame or None if parsing fails
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        header_line = None
        data_lines = []
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#EOF") or line.startswith("#BOF"):
                continue
            if line.startswith("#SYM|"):
                header_line = line[1:]  # remove #
                continue
            if line.startswith("#"):
                continue
            data_lines.append(line)
        
        if not header_line:
            logger.warning(f"❌ No header found in {file_path.name}")
            return None
        
        columns = [col.strip().upper() for col in header_line.split('|') if col.strip()]
        
        parsed_rows = []
        for line in data_lines:
            row_data = [col.strip() for col in line.split('|')]
            while len(row_data) < len(columns):
                row_data.append('')
            parsed_rows.append(row_data[:len(columns)])
        
        df = pd.DataFrame(parsed_rows, columns=columns)
        
        # Metadata
        df['COUNTRY'] = file_path.stem
        df['LAST_UPDATED'] = folder_timestamp

        # Clean up
        df = df.replace({'NA': pd.NA, '': pd.NA})

        for col in ['REBATERATE', 'FEERATE', 'AVAILABLE']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"✅ Parsed {file_path.name}: {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"❌ Error parsing {file_path.name}: {e}")
        return None

def update_usa_master(latest_folder):
    """
    Update USA master Parquet with new/changed rows
    """
    master_dir = Path("data/master_files")
    master_dir.mkdir(exist_ok=True)
    master_path = master_dir / "master_usa.parquet"

    # Timestamp from folder
    folder_timestamp = datetime.strptime(latest_folder.name, "%Y-%m-%d_%H-%M-%S")

    # Load or initialize master file
    if master_path.exists():
        master_df = pd.read_parquet(master_path)
        logger.info(f"📖 Loaded existing master with {len(master_df)} rows")
    else:
        master_df = pd.DataFrame()
        logger.info("🆕 No master file found — creating a new one.")

    usa_file = latest_folder / "usa.txt"
    if not usa_file.exists():
        logger.error("❌ usa.txt not found in latest folder.")
        return

    df = parse_ibkr_txt_file(usa_file, folder_timestamp)
    if df is None or df.empty:
        logger.warning("⚠️ Parsed DataFrame is empty or None.")
        return

    # Normalize columns to match dedup logic
    df.columns = [col.upper() for col in df.columns]

    # Deduplicate based on SYM and ISIN
    dedup_cols = ['SYM', 'ISIN']
    if not all(col in df.columns for col in dedup_cols):
        logger.error(f"❌ Missing dedup columns in {usa_file.name}")
        return

    if master_df.empty:
        updated_master = df
    else:
        combined = pd.concat([master_df, df], ignore_index=True)
        combined.sort_values("LAST_UPDATED", ascending=True, inplace=True)
        updated_master = combined.drop_duplicates(subset=["SYM", "ISIN", "COUNTRY"], keep="last")

    updated_master.to_parquet(master_path, index=False)
    logger.info(f"💾 USA master updated: {len(updated_master)} total rows")

    print("\n✅ Update complete.")
    print(f"📁 Saved to: {master_path}")
    print(f"📊 Rows added: {len(df)}")

def main():
    logger.info("🚀 Starting USA short borrow updater")
    latest_folder = get_latest_download_folder()
    if latest_folder:
        update_usa_master(latest_folder)

if __name__ == "__main__":
    main()
