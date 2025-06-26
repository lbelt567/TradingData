import requests
import json
import os
from datetime import datetime
import pandas as pd
from edgar import set_identity, get_filings

# Define directory to save CSV files
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data_upload", "financial_filings")
os.makedirs(DATA_DIR, exist_ok=True)

# SEC Company Ticker JSON URL
SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"

def fetch_cik_ticker_mapping():
    """Fetches the CIK-to-Ticker mapping from the SEC's online JSON file using Pandas."""
    headers = {"User-Agent": "Nico Monsalve (nmonsalve@g.hmc.edu)"}
    response = requests.get(SEC_TICKER_URL, headers=headers)

    if response.status_code == 200:
        data = response.json()
        
        # Convert JSON to DataFrame
        df = pd.DataFrame.from_dict(data, orient="index")

        # Ensure CIK is zero-padded to 10 digits for proper mapping
        df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)

        # Convert DataFrame to dictionary for fast lookups
        return pd.Series(df["ticker"].values, index=df["cik_str"]).to_dict()
    else:
        print(f"Failed to fetch ticker data. Status code: {response.status_code}")
        return {}

# Load the CIK-to-Ticker mapping once
CIK_TICKER_MAP = fetch_cik_ticker_mapping()

def generate_filing_link(cik, accession_number):
    """Generates the SEC EDGAR link for a given CIK and Accession Number."""
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}.txt"

def add_ticker_column(df):
    """Maps CIKs to tickers and places the ticker column next to the company column."""
    df["ticker"] = df["cik"].astype(str).str.zfill(10).map(CIK_TICKER_MAP)
    
    # Ensure ticker appears immediately after company column
    cols = df.columns.tolist()
    if "company" in cols and "ticker" in cols:
        cols.insert(cols.index("company") + 1, cols.pop(cols.index("ticker")))
        df = df[cols]

    return df

def get_10k(current_year, current_quarter):
    filings10k = get_filings(year=current_year, quarter=current_quarter, form="10-K")
    df_filings10k = filings10k.to_pandas()
    df_filings10k["filing_link"] = df_filings10k.apply(lambda row: generate_filing_link(row["cik"], row["accession_number"]), axis=1)

    # Add ticker column and reorder
    df_filings10k = add_ticker_column(df_filings10k)
    
    # Save to CSV
    df_filings10k.to_csv(os.path.join(DATA_DIR, f"10-K_filings_{current_year}_Q{current_quarter}.csv"), index=False)
    print(f"Saved: 10-K_filings_{current_year}_Q{current_quarter}.csv")
    print(df_filings10k.head())

def get_10q(current_year, current_quarter):
    filings10q = get_filings(year=current_year, quarter=current_quarter, form="10-Q")
    df_filings10q = filings10q.to_pandas()
    df_filings10q["filing_link"] = df_filings10q.apply(lambda row: generate_filing_link(row["cik"], row["accession_number"]), axis=1)

    # Add ticker column and reorder
    df_filings10q = add_ticker_column(df_filings10q)

    # Save to CSV
    df_filings10q.to_csv(os.path.join(DATA_DIR, f"10-Q_filings_{current_year}_Q{current_quarter}.csv"), index=False)
    print(f"Saved: 10-Q_filings_{current_year}_Q{current_quarter}.csv")
    print(df_filings10q.head())

def get_8k(current_year, current_quarter):
    filings8k = get_filings(year=current_year, quarter=current_quarter, form="8-K")
    df_filings8k  = filings8k.to_pandas()
    df_filings8k["filing_link"] = df_filings8k.apply(lambda row: generate_filing_link(row["cik"], row["accession_number"]), axis=1)

    # Add ticker column and reorder
    df_filings8k = add_ticker_column(df_filings8k)

    # Save to CSV
    df_filings8k.to_csv(os.path.join(DATA_DIR, f"8-K_filings_{current_year}_Q{current_quarter}.csv"), index=False)
    print(f"Saved: 8-K_filings_{current_year}_Q{current_quarter}.csv")
    print(df_filings8k.head())

def main():
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_quarter = (current_month - 1) // 3 + 1
    
    get_10k(current_year, current_quarter)
    get_10q(current_year, current_quarter)
    get_8k(current_year, current_quarter)

if __name__ == "__main__":
    set_identity("Nico Monsalve (nmonsalve@g.hmc.edu)")
    main()
