import os
import pytest
from scrape_logic.nasdaq_earnings_calendar import save_earnings_to_csv
import pandas as pd
from scrape_logic.nasdaq_earnings_calendar import fetch_earnings_data
import subprocess
import datetime
import glob

# workflow verification tests
@pytest.mark.parametrize("csv_file", [
    "data_upload/earnings_calendar.csv",
    "data_upload/fomc_meeting_times.csv",
])
def test_csv_files_exist(csv_file):
    """Ensure workflow output CSV files exist"""
    assert os.path.exists(csv_file), f"{csv_file} was not created by the workflow."



# checks whether the scraped data is being saved properly to the intended csv files
@pytest.fixture
def mock_nasdaq_data():
    return [
        {"name": "Apple Inc.", "symbol": "AAPL", "eps": 1.50},
        {"name": "Microsoft Corp.", "symbol": "MSFT", "eps": 2.10},
    ]

def test_save_nasdaq_data(mock_nasdaq_data):
    save_earnings_to_csv(mock_nasdaq_data, "2025-02-18", "2025-02-18 10:00:00")
    csv_path = "data_upload/nasdaq_earnings_upload/nasdaq_earnings_2025-02-18.csv"
    assert os.path.exists(csv_path), "Nasdaq earnings CSV not created."
    
    df = pd.read_csv(csv_path)
    
    assert not df.empty, "CSV file should not be empty."
    assert set(df.columns) >= {"Company Name", "Symbol", "EPS Forecast"}, "CSV is missing expected columns."
    assert df.iloc[0]["Company Name"] == "Apple Inc.", "First entry in CSV is incorrect."

 

# ensures that the scrapers don’t crash and return None or handle errors correctly if an API call fails
def test_nasdaq_api_error_handling(monkeypatch):
    """Test that the script handles API request errors."""
    # mock the `requests.get` to simulate a failed API call
    def mock_requests_get(*args, **kwargs):
        raise Exception("API request failed")
    
    monkeypatch.setattr("requests.get", mock_requests_get)
    
    earnings_data = fetch_earnings_data("2025-02-18")
    
    # assert that the function returns None when the API request fails
    assert earnings_data is None, "Expected None when the API request fails." 



# runs the script manually and checks that the return code is 0 (success)
def test_workflow_trigger():
    """Test if the GitHub Action triggers the correct script."""
    result = subprocess.run(['python', 'scrape_logic/earnings_scrape.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    assert result.returncode == 0, f"Script failed with error: {result.stderr.decode()}"  


# testing the structure of output is correct: 
def test_nasdaq_api_response_structure():
    """Check if the API response returns a valid structure."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    earnings_data = fetch_earnings_data(today)

    assert isinstance(earnings_data, list), "Earnings data should be a list."
    if earnings_data:
        sample_record = earnings_data[0]
        expected_keys = {
            "name", "symbol", "marketCap", "epsForecast", "lastYearEPS", "time",
            "fiscalQuarterEnding", "noOfEsts"
        }
        assert isinstance(sample_record, dict), "Each record should be a dictionary."
        assert expected_keys.issubset(sample_record.keys()), f"Missing keys in API response: {expected_keys - sample_record.keys()}"



# if this test fails, scraper stopped working!
def test_scraper():
    """Check if the scraper is running and returning expected data format."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    earnings_data = fetch_earnings_data(today)

    assert earnings_data is not None, "Scraper returned None; possible failure."
    assert isinstance(earnings_data, list), "Scraper did not return a list."
    if earnings_data:
        assert isinstance(earnings_data[0], dict), "First entry is not a dictionary."



# this test checks if the CSVs are loading after workflow

#   Get the latest file:
def get_latest_file(directory, pattern):
    """Find the most recent file matching the pattern."""
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {directory} with pattern {pattern}")
    return max(files, key=os.path.getmtime)          # Get the most recently modified file



def test_data_loading():
    """Test that the CSV files contain data after workflows run."""
    earnings_file = get_latest_file("data_upload/nasdaq_earnings_upload", "nasdaq_earnings_*.csv")
    earnings_df = pd.read_csv(earnings_file)
    assert not earnings_df.empty, "Nasdaq earnings data should not be empty."