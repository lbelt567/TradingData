import os
import unittest
import pandas as pd

# Define folder containing nasdaq earnings CSVs
NASDAQ_EARNINGS_DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_upload', 'nasdaq_earnings_upload'))

# Define valid ranges
MARKET_CAP_MIN = 1e5  # $100 thousand
MARKET_CAP_MAX = 5e12  # $5 trillion
EPS_FORECAST_MIN = -10  # Minimum reasonable EPS
EPS_FORECAST_MAX = 50  # Maximum reasonable EPS

def clean_numeric_column(series, remove_dollar=True):
    """
    Converts a Pandas Series to numeric values, removing dollar signs, commas, and parentheses.
    """
    series = series.astype(str).str.replace(',', '', regex=True)
    if remove_dollar:
        series = series.str.replace(r'\$', '', regex=True)
    series = series.str.replace(r'[()]', '', regex=True)
    return pd.to_numeric(series, errors='coerce')

class TestNasdaqEarningsData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Ensures the directory exists before running tests.
        """
        if not os.path.exists(NASDAQ_EARNINGS_DATA_FOLDER):
            os.makedirs(NASDAQ_EARNINGS_DATA_FOLDER)

    def test_earnings_data_validity(self):
        """
        Checks that Market Cap and EPS Forecast values are within reasonable ranges for all CSV files.
        """
        # Get list of all CSV files
        csv_files = [f for f in os.listdir(NASDAQ_EARNINGS_DATA_FOLDER) if f.endswith('.csv')]

        if not csv_files:
            self.skipTest("No CSV files found in data_upload/nasdaq_earnings_upload/")

        all_passed = True  # Track if all tests pass
        passed_tests = []  # Store filenames that passed validation

        for file in csv_files:
            file_path = os.path.join(NASDAQ_EARNINGS_DATA_FOLDER, file)
            df = pd.read_csv(file_path)

            # Convert columns to numeric
            df['Market Cap'] = clean_numeric_column(df['Market Cap'])
            df['EPS Forecast'] = clean_numeric_column(df['EPS Forecast'], remove_dollar=True)

            # Check for missing values (warn but do not fail)
            if df[['Market Cap', 'EPS Forecast']].isnull().any().any():
                print(f"Warning: NaN values detected in {file}")

            # Exclude NaN values before checking for invalid ranges
            invalid_market_cap = df[df['Market Cap'].notna() & ~(df['Market Cap'].between(MARKET_CAP_MIN, MARKET_CAP_MAX, inclusive="both"))]
            invalid_eps_forecast = df[df['EPS Forecast'].notna() & ~(df['EPS Forecast'].between(EPS_FORECAST_MIN, EPS_FORECAST_MAX, inclusive="both"))]

            # If no invalid values, mark file as passed
            if invalid_market_cap.empty and invalid_eps_forecast.empty:
                passed_tests.append(file)
            else:
                all_passed = False
                self.assertTrue(invalid_market_cap.empty, f"Unreasonable Market Cap values found in {file}:\n{invalid_market_cap[['Company Name', 'Symbol', 'Market Cap']]}")
                self.assertTrue(invalid_eps_forecast.empty, f"Unreasonable EPS Forecast values found in {file}:\n{invalid_eps_forecast[['Company Name', 'Symbol', 'EPS Forecast']]}")

        # Summary of passed test cases
        if all_passed:
            print("\n All tests passed! The following files passed validation:")
            for test in passed_tests:
                print(f"  - {test}")

if __name__ == "__main__":
    unittest.main()
