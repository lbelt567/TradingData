import requests
import datetime
import csv
import os
from pprint import pprint

# Nasdaq API URL
API_URL = "https://api.nasdaq.com/api/calendar/earnings"

# Headers to mimic a real browser request
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Data Upload Directory (Inside data_upload/nasdaq_earnings_upload)
DATA_UPLOAD_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_upload', 'nasdaq_earnings_upload'))

# Ensure the directory exists before writing files
os.makedirs(DATA_UPLOAD_FOLDER, exist_ok=True)

def get_current_dates():
    """Returns today's date and current timestamp."""
    today_date = datetime.date.today().strftime("%Y-%m-%d")
    scraped_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return today_date, scraped_at

def fetch_earnings_data(date):
    """Fetches earnings data from Nasdaq API for a given date."""
    print(f"Fetching earnings data for {date}...")

    try:
        response = requests.get(url=API_URL, headers=HEADERS, params={"date": date})
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("rows", [])
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")  # Catch any other unexpected exceptions
        return None 

def save_earnings_to_csv(earnings_list, date, scraped_at):
    """Saves earnings data to a CSV file inside data_upload/nasdaq_earnings_upload."""
    csv_filename = os.path.join(DATA_UPLOAD_FOLDER, f"nasdaq_earnings_{date}.csv")

    if not earnings_list:
        print(f"No earnings data available for {date}. Skipping file update.")
        return  # Avoid creating an empty file

    # Check if file exists to determine whether to append or write a new file
    file_exists = os.path.isfile(csv_filename)

    with open(csv_filename, mode='a' if file_exists else 'w', newline='') as file:
        writer = csv.writer(file)

        # Write the header only if the file is new
        if not file_exists:
            writer.writerow([
                "Company Name", "Symbol", "Market Cap", "EPS Forecast", "Time", 
                "Fiscal Quarter Ending", "No. of ESTs", "Last Year's EPS", "Scraped At"
            ])

        for earnings in earnings_list:
            writer.writerow([
                earnings.get("name", ""),
                earnings.get("symbol", ""),
                earnings.get("marketCap", ""),
                earnings.get("epsForecast", ""),
                earnings.get("time", ""),
                earnings.get("fiscalQuarterEnding", ""),
                earnings.get("noOfEsts", ""),
                earnings.get("lastYearEPS", ""),
                scraped_at
            ])

    print(f"Earnings data saved to {csv_filename}")

def main():
    """Main function to fetch and store Nasdaq earnings data."""
    today_date, scraped_at = get_current_dates()
    earnings_list = fetch_earnings_data(today_date)

    if earnings_list:
        save_earnings_to_csv(earnings_list, today_date, scraped_at)
    else:
        print("No earnings data available for today.")

    pprint(earnings_list)

if __name__ == "__main__":
    main() 
