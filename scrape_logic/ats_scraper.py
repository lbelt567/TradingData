import datetime
import holidays
import requests
import base64
import json
import pandas as pd
import os

# FINRA API Credentials
FIP_TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"
GRANT_TYPE = "client_credentials"
CLIENT_ID = "f8fcc6142ec9417ba441"       # Replace with actual credentials
CLIENT_SECRET = "Harm0708#Harm0708#"  # Replace with actual credentials

BASE_URL = "https://api.finra.org"
GROUP_NAME = "otcMarket"
DATASET_NAME = "weeklySummary"
DATA_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'data_upload', 'ats_upload')


def get_first_business_days(start_date, end_date):
    """Get the first business days (Monday or next business day if Monday is a holiday)."""
    us_holidays = holidays.US()
    first_days = []
    current_date = start_date

    while current_date <= end_date:
        if current_date.weekday() == 0:  # Monday
            first_business_day = current_date
            if first_business_day in us_holidays:
                first_business_day += datetime.timedelta(days=1)  # Move to Tuesday if Monday is a holiday
            first_days.append(first_business_day.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=7)  # Move to next Monday

    return first_days


def get_finra_access_token():
    """Fetch FINRA OAuth2 access token."""
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth_base64 = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(FIP_TOKEN_URL, headers=headers, data={"grant_type": GRANT_TYPE})
    response.raise_for_status()
    return response.json().get("access_token")


def fetch_finra_data(access_token, filter_date):
    """Fetch FINRA OTC market data for a given filter date."""
    endpoint_url = f"{BASE_URL}/data/group/{GROUP_NAME}/name/{DATASET_NAME}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    MAX_RECORDS = 100000000
    BATCH_SIZE = 5000
    all_records = []
    offset = 0

    while len(all_records) < MAX_RECORDS:
        payload = {
            "limit": BATCH_SIZE,
            "offset": offset,
            "compareFilters": [
                {"compareType": "EQUAL", "fieldName": "initialPublishedDate", "fieldValue": filter_date}
            ]
        }

        response = requests.post(endpoint_url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"No more data for {filter_date}.")
            break

        all_records.extend(data)

        if len(data) < BATCH_SIZE:
            print(f"Last page reached for {filter_date}. Stopping pagination.")
            break

        offset += BATCH_SIZE

    return all_records


def save_data_to_csv(data, filter_date):
    """Save fetched data to a CSV file in the data_upload folder."""
    if not os.path.exists(DATA_UPLOAD_FOLDER):
        os.makedirs(DATA_UPLOAD_FOLDER)

    csv_filename = os.path.join(DATA_UPLOAD_FOLDER, f"filtered_weekly_otc_data_{filter_date}.csv")
    df = pd.DataFrame(data)
    df.to_csv(csv_filename, index=False)
    print(f"Data saved to {csv_filename}. Total records: {len(df)}")


def main():
    """Main function to fetch and save OTC market data."""
    start_date = datetime.date(2022, 2, 28)

    # Calculate this week's Monday
    today = datetime.date.today()
    this_monday = today - datetime.timedelta(days=(today.weekday()))  # Moves back to the most recent Monday

    end_date = this_monday
    filter_dates = get_first_business_days(start_date, end_date)
    filter_date = filter_dates[-1]

    access_token = get_finra_access_token()
    print("Access token obtained.")

    print(f"Fetching data for {filter_date}...")
    data = fetch_finra_data(access_token, filter_date)
    if data:
        save_data_to_csv(data, filter_date)
    else:
        print(f"No data found for {filter_date}.")

    # Load last saved file for verification
    last_csv = os.path.join(DATA_UPLOAD_FOLDER, f"filtered_weekly_otc_data_{filter_dates[-1]}.csv")
    df = pd.read_csv(last_csv)
    print(df.head())

if __name__ == "__main__":
    main()
