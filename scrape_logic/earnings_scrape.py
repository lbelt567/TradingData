import requests
import os

DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_upload'))

ALPHA_API_KEY = os.getenv('ALPHA_API_KEY', '')
ALPHA_URL = os.getenv(
    'ALPHA_URL',
    'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey=',
)
ALPHA_FILENAME = 'earnings_calendar.csv'


def fetch_alpha_csv(url, output_filename):
    response = requests.get(url)
    data = response.text

    os.makedirs(DATA_FOLDER, exist_ok=True)

    output_filepath = os.path.join(DATA_FOLDER, output_filename)

    with open(output_filepath, 'w') as file:
        file.write(data)

    print("Data saved to earnings_calendar.csv")


def main(url, api_key):
    validated_url = url + api_key
    fetch_alpha_csv(validated_url, ALPHA_FILENAME)


if __name__ == "__main__":
    main(ALPHA_URL, ALPHA_API_KEY)
