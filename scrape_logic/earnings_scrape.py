import requests
import csv
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import configuration

def fetch_alpha_csv(url, output_filename):
    response = requests.get(url)
    data = response.text

    if not os.path.exists(configuration.DATA_FOLDER):
        os.makedirs(configuration.DATA_FOLDER, exist_ok=True)

    output_filepath = os.path.join(configuration.DATA_FOLDER, output_filename)

    with open(output_filepath, 'w') as file:
        file.write(data)

    print("Data saved to earnings_calendar.csv")

def main(url, api_key):
    validated_url = url + api_key
    output_filename = configuration.ALPHA_FILENAME
    fetch_alpha_csv(validated_url, output_filename)
    
if __name__ == "__main__":
    api_key = configuration.ALPHA_API_KEY
    url = configuration.ALPHA_URL
    
    main(url, api_key)
