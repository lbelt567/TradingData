import os

DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "data_upload"))

FOMC_URL = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
FOMC_FILENAME = 'fomc_meeting_times.csv'

#Eventually store in secrets manager
ALPHA_API_KEY = 'C7DEHKVVQH4SDSTX'
ALPHA_URL = 'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey='
ALPHA_FILENAME = 'earnings_calendar.csv'