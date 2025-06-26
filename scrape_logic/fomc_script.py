import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import configuration

def fetch_html(url):
    """
    Function to fetch the html response for a given url using requests
    """
    response = requests.get(url)
    response.raise_for_status
    return response.content

def parse_html(html_content):
    """
    Function to prepare the html contents as a BeautifulSoup obj
    """
    return BeautifulSoup(html_content, 'html.parser')

def extract_fomc_meeting_dates(soup):
    """
    Buisness logic to parse data for fomc given BeautifulSoup obj
    """
    
    meetings = []

    #First level
    article_div = soup.find('div', {'id': 'article'})
    if not article_div:
        print("Main article section not found.")
        return meetings

    #Itterate through div header sections
    current_year = None
    for section in article_div.find_all('div', class_='panel panel-default'):

        #Get year
        year_heading = section.find('div', class_='panel-heading')
        if year_heading:
            year_link = year_heading.find('a')
            if year_link and year_link.text.strip().endswith('FOMC Meetings'):
                current_year = year_link.text.strip().split()[0]

        #Get fomc meeting information for each row
        meeting_rows = section.find_all('div', class_=['row fomc-meeting', 'fomc-meeting--shaded row fomc-meeting'])
        for row in meeting_rows:
            month_div = row.find('div', class_='fomc-meeting__month')
            month = month_div.get_text(strip=True) if month_div else None

            date_div = row.find('div', class_='fomc-meeting__date')
            date = date_div.get_text(strip=True) if date_div else None

            transcript_div = row.find('div', class_='fomc-meeting__minutes')
            html_link = None
            release_date = None

            if transcript_div:
                html_link_tag = transcript_div.find('a', href=True, string="HTML")
                if html_link_tag:
                    html_link = 'https://www.federalreserve.gov/' + html_link_tag['href']
                
                release_text = transcript_div.get_text(strip=True)
                if "Released" in release_text:
                    release_date = release_text.split("Released")[-1].strip()[:-1].strip().strip('"')

            #Check for flags in rows and set in table
            has_summary_projections = False
            is_unscheduled = False
            is_cancelled = False
            is_notation_vote = False

            if date:
                if '*' in date:
                    has_summary_projections = True
                    date = date.replace('*', '').strip()

                if 'unscheduled' in date.lower():
                    is_unscheduled = True
                    date = date.lower().replace('(unscheduled)', '').strip()
                elif 'cancelled' in date.lower():
                    is_cancelled = True
                    date = date.lower().replace('(cancelled)', '').strip()
                elif 'notation vote' in date.lower():
                    is_notation_vote = True
                    date = date.lower().replace('(notation vote)', '').strip()

            #Append fomc object to append
            if current_year and month and date:
                meetings.append({
                    'Year': current_year,
                    'Month': month,
                    'Dates': date,
                    'Summary of Economic Projections': has_summary_projections,
                    'Unscheduled': is_unscheduled,
                    'Cancelled': is_cancelled,
                    'Notation Vote': is_notation_vote,
                    'Transcript HTML Link': html_link,
                    'Transcript Release Date': release_date
                })

    return meetings


def save_to_csv(meetings, output_filename):
    """
    Save fomccalender to a dataframe and create csv
    """
    if not os.path.exists(configuration.DATA_FOLDER):
        os.makedirs(configuration.DATA_FOLDER)

    # Define full output file path using the corrected absolute path
    output_filepath = os.path.join(configuration.DATA_FOLDER, output_filename)

    # Save the CSV
    df = pd.DataFrame(meetings)
    df.to_csv(output_filepath, index=False)


def main(url, output_filename):
    html_content = fetch_html(url)
    soup = parse_html(html_content)
    meetings = extract_fomc_meeting_dates(soup)

    if meetings:
        save_to_csv(meetings, output_filename)
        print(f"Data has been successfully saved to {output_filename}")
    else:
        print("No meeting data found")

if __name__ == "__main__":
    url = configuration.FOMC_URL
    output_filename = configuration.FOMC_FILENAME
    main(url, output_filename)