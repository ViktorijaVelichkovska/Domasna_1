import os
import time
from datetime import date
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from data_processing import fetch_decade_data, fetch_incomplete_data, retrieve_last_record_date

def fil1(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    select_menu = soup.find('select', class_='form-control')
    company_options = [option.get_text(strip=True) for option in select_menu.find_all('option')]

    # Filter alphabetic company names only
    company_names = [name for name in company_options if name.isalpha()]
    return company_names

def fil2(company_list):
    last_dates = []
    for company in company_list:
        last_dates.append(retrieve_last_record_date(company))
    return last_dates

def fil3(companies_with_dates):
    collected_data = []
    os.makedirs('all_data', exist_ok=True)
    print("Starting data scraping process...")

    def process_company_data(company):
        company_name, last_update = company
        print(f"Processing company: {company_name}")
        start_time = time.time()

        if last_update is None:
            collected_data.append(fetch_decade_data(company_name))
        elif last_update != date.today():
            fetch_incomplete_data(company_name, last_update)

        print(f"Finished processing {company_name} in {time.time() - start_time:.2f} seconds")

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_company_data, companies_with_dates)

    return collected_data

if __name__ == '__main__':
    start_time = time.time()

    #fetch company names
    company_url = "https://www.mse.mk/mk/stats/symbolhistory/KMB"
    company_list = fil1(company_url)
    print("Company names fetched:", company_list)

    #last update dates for each company
    companies_with_dates = fil2(company_list)

    #scrape data based on last update dates
    scraped_data = fil3(companies_with_dates)

    #convert the collected data to a DataFrame
    data_df = pd.DataFrame(scraped_data)
    print(data_df)

    print(f"Total execution time: {time.time() - start_time:.2f} seconds")
    print(f"Total data entries scraped: {len(scraped_data)}")

