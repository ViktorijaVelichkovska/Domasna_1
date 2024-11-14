from bs4 import BeautifulSoup
import os
from time import sleep
import requests
from datetime import datetime, timedelta
import pandas as pd


def fetch_decade_data(company_code):
    records = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=364)

    for _ in range(10):  #loop for 10 years of data
        records.extend(query_company_data_for_period(company_code, end_date.strftime('%d.%m.%Y'),
                                                     start_date.strftime('%d.%m.%Y')))
        end_date = start_date - timedelta(days=1)
        start_date = end_date - timedelta(days=365)

    return company_code, records


def fetch_incomplete_data(company_code, last_date):
    records = []
    repeat = True
    last_date = datetime.strptime(last_date, '%d.%m.%Y').date()
    base_url = "https://www.mse.mk/mk/stats/symbolhistory/"
    url = base_url + company_code
    target_date = datetime.now().date()
    start_date = last_date

    while repeat:
        if (target_date - start_date).days >= 364:
            start_date = target_date - timedelta(days=364)
            target_str = target_date.strftime('%Y-%m-%d')
            start_str = start_date.strftime('%Y-%m-%d')
            records.extend(query_company_data_for_period(company_code, target_str, start_str))
        else:
            start_date = last_date
            target_str = target_date.strftime('%Y-%m-%d')
            start_str = start_date.strftime('%Y-%m-%d')
            records.extend(query_company_data_for_period(company_code, target_str, start_str))
            repeat = False

    new_data_df = pd.DataFrame(records)
    existing_data_df = pd.read_csv(f'all_data/{company_code}.csv')
    combined_data = pd.concat([new_data_df, existing_data_df], ignore_index=True)
    combined_data.to_csv(f'all_data/{company_code}.csv', index=False)


def query_company_data_for_period(company_code, end_date, start_date):
    results = []
    base_url = "https://www.mse.mk/mk/stats/symbolhistory/"
    url = base_url + company_code
    json_payload = \
        {
        "FromDate": start_date,
        "ToDate": end_date,
        "Code": company_code,
    }

    response = requests.get(url, json=json_payload)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find('table', id='resultsTable')
    sleep(0.02)

    if table:
        rows = table.find_all('tr')
        for row in rows:
            columns = row.find_all('td')
            if columns:
                entry = {
                    "date": columns[0].text.strip(),
                    "last_transaction_price": columns[1].text.strip(),
                    "max_price": columns[2].text.strip(),
                    "min_price": columns[3].text.strip(),
                    "avg_price": columns[4].text.strip(),
                    "percentage": columns[5].text.strip(),
                    "profit": columns[6].text.strip(),
                    "total_profit": columns[7].text.strip(),
                    "company_code": company_code
                }
                results.append(entry)
    return results


def retrieve_last_record_date(company_code):
    file_path = f'all_data/{company_code}.csv'
    if os.path.exists(file_path):
        data_frame = pd.read_csv(file_path)
        last_recorded_date = data_frame.iloc[0]["date"]
        return company_code, last_recorded_date
    else:
        return company_code, None
