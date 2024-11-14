import pandas as pd
from filters import fil1, fil2, fil3

def data_processing(input_data):
    data = fil1(input_data)
    print(data)

    data = fil2(data)
    print(data)

    data = fil3(data)
    return data

if __name__ == "__main__":

    data = data_processing("https://www.mse.mk/mk/stats/symbolhistory/KMB")

    print(f"Number of new companies: {len(data)}")
    data_in_total = 0

    for company_code, company_data in data:
        data_in_total = data_in_total + len(company_data)
        df = pd.DataFrame(company_data)
        df.to_csv(f'all_data/{company_code}.csv', index=False)


    print(f"Number of new data scraped: {data_in_total} (rows)")
