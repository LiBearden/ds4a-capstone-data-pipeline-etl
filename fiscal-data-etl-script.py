import json
import requests
import pandas as pd
from datetime import datetime

# Make a request to the US Treasury API and retrive the request response content
def extract(url: str) -> dict:
    res = requests.get(url)
    #print(res.content)
    if not res:
        raise Exception('No data fetched!')
    return json.loads(res.content)

# Transform the JSON data response content to a dataframe, filtering out unneeded metrics
def transform(data: dict) -> pd.DataFrame:
    transformed = []
    for record in data["data"]:
        transformed.append({
            'Record Date': record['record_date'],
            'State': record['state_nm'],
            'Interest Rate Percent': record['interest_rate_pct'],
            'Record Fiscal Year': record['record_fiscal_year'],
            'Record Fiscal Quarter': record['record_fiscal_quarter'],
            'Record Calendar Year': record['record_calendar_year'],
            'Record Calendar Month': record['record_calendar_quarter']
        })
    print(pd.DataFrame(transformed))
    return pd.DataFrame(transformed)


# Load the dataframe into CSV format
def load(data: pd.DataFrame, path: str) -> None:
    data.to_csv(path_or_buf=path, index=False)


# Run the script, extracting, transforming, and loading the data from the API requests into CSVs
if __name__ == '__main__':
    fiscal_urls = ["https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/title_xii?page%5Bnumber%5D=1&page%5Bsize%5D=10000","https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/title_xii?page%5Bnumber%5D=2&page%5Bsize%5D=10000"]
    for i in fiscal_urls:
        fiscal_records = extract(url=f'{i}')
        df_fiscal_data = transform(fiscal_records)
        load(data=df_fiscal_data, path=f'/Users/elliot/dev/ds4a-data-engineering/data-pipeline-warehouse/data-pipeline-etl/data/fiscal_{int(datetime.now().timestamp())}.csv')