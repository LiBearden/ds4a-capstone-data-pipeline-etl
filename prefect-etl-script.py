import json
import requests
import pandas as pd
from datetime import datetime


def extract(url: str) -> dict:
    res = requests.get(url)
    #print(res.content)
    if not res:
        raise Exception('No data fetched!')
    return json.loads(res.content)


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


# def load(data: pd.DataFrame, path: str) -> None:
#     data.to_csv(path_or_buf=path, index=False)


if __name__ == '__main__':
    fiscal_records = extract(url='https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/title_xii')
    df_users = transform(fiscal_records)
#     load(data=df_users, path=f'data/users_{int(datetime.now().timestamp())}.csv')