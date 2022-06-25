import os
import json
import requests
import pandas as pd
from datetime import datetime
import boto3
from prefect import task, Flow
from prefect.schedules import CronSchedule
from pipeline_secrets import s3_access_key_id, s3_secret_access_key, s3_bucket


# Task: Make a request to the US Treasury API and retrive the request response content
@task
def extract(url: str) -> dict:
    res = requests.get(url)
    # print(res.content)
    if not res:
        raise Exception('No data fetched!')
    return json.loads(res.content)


# Task: Transform the JSON data response content to a dataframe, filtering out unneeded metrics
@task
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
    # print(pd.DataFrame(transformed))
    return pd.DataFrame(transformed)


# Task: Load the dataframe into CSV format
@task
def load(data: pd.DataFrame, path: str) -> None:
    data.to_csv(path_or_buf=path, index=False)
    filename = os.path.basename(path)

    # Create S3 Client to access bucket and upload the created CSV file
    s3_client = boto3.client(
        's3',
        aws_access_key_id = s3_access_key_id,
        aws_secret_access_key = s3_secret_access_key
)
    try:
        s3_client.upload_file(path, s3_bucket, f'us-treasury-financial-data/{filename}')
        print('File successfully loaded')
    except:
        print('Client error: file could not be uploaded to S3')
        raise error

# Set cron job schedule to once per week
scheduler = CronSchedule(
    cron='* * * * *'  # Example state, appropriate cron interval for this dataset would be once a week -- cron='0 0 * * 0'
)


# Create the flow to begin extracting, transforming, and loading the data from the API requests into CSVs
def prefect_flow():
    with Flow(name='treasury-financial-data', schedule=scheduler) as flow:
        param_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/title_xii"

        for year in range(2016, 2023):
            fiscal_records = extract(url=param_url + f'?filter=record_calendar_year:in:({year})&page%5Bnumber%5D=1&page%5Bsize%5D=10000')
            df_fiscal_data = transform(fiscal_records)
            load(data=df_fiscal_data, path=f'data/treasury-data/treasury-fiscal-interest-data_{year}.csv')
    return flow


# Run the flow script
if __name__ == '__main__':
    treasury_flow = prefect_flow()
    treasury_flow.run()

# (Optional) Run Saturn <> Prefect Integration Registration to provision Dask cluster
#     treasury_flow = integration.register_flow_with_saturn(
#     flow=treasury_flow,
#     dask_cluster_kwargs={
#         "n_workers": 3,
#         "worker_size": "xlarge",
#         "scheduler_size": "medium",
#         "worker_is_spot": False,
#     },
# )
#     treasury_flow.register(project_name=PREFECT_CLOUD_PROJECT_NAME)