from fileinput import filename
import os
import json
import requests
import pandas as pd
import boto3
from prefect import task, Flow
from prefect.schedules import CronSchedule
from pipeline_secrets import s3_access_key_id, s3_secret_access_key, s3_bucket, census_key


CENSUS_KEY = os.environ.get('CENSUS_KEY')

# Make a request to the US Census API and retrive the request response content
@task
def extract(url: str) -> dict:
    res = requests.get(url)
    # print(res.content)
    if not res:
        raise Exception('No data fetched!')
    return json.loads(res.content)


# Transform the nested list data response content to a dataframe, filtering out unneeded metrics
@task
def transform(data: dict) -> pd.DataFrame:
    transformed = []
    for record in data[1:]:
        transformed.append({
            'State': record[0],
            'Population in Poverty': record[1],
            'Population in Poverty (Margin of Error)': record[2],
            'Poverty Rate (Rate Estimate)': record[3],
            'Year': record[4]
        })
    # print(pd.DataFrame(transformed))
    return pd.DataFrame(transformed)


# Load the dataframe into CSV format
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
        s3_client.upload_file(path, s3_bucket, f'us-census-small-area-poverty-data/{filename}')
        print('File successfully loaded')
    except:
        print('Client error: file could not be uploaded to S3')
        raise error


scheduler = CronSchedule(
    cron='* * * * *'  # Example state, appropriate cron interval for this dataset would be 1 year, on January 1st -- cron='0 0 1 1 *'
)


# Create the flow to begin extracting, transforming, and loading the data from the API requests into CSVs
def prefect_flow():
    with Flow(name='small-area-poverty-data-pipeline', schedule=scheduler) as flow:
        param_url = 'https://api.census.gov/data/timeseries/poverty/saipe'
        for year in range(2000, 2017):
            fiscal_records = extract(url=param_url + f'?get=NAME,SAEPOVALL_PT,SAEPOVALL_MOE,SAEPOVRTALL_MOE,SAEPOVRTALL_PT&for=state:*&time={year}&key={census_key}')
            df_fiscal_data = transform(fiscal_records)
            load(data=df_fiscal_data, path=f'data/small-area-poverty-data/poverty_small-area_{year}.csv')
    return flow


# Run the script, extracting, transforming, and loading the data from the API requests into CSVs
if __name__ == '__main__':
    census_sa_flow = prefect_flow()
    census_sa_flow.run()

# (Optional) Run Saturn <> Prefect Integration Registration to provision Dask cluster
#     census_sa_flow = integration.register_flow_with_saturn(
#     flow=census_sa_flow,
#     dask_cluster_kwargs={
#         "n_workers": 3,
#         "worker_size": "xlarge",
#         "scheduler_size": "medium",
#         "worker_is_spot": False,
#     },
# )
#     census_sa_flow.register(project_name=PREFECT_CLOUD_PROJECT_NAME)