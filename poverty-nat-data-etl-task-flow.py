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
            'Country': record[0],
            'Percent Population in Poverty': record[1],
            'Year': record[2],
            'Race / Ethnicity': record[3]
        })
    # print(pd.DataFrame(transformed))

    # Transform the "Race / Ethnicity" key for each dataframe element to their human-readable names in the US Census legend
    pov_df = pd.DataFrame(transformed)
    pov_df.loc[pov_df["Race / Ethnicity"] == "1", "Race / Ethnicity"] = "All"
    pov_df.loc[pov_df["Race / Ethnicity"] == "2", "Race / Ethnicity"] = "White"
    pov_df.loc[pov_df["Race / Ethnicity"] == "3", "Race / Ethnicity"] = "White"
    pov_df.loc[pov_df["Race / Ethnicity"] == "4", "Race / Ethnicity"] = "White"
    pov_df.loc[pov_df["Race / Ethnicity"] == "5", "Race / Ethnicity"] = "White"
    pov_df.loc[pov_df["Race / Ethnicity"] == "6", "Race / Ethnicity"] = "Black"
    pov_df.loc[pov_df["Race / Ethnicity"] == "7", "Race / Ethnicity"] = "Black"
    pov_df.loc[pov_df["Race / Ethnicity"] == "8", "Race / Ethnicity"] = "Black"
    pov_df.loc[pov_df["Race / Ethnicity"] == "9", "Race / Ethnicity"] = "Asian"
    pov_df.loc[pov_df["Race / Ethnicity"] == "10", "Race / Ethnicity"] = "Asian"
    pov_df.loc[pov_df["Race / Ethnicity"] == "11", "Race / Ethnicity"] = "Asian"
    pov_df.loc[pov_df["Race / Ethnicity"] == "12", "Race / Ethnicity"] = "Hispanic"
    print(pov_df)
    return pov_df


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
        s3_client.upload_file(path, s3_bucket, f'us-census-natl-poverty-data/{filename}')
        print('File successfully loaded')
    except:
        print('Client error: file could not be uploaded to S3')
        raise error


scheduler = CronSchedule(
    cron='* * * * *'  # Example state, appropriate cron interval for this dataset would be 1 year, on January 1st -- cron='0 0 1 1 *'
)


# Create the flow to begin extracting, transforming, and loading the data from the API requests into CSVs
def prefect_flow():
    with Flow(name='natl-poverty-data-pipeline', schedule=scheduler) as flow:
        param_url = "https://api.census.gov/data/timeseries/poverty/histpov2"
        for year in range(1959, 2021):
            natl_poverty_records = extract(url=param_url + f'?get=NAME,PCTPOV,YEAR,RACE&for=us:*&time={year}&key={census_key}')
            df_natl_poverty_records = transform(natl_poverty_records)
            load(data=df_natl_poverty_records, path=f'data/natl-poverty-data/poverty-rate_natl_{year}.csv')
    return flow


# Run the flow script
if __name__ == '__main__':
    census_natl_flow = prefect_flow()
    census_natl_flow.run()

# (Optional) Run Saturn <> Prefect Integration Registration to provision Dask cluster
#     census_natl_flow = integration.register_flow_with_saturn(
#     flow=census_natl_flow,
#     dask_cluster_kwargs={
#         "n_workers": 3,
#         "worker_size": "xlarge",
#         "scheduler_size": "medium",
#         "worker_is_spot": False,
#     },
# )
#     census_natl_flow.register(project_name=PREFECT_CLOUD_PROJECT_NAME)