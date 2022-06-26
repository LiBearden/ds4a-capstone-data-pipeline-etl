# DS4A Data Engineering Capstone: ETL Pipeline and Data Warehouse Deployment
 ETL pipelines and data warehouse construction and orchestration for the DS4A Data Engineering capstone project.

## Overview
For this section of the capstone project, I have used the following resources / tools to build our data pipelines and set up our data warehouse:
- Python 3
- Prefect
- AWS S3
- AWS CloudFormation Stacks
- Databricks Workspaces + Datawarehouse
- Dask (on the Saturn Cloud Data Science platform)

## Pipeline Orchestration Process

### Prerequisites
- Prefect account
- Saturn Cloud Account
- AWS Account
- Databricks Account


### Environment Setup

The code in this example uses prefect for orchestration (figuring out what to do, and in what order) and Dask Cluster for execution (doing the things).

While Prefect is our orchestration tool of choice and our Dask cluster has been configured to execute those orchestration tasks, we still have to set up our system environment to be able to run the scripts that define those tasks, as shown below:

Create a new conda virtual environment and activate it:
```
$ conda create —n data_pipeline_env python=3.9
```

And activate it:
```
$ conda activate data_pipeline_env
```

Then, install the project dependencies:
```
$ pip install -r requirements.txt
```

And be sure to set environment variables to instantiate our "read-safe" credentials for our Saturn Cloud account:
```
$ export SATURN_USERNAME='SATURN_USERNAME'
```

Finally, we authenticate our Prefect account with our API key secret:
```
$ prefect auth login --key PREFECT_API_KEY
```

### Prefect Cloud Project Management

Since Prefect organizes flows in association with projects, I created a project called `"data-pipeline-warehouse-t23"` within my account, and initialized the prefect client within the Saturn setup script in preparation for the upcoming tasks.

```python
client = prefect.Client()
client.create_project(project_name=PREFECT_CLOUD_PROJECT_NAME)
integration = PrefectCloudIntegration(prefect_cloud_project_name=PREFECT_CLOUD_PROJECT_NAME)
```

<hr>

### Data Pipeline Tasks (Per Dataset)

Prefect organizes groups of tasks into "flows" (workflows), which must be defined programmatically by defining functions -- the following is a breakdown of each task that we have defined for the scripts below (specifying the extraction, transformation, and loading of the data running from APIs through our pipeline):

- `extract`: Retrieving the data needed from the respective API
- `transform`: Transforming the returned response from the API into a dataframe that matches our warehouse table schemas
- `load`: Loading the data into CSVs locally, and into S3 buckets that as synchronized to our Databricks Workspace + Warehouse Cluster

The integration sections that are commented out instantiate the flows in in the Saturn <> Prefect integration, and then pass the scheduling task on to the Cloud for execution by our Prefect project.

#### (Optional) Local Directory Setup for CSV Generation
For the sake of demonstration, you can also create some local directories to house the resultant CSV files in:
```
$ mkdir -p data/treasury-data
$ mkdir -p data/natl-poverty-data
$ mkdir -p data/small-area-poverty-data
```

## Review

### Prefect Flows
#### Output
When the Prefect flows are successfully created and scheduled in the cloud (via the commented code for each script above), the output will look like this:

##### Treasury Dataset
![Treasury Financial Flow](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/treasury_finanical-flow.png)
##### National Poverty Dataset
![Census National Poverty Flow](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/census_natl_poverty-flow.png)
##### Small Area Poverty Dataset
![Census Small Area Poverty Flow](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/census_sa_poverty-flow.png)

#### In the Project Dashboard (Cloud)
In the Prefect cloud dashboard, the flows will appear like so:
![Prefect Dashboard Flows](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/prefect_dashboard_flows.png)

### AWS Cloud Formation -> Databricks Workspace + Warehouse
Our S3 bucket is linked to our CloudFormation stack, which is housing our Databricks Workspace and Warehouse:
##### AWS Cloud Formation
![AWS Cloud Formation](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/AWS_databricks_warehouse_stack_updated.png)

##### Databricks Workspace + Warehouse
![Databricks](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/databricks_dash_updated.png)

##### Table in Databricks Warehouse
![Warehouse Table](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/databricks_warehouse_table_view.png)

### Databricks Workspace SQL Dashboard
From the Databricks Workspace, it is easy to access tables via SQL for the entire warehouse via a single unified dashboard:
![Warehouse SQL Dash](https://team23-data-load-documentation.s3.us-east-2.amazonaws.com/notebook-images/data-pipeline-warehouse/databricks_workspace_SQL.png)
