import os
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


with open("/opt/airflow/google/airflow_gcp_secrets.json", "r") as file:
    creds = json.load(file)
project_id=creds["project_id"]
bucket=os.getenv('GCP_GCS_BUCKET')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Defining the DAG
upload_workflow = DAG(
    "GCP_upload_yellow",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2024, 2, 1),
    end_date=datetime(2024, 7, 1),
    catchup=True, 
    max_active_runs=1,
)

output_file_template = 'yellow_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}.parquet'
url_template = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file,
        timeout=600
    )


with upload_workflow:
    
    wget_task = BashOperator(
        task_id="curl",
        bash_command=f"curl -L -o {path_to_local_home}/{output_file_template} {url_template}",
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": bucket,
            "object_name": f"raw/{output_file_template}",
            "local_file": f"{path_to_local_home}/{output_file_template}",
            "gcp_conn_id": "gcp-airflow"
        },
    )

    remove_task = BashOperator(
        task_id="remove_file",
        bash_command=f"rm {path_to_local_home}/{output_file_template}"
    )

    wget_task >> upload_task >> remove_task


    











