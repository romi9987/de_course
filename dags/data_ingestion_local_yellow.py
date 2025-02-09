import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# these variables come from the environment variables (env) that come from docker-compose that come from .env file
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    "local_ingestion_yellow",
    schedule_interval="0 6 2 * *", # Schedule: Runs at 6:00 AM on the 2nd day of each month (0 6 2 * *)
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 3, 28),
    catchup=True, # This parameter ensures that if the DAG was paused or missed runs during the period between the start_date 
    # and end_date, Airflow will try to "catch up" and run all the missed executions, one for each scheduled date.
    max_active_runs=1, #This limits the DAG to only have one active run at any time, 
    # preventing overlapping executions of the DAG
)

url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
url_template = url_prefix + '/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
file_template_csv_gz = airflow_home + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
file_template_csv = airflow_home + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
table_name_template = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

with local_workflow:
    
    curl_task = BashOperator(
        task_id="curl",
        bash_command=f"curl -L -o {file_template_csv_gz} {url_template}",
    ) #To handle the redirect properly use -L flag

    gunzip_task = BashOperator(
        task_id="gunzip",
        bash_command=f"gunzip {file_template_csv_gz}"
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user = PG_USER,
            password = PG_PASSWORD,
            host = PG_HOST,
            port = PG_PORT,
            db = PG_DATABASE,
            table_name = table_name_template,
            csv_name = file_template_csv
        ),
    )

    remove_task = BashOperator(
        task_id="remove_file",
        bash_command=f"rm {file_template_csv}"
    )

    curl_task >> gunzip_task >> ingest_task >> remove_task