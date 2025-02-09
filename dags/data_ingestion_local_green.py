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
    "local_ingestion_green",
    schedule_interval="0 6 2 * *", #will run at 06:00 (6 AM) on the 2nd day of every month
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 3, 28),
)

# url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

# we use jinja template {{  }} execution_date is a timestamp
# we use strftime('%Y-%m') to use format year-month and we use \ to escape ' character
url_template = url_prefix + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'

file_template_csv_gz = airflow_home + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
file_template_csv = airflow_home + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

table_name_template = 'green_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

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