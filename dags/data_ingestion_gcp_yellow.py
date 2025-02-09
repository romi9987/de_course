import os
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pyarrow
import pyarrow.csv
import pyarrow.parquet 


# Make sure the values ​​match your gcp values
with open("/opt/airflow/google/airflow_gcp_secrets.json", "r") as file:
    creds = json.load(file)
project_id=creds["project_id"]
bucket=os.getenv('GCP_GCS_BUCKET')
bigquery_dataset=os.getenv('GCP_BIGQUERY')
airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Defining the DAG
dag = DAG(
    "GCP_ingestion_yellow",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 3, 1),
    catchup=True, 
    max_active_runs=1,
)

# Variables
url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
url_template = url_prefix + '/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
file_template_csv_gz = airflow_home + '/output_{{ execution_date.strftime(\'%Y_%m\') }}.csv.gz'
file_template_csv = airflow_home + '/output_{{ execution_date.strftime(\'%Y_%m\') }}.csv'
file_template_parquet = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.parquet'
table_name_template = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
consolidated_table_name = "yellow_{{ execution_date.strftime(\'%Y\') }}"

# Utility functions
def format_to_parquet(src_file):

    table = pyarrow.csv.read_csv(src_file)

    # change ehail_fee to float64
    if 'ehail_fee' in table.column_names:
        # Convertir la columna 'ehail_fee' a float (FLOAT64)
        table = table.set_column(
            table.schema.get_field_index('ehail_fee'),
            'ehail_fee',
            pyarrow.array(table['ehail_fee'].to_pandas().astype('float64'))  
        )

    pyarrow.parquet.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file,
        timeout=600
    )


with dag:
# Task 1: Download file
# curl_task: Uses the curl function to download csv.gz file for the specified month.
    curl_task = BashOperator(
        task_id="curl",
        bash_command=f"curl -L -o {file_template_csv_gz} {url_template}",
    ) #To handle the redirect properly use -L flag

# Task 2: Unzip file
    gunzip_task = BashOperator(
        task_id="gunzip",
        bash_command=f"gunzip {file_template_csv_gz}"
    )

# Task 3: Format to parquet
# process_task: Converts the .csv file into Parquet format.
    process_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": file_template_csv
        },
        retries=10,
    )

# Task 4: Upload file to google storage
# local_to_gcs_task: This task uploads the downloaded file from the local machine to Google Cloud Storage (GCS). 
# The upload_to_gcs utility function is used to upload the file to the specified bucket and path ({file_template}).
    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": bucket,
            "object_name": file_template_parquet,
            "local_file": file_template_parquet,
            "gcp_conn_id": "gcp-airflow"
        },
        retries=10,
    )

# Task 5: Create final table
# Final table: After processing the data and ensuring there are no duplicates or inconsistencies, 
# the final data is merged into this table. 
# It represents the cleaned, transformed, and de-duplicated dataset including data from all months.

    create_final_table_task = BigQueryInsertJobOperator(
        task_id="create_final_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{project_id}.{bigquery_dataset}.{consolidated_table_name}`
                    (
                        unique_row_id BYTES,
                        filename STRING,      
                        VendorID INT64,
                        tpep_pickup_datetime TIMESTAMP,
                        tpep_dropoff_datetime TIMESTAMP,
                        passenger_count INT64,
                        trip_distance FLOAT64,
                        RatecodeID INT64,
                        store_and_fwd_flag STRING,  
                        PULocationID INT64,
                        DOLocationID INT64,   
                        payment_type INT64,   
                        fare_amount FLOAT64,
                        extra FLOAT64,
                        mta_tax FLOAT64,
                        tip_amount FLOAT64,
                        tolls_amount FLOAT64,
                        improvement_surcharge FLOAT64,
                        total_amount FLOAT64,
                        congestion_surcharge FLOAT64
                    )    
                """,
                "useLegacySql": False,
            }
        },
        retries=3,
    )

# Task 6: Create external monthly table
# External Table: Serves as the initial point of access to the raw data. 
# The data in this table is not physically stored in BigQuery. There is a External table for each month

    create_external_table_task = BigQueryInsertJobOperator(
        task_id="create_external_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{bigquery_dataset}.{table_name_template}_ext`
                    (
                        VendorID INT64,
                        tpep_pickup_datetime TIMESTAMP,
                        tpep_dropoff_datetime TIMESTAMP,
                        passenger_count INT64,
                        trip_distance FLOAT64,
                        RatecodeID INT64,
                        store_and_fwd_flag STRING,  
                        PULocationID INT64,
                        DOLocationID INT64,   
                        payment_type INT64,   
                        fare_amount FLOAT64,
                        extra FLOAT64,
                        mta_tax FLOAT64,
                        tip_amount FLOAT64,
                        tolls_amount FLOAT64,
                        improvement_surcharge FLOAT64,
                        total_amount FLOAT64,
                        congestion_surcharge FLOAT64
                    )
                    OPTIONS (
                        uris = ['gs://{bucket}/{file_template_parquet}'],
                        format = 'PARQUET'
                    );
                """,
                "useLegacySql": False,
            }
        },
        retries=3,
    )

# Task 7: Create native monthly table
# Temporary table: This is a native table created in BigQuery using the data from the external table. 
# Copies the entire dataset from the associated external table into this table, while enriching it 
# with the additional columns unique_row_id and filename. There is a native table for each month.

    create_temp_table_task = BigQueryInsertJobOperator(
        task_id="create_temp_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{project_id}.{bigquery_dataset}.{table_name_template}_tmp`
                    AS
                    SELECT
                        MD5(CONCAT(
                            COALESCE(CAST(VendorID AS STRING), ""),
                            COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
                            COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
                            COALESCE(CAST(PULocationID AS STRING), ""),
                            COALESCE(CAST(DOLocationID AS STRING), "")
                        )) AS unique_row_id,
                        "{file_template_parquet}" AS filename,
                        *
                    FROM `{project_id}.{bigquery_dataset}.{table_name_template}_ext`;
                """,
                "useLegacySql": False,
            }
        },
        retries=3,
    )


# Task 8: Merge
    merge_to_final_table_task = BigQueryInsertJobOperator(
        task_id="merge_to_final_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{project_id}.{bigquery_dataset}.{consolidated_table_name}` T
                    USING `{project_id}.{bigquery_dataset}.{table_name_template}_tmp` S
                    ON T.unique_row_id = S.unique_row_id
                    WHEN NOT MATCHED THEN
                        INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge)
                        VALUES (S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime, S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID, S.DOLocationID, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.improvement_surcharge, S.total_amount, S.congestion_surcharge);
                """,
                "useLegacySql": False,
            }
        },
        retries=3,
    )

# Task 9: Delete local files after upload
    cleanup_task = BashOperator(
        task_id="cleanup_files",
        bash_command=f"rm -f {file_template_csv_gz} {file_template_csv} {airflow_home}/{file_template_parquet}",
    )


curl_task >> gunzip_task >> process_task >> upload_task >> create_final_table_task >> create_external_table_task >> create_temp_table_task >> merge_to_final_table_task >> cleanup_task