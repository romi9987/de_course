import os
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
#BigQueryInsertJobOperator: Executes SQL queries in Google BigQuery as tasks.

from airflow.providers.google.cloud.hooks.gcs import GCSHook
#GCSHook: A helper class to interact with Google Cloud Storage (GCS). Uploads Parquet files to GCS.

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
    "GCP_ingestion_green",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    catchup=True, 
    max_active_runs=1,
)

# Variables:
url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

# url_template: This template generates the download URL for the .csv.gz file. 
# The {{ execution_date.strftime('%Y-%m') }} dynamically inserts the YYYY-MM format of the execution date into the URL.
url_template = url_prefix + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'

# file_template_csv_gz: This template generates the file name for the compressed .csv.gz file. 
# For example "output_2019_01.csv.gz"
file_template_csv_gz = airflow_home + '/output_{{ execution_date.strftime(\'%Y_%m\') }}.csv.gz'

# file_template_csv: This template generates the file name for the decompressed .csv file. 
# For example "output_2019_01.csv"
file_template_csv = airflow_home + '/output_{{ execution_date.strftime(\'%Y_%m\') }}.csv'

# file_template_parquet: This template generates the file name for the .parquet file. For example "output_2019_01.parquet"
file_template_parquet = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.parquet'

# Table Name Template: This creates a template for the BigQuery table name. For example "yellow_taxi_2025_01"
table_name_template = 'green_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
consolidated_table_name = "green_{{ execution_date.strftime(\'%Y\') }}"

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

# This task uses the BigQueryInsertJobOperator to execute a SQL query in BigQuery.
# The query creates the table green_2019 in the dataset specified, if it doesn't already exist.
# The table is created using a CREATE TABLE IF NOT EXISTS statement to ensure it is only created once.
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
                        lpep_pickup_datetime TIMESTAMP,
                        lpep_dropoff_datetime TIMESTAMP,
                        store_and_fwd_flag STRING,
                        RatecodeID INT64,
                        PULocationID INT64,
                        DOLocationID INT64,
                        passenger_count INT64,
                        trip_distance FLOAT64,
                        fare_amount FLOAT64,
                        extra FLOAT64,
                        mta_tax FLOAT64,
                        tip_amount FLOAT64,
                        tolls_amount FLOAT64,
                        ehail_fee FLOAT64,
                        improvement_surcharge FLOAT64,
                        total_amount FLOAT64,
                        payment_type INT64,
                        trip_type INT64,
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

# This task creates an external BigQuery table for a given month from the parquet file in GCS. 
# The external table references the raw data in GCS, using parquet file and specifies the format as PARQUET. 
# This task uses the BigQueryInsertJobOperator to run another SQL query

    create_external_table_task = BigQueryInsertJobOperator(
        task_id="create_external_table",
        gcp_conn_id="gcp-airflow",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{bigquery_dataset}.{table_name_template}_ext`
                    (
                        VendorID INT64,
                        lpep_pickup_datetime TIMESTAMP,
                        lpep_dropoff_datetime TIMESTAMP,
                        store_and_fwd_flag STRING,
                        RatecodeID INT64,
                        PULocationID INT64,
                        DOLocationID INT64,
                        passenger_count INT64,
                        trip_distance FLOAT64,
                        fare_amount FLOAT64,
                        extra FLOAT64,
                        mta_tax FLOAT64,
                        tip_amount FLOAT64,
                        tolls_amount FLOAT64,
                        ehail_fee FLOAT64,
                        improvement_surcharge FLOAT64,
                        total_amount FLOAT64,
                        payment_type INT64,
                        trip_type INT64,
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

# This task creates a temporary native table in BigQuery for a given month by reading from the external table created earlier. 
# It generates a unique unique_row_id for each record by hashing certain fields, and stores the file name for reference.

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
                            COALESCE(CAST(lpep_pickup_datetime AS STRING), ""),
                            COALESCE(CAST(lpep_dropoff_datetime AS STRING), ""),
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
# This task performs a merge operation to update the final table (green_2022) with data from the temporary table. 
# It inserts records into the final table where there is no match (based on the unique_row_id), ensuring that only new 
# or updated data is added. This task uses the BigQueryInsertJobOperator to perform a MERGE SQL operation.

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
                        INSERT (unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge)
                        VALUES (S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime, S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count, S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee, S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge);
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