Quick hack to load files directly to GCS, without Airflow. 
Downloads csv files from 
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
and uploads them to your Cloud Storage Account as parquet files.

    Install pre-reqs (more info in web_to_gcs.py script)
    Run: python web_to_gcs.py

