FROM apache/airflow:2.10.4-python3.8

# Define Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow

# Use root to create directories and set permissions
USER root
RUN mkdir -p /opt/airflow/shared && chmod -R 777 /opt/airflow/shared

# Ensure Google Cloud secrets JSON is readable
COPY ./conf/airflow_gcp_secrets.json /opt/airflow/google/airflow_gcp_secrets.json
RUN chmod 644 /opt/airflow/google/airflow_gcp_secrets.json

# Switch to the correct Airflow user
USER 50000:0

# Set working directory
WORKDIR $AIRFLOW_HOME

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt