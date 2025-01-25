import os
import argparse

import pandas as pd
from sqlalchemy import create_engine #create engine to connect to database with sqlalchemy


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    # download parquet from:
    # url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    #  and write to csv
    parquet_name = "output.parquet"
    csv_name = "output.csv"
    
    os.system(f"wget {url} -O {parquet_name}")
    df = pd.read_parquet(parquet_name)
    df.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    nyc_taxi_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000) #the data has almost 1400 000 rows, 
    nyc_taxi = next(nyc_taxi_iter)

    nyc_taxi.tpep_pickup_datetime = pd.to_datetime(nyc_taxi.tpep_pickup_datetime) #change the column into timestamp
    nyc_taxi.tpep_dropoff_datetime = pd.to_datetime(nyc_taxi.tpep_dropoff_datetime) #change the column into timestamp

    nyc_taxi.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace") #to create columns
    nyc_taxi.to_sql(name=table_name, con=engine, if_exists="append") #to add rows

    while True: #it throws error when there is no more data
        try:
            nyc_taxi = next(nyc_taxi_iter)
            nyc_taxi.tpep_pickup_datetime = pd.to_datetime(nyc_taxi.tpep_pickup_datetime)
            nyc_taxi.tpep_dropoff_datetime = pd.to_datetime(nyc_taxi.tpep_dropoff_datetime)

            nyc_taxi.to_sql(name=table_name, con=engine, if_exists="append")
        except StopIteration:
            print('completed')
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)
