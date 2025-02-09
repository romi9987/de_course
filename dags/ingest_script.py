import pandas as pd
from sqlalchemy import create_engine #create engine to connect to database with sqlalchemy


def ingest_callable(user, password, host, port, db, table_name, csv_name):

    port = int(port)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    nyc_taxi_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000) #the data has almost 1400 000 rows, 
    nyc_taxi = next(nyc_taxi_iter)

    if 'green' in table_name:
        nyc_taxi.lpep_pickup_datetime = pd.to_datetime(nyc_taxi.lpep_pickup_datetime) #change the column into timestamp
        nyc_taxi.lpep_dropoff_datetime = pd.to_datetime(nyc_taxi.lpep_dropoff_datetime) #change the column into timestamp
    elif 'yellow' in table_name:
        nyc_taxi.tpep_pickup_datetime = pd.to_datetime(nyc_taxi.tpep_pickup_datetime) #change the column into timestamp
        nyc_taxi.tpep_dropoff_datetime = pd.to_datetime(nyc_taxi.tpep_dropoff_datetime) #change the column into timestamp

    nyc_taxi.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace") #to create columns
    nyc_taxi.to_sql(name=table_name, con=engine, if_exists="append") #to add rows

    while True: #it throws error when there is no more data
        try:
            nyc_taxi = next(nyc_taxi_iter)
            if 'green' in table_name:
                nyc_taxi.lpep_pickup_datetime = pd.to_datetime(nyc_taxi.lpep_pickup_datetime)
                nyc_taxi.lpep_dropoff_datetime = pd.to_datetime(nyc_taxi.lpep_dropoff_datetime)
            elif 'yellow' in table_name:
                nyc_taxi.tpep_pickup_datetime = pd.to_datetime(nyc_taxi.tpep_pickup_datetime)
                nyc_taxi.tpep_dropoff_datetime = pd.to_datetime(nyc_taxi.tpep_dropoff_datetime)

            nyc_taxi.to_sql(name=table_name, con=engine, if_exists="append")
        except StopIteration:
            print('completed')
            break


