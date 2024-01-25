import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:9876/ny_taxi')

df = pd.read_csv("data/yellow_tripdata_2021-01.csv", nrows=10)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")

df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize=10_000)

while True:
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

    print("next chunk")
