import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:9876/ny_taxi')

green_csv = "../data/green_tripdata_2019-09.csv"
df = pd.read_csv(green_csv, nrows=10)
df_iter = pd.read_csv(green_csv, iterator=True, chunksize=10_000)

df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

green_table = "green_taxi_data"
df.head(n=0).to_sql(name=green_table, con=engine, if_exists="replace")

print("Import green taxi data")

for chunk in df_iter:
    chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
    chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
    chunk.to_sql(name=green_table, con=engine, if_exists="append")
    print("Next green taxi chunk")

print("=========" * 2)
# Zone import
zone_csv = "taxi+_zone_lookup.csv"
zone_df = pd.read_csv(zone_csv, nrows=10)
zone_df_iter = pd.read_csv(zone_csv, iterator=True, chunksize=10_000)

zone_table = "taxi_zone_lookup"
zone_df.head(n=0).to_sql(name=zone_table, con=engine, if_exists="replace")

for chunk in zone_df_iter:
    chunk.to_sql(name=zone_table, con=engine, if_exists="append")
    print("Next zone chunk")
