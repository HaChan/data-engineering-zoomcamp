import os
import glob
import csv
import time
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import text
from concurrent.futures import ThreadPoolExecutor, as_completed

db_username = 'postgres'
db_password = 'postgres'
db_host = 'localhost'
db_port = '5432'
db_name = 'dev'
schema_name = 'ny_tripdata'  # Your PostgreSQL schema
directory = "data/ny_taxi"

engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

def copy_from(table, conn, keys, data_iter, pre_truncate=False, fatal_failure=False):
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)

def import_data(file, prefix, db_name, date_cols):
    start_time = time.time()
    print(f"Start import {file}")
    df_iter = pd.read_csv(file, chunksize=100_000, parse_dates=date_cols, low_memory=False,
                          dtype={'VendorID': 'Int64', 'passenger_count': 'Int64',
                                 'RatecodeID': 'Int64', 'payment_type': 'Int64'})
    for chunk in df_iter:
        #c = chunk.to_sql(name=db_name, schema=schema_name, con=engine, if_exists="append", method='multi', index=False)
        c = chunk.to_sql(name=db_name, schema=schema_name, con=engine, if_exists="append", method=copy_from, index=False)

        print(f"Finish chunk of {file}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.6f} seconds")
    print(f"Success import {file}")

def import_files(directory, prefix, db_name, date_cols):
    files = glob.glob(os.path.join(directory, prefix + "*"))
    for file in files:
        import_data(file, prefix, db_name, date_cols)

def import_concurrent(directory, prefix, db_name, date_cols):
    files = glob.glob(os.path.join(directory, prefix + "*"))
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(import_data, csv_file, prefix, db_name, date_cols)
            for csv_file in files]

        # Wait for all futures to complete
        for future in as_completed(futures):
            # Handle exceptions or errors if needed
            pass

#import_data("data/ny_taxi/yellow_tripdata_2019-08.csv.gz", "yellow_tripdata", "yellow_tripdata", ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
#import_files(directory, "yellow_tripdata", "yellow_tripdata", ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
#import_concurrent(directory, "yellow_tripdata", "yellow_tripdata", ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
#import_concurrent(directory, "green_tripdata", "green_tripdata", ['lpep_pickup_datetime', 'lpep_dropoff_datetime'])
import_concurrent(directory, "fhv_tripdata", "fhv_tripdata", ['pickup_datetime', 'dropOff_datetime'])
