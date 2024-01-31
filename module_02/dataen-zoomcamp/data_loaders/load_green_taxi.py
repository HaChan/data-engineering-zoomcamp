import io
import pandas as pd
import requests
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(**kwargs) -> DataFrame:
    trip_data = []
    chunksize = 10_000
    for month in [10, 11, 12]:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
        response = requests.get(url)
        content = io.BytesIO(response.content)
        date_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        for chunk in pd.read_csv(content, compression="gzip", chunksize=chunksize, parse_dates=date_cols):
            trip_data.append(chunk)

    return pd.concat(trip_data, ignore_index=True)


@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
