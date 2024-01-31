import pandas as pd
from pandas import DataFrame
import re
import math

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def snake_to_camel(str):
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', str).lower()

@transformer
def transform_df(df: DataFrame, *args, **kwargs) -> DataFrame:
    # Specify your transformation logic here
    cond = (df["passenger_count"] > 0) | (df["trip_distance"] > 0)
    df = df[cond]
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    column_headers = df.columns.tolist()
    camel_case_count = sum(1 for header in column_headers if re.match(r'[A-Z][a-z]*', header))
    print(camel_case_count)
    print(df['VendorID'].unique())
    df = df.rename(snake_to_camel, axis=1)

    return df


@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
