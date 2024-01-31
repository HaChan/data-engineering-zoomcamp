import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import LocalFileSystem
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    table = pa.Table.from_pandas(df)
    output_dir = '/home/src/partitioned_data'

    partitioning = pa.dataset.partitioning(field_names=["lpep_pickup_date"])

    localfs = LocalFileSystem()
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=["lpep_pickup_date"],
        filesystem=localfs
    )
