from pandas import DataFrame

import os
import pyarrow as pa
import pyarrow.parquet as pq

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/etlwork-6f1e5b625361.json"

bucket_name = "de-zoomcamp-odeke"
project_id = "etlwork"
table_name = "green_taxi_2022"

root_path = f"{bucket_name}/{table_name}"


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(table, root_path=root_path, filesystem=gcs)
