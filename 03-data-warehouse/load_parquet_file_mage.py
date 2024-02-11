import io
import pandas as pd
import requests
import pyarrow.parquet as pq

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{n:02d}.parquet"

    dfs = []

    for n in range(1, 13):
        url = base_url.format(n=n)
        response = requests.get(url)

        if response.status_code == 200:
            parquet_file = pq.read_table(io.BytesIO(response.content))

            df = parquet_file.to_pandas()

            dfs.append(df)
        else:
            print(f"Failed to fetch data from {url}")

    concat_dfs = pd.concat(dfs, ignore_index=True)

    return concat_dfs


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, "The output is undefined"
    assert len(output) > 0, "The output DataFrame is empty"
