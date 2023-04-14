from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import glob



@task(log_prints=True, retries=3)
def extract_from_gcs() -> Path:
    """Download stock data from GCS"""
    gcs_path = "data/split_parquet/*.parquet"
    local_path = Path("data/parquet/")
    local_path.mkdir(parents=True, exist_ok=True)
    gcs_block = GcsBucket.load('nyse-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
    return local_path


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Merge all the files in the folder and clean"""
    df = pd.concat([pd.read_parquet(file) for file in path.glob("*.parquet")])
    print(df.shape)
    return df


@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    GcpCredentials.load('nyse-cred')
    df.to_gbq(
        destination_table=
    )



@flow
def etl_gcs_to_bq():
    path = extract_from_gcs()
    df = transform(path)
    write_to_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()