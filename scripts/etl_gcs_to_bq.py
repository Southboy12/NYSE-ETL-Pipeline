from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import glob
from pandasql import sqldf
import pandasql as ps
import duckdb


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

    # Convert date column from string to date
    df['date'] = pd.to_datetime(df.date)
    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.month_name()
    df['day'] = df.date.dt.day

    print(df.head())
    return df

@task(log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load('nyse-cred')
    df.to_gbq(
        destination_table="stock_data.nyse_table",
        project_id="sincere-office-375210",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='replace'
    )


@flow
def etl_gcs_to_bq():
    path = extract_from_gcs()
    df = transform(path)
    write_to_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()