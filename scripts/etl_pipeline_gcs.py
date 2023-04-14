import pandas as pd
import opendatasets as od
import os
from pathlib import Path
import glob
from prefect import flow, task, Flow
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import numpy as np



@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(dataset_url: str) -> pd.DataFrame:
    """Read daily stock prices into pandas DataFrame"""
    od.download(dataset_url, './data')
    csv_path = Path('./data/nyse-daily-stock-prices')
    files = os.listdir(csv_path)
    #print(len(files))
    dfs = []
    for file in files:
        df = pd.read_csv(str(csv_path) + '/' + file)
        dfs.append(df)
    combined_df = pd.DataFrame(pd.concat(dfs), columns=['ticker', 'date', 'open', 'high', 'low', 'close'])
    return combined_df

@task(log_prints=True)
def write_to_local(df: pd.DataFrame) -> Path:
    """Write DataFrame locally as parquet file"""
    parquet_path = "./data/parquet/nyse.parquet"
    df.to_parquet(parquet_path, compression='gzip')
    return parquet_path

@task(log_prints=True)
def write_to_gcs(path: Path, df: pd.DataFrame) -> None:
    """Upload local parquet file to GCS"""
    print(df.shape)
    dfs = np.array_split(df, 20)
    for i, df_split in enumerate(dfs):
        print(df_split.shape)
        split_path = Path(f"./data/split_parquet/{i}.parquet")
        df_split.to_parquet(split_path)

    #for file in os.listdir("./data/split_parquet"):
    gcs_block = GcsBucket.load("nyse-gcs")
    gcs_block.upload_from_folder(from_folder="./data/split_parquet", to_folder="./data/split_parquet")
    return

@flow()
def parent_flow():
    dataset_url = 'https://www.kaggle.com/datasets/svaningelgem/nyse-daily-stock-prices'
    df = extract_data(dataset_url)
    path = write_to_local(df)
    write_to_gcs(path, df)


if __name__ == "__main__":
    parent_flow()