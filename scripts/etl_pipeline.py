import pandas as pd
import opendatasets as od
import os
from pathlib import Path
import glob



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
    return dfs


def write_to_local(dfs: pd.DataFrame) -> Path:
    """Write DataFrame locally as parquet file"""
    for df in dfs:
        csv_name = df['ticker'][0]
        parquet_path = './data/parquet/'
        df.to_parquet(f"{parquet_path}{csv_name}.parquet", compression='gzip')


def write_to_gsc():
    """Upload local parquet file to GCS"""


if __name__ == "__main__":
    dataset_url = 'https://www.kaggle.com/datasets/svaningelgem/nyse-daily-stock-prices'
    dfs = extract_data(dataset_url)
    path = write_to_local(dfs)
    write_to_gsc()
