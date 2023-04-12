import pandas as pd
import opendatasets as od
import os
from pathlib import Path
import glob



def extract_data(dataset_url: str) -> pd.DataFrame:
    """"""
    od.download(dataset_url, './data')
    csv_path = Path('./data/nyse-daily-stock-prices')
    files = os.listdir(csv_path)
    #print(len(files))
    parquet_path = Path('./data/parquet')
    dfs = []
    for file in files[:5]:
        df = pd.read_csv(str(csv_path) + '/' + file)
        dfs.append(df)
    return dfs
    #df.to_parquet(f"{str(parquet_path)} + {file[:-3]}.parquet")


if __name__ == "__main__":
    dataset_url = 'https://www.kaggle.com/datasets/svaningelgem/nyse-daily-stock-prices'
    extract_data(dataset_url)
