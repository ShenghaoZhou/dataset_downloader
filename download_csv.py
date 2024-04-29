import gdown
import requests
import pandas as pd
from pathlib import Path
import asyncio
import argparse
import functools
import multiprocessing as mp
"""
Given a dataset (a sequence of data sequence name + url), download dataset
For normal link, 
For google drive link, gdown will be used to download the file
"""


def download_normal_link(url, name=None):
    if name is None:
        name = url.split("/")[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_googledrive_link(url, name=None):
    if name is None:
        gdown.download(url, quiet=False, fuzzy=True)
    else:
        gdown.download(url, name=name, quiet=False, fuzzy=True)


def is_google_drive_link(url):
    return "drive.google.com" in url


def download_link(url, name=None):
    if is_google_drive_link(url):
        download_googledrive_link(url, name)
    else:
        download_normal_link(url, name)


def download_dataset_csv(save_dir, csv_file):
    csv = pd.read_csv(csv_file)
    dataset_name = Path(csv_file).stem
    print("Dataset: ", dataset_name)
    tgt_dir = save_dir / dataset_name
    tgt_dir.mkdir(exist_ok=True)
    for i, row in csv.iterrows():
        print("Downloading", row["seq_name"])
        if not is_google_drive_link(row["url"]):
            name = row["url"].split("/")[-1]
            save_path = tgt_dir / name
        else:
            save_path = tgt_dir / "{}.bag".format(row["seq_name"])
        download_link(row["url"], name=save_path)





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_csv_dir")
    args = parser.parse_args()

    data_csv_dir = Path(args.data_csv_dir)

    csv_file_lst = list(data_csv_dir.glob("*.csv"))
    download_fn = functools.partial(download_dataset_csv, data_csv_dir)
    with mp.Pool(mp.cpu_count()) as p:
        p.map(download_fn, csv_file_lst)
