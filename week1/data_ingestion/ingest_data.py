#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

rides_file_path = "data/yellow_tripdata.csv"
zones_file_path = "data/taxi_zone_lookup.csv"


def download_csv(url, file_path):
    os.system(f"wget {url} -O {file_path}")


def download_data(rides_url, zones_url):
    download_csv(rides_url, rides_file_path)
    download_csv(zones_url, zones_file_path)


def transform_rides_df(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


def ingest_csv(file_name, engine, table_name, transform_df):

    print(f"Ingesting data from {file_name}")

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    df = transform_df(next(df_iter))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    for df in df_iter:
        t_start = time()

        df_out = transform_df(df)

        df_out.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('\t inserted another chunk, took %.3f second' % (t_end - t_start))

    print(f"Finished ingesting {file_name}")


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    rides_table_name = params.rides_table_name
    zones_table_name = params.zones_table_name
    rides_url = params.rides_url
    zones_url = params.zones_url
    use_cached = params.use_cached

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if not use_cached:
        print("Downloading data")
        download_data(rides_url, zones_url)
    else:
        print("Using previously downloaded data")

    ingest_csv(rides_file_path, engine, rides_table_name, transform_rides_df)
    ingest_csv(zones_file_path, engine, zones_table_name, lambda df: df)

    print('Job finished successfully')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--rides_table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--zones_table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--rides_url', required=True, help='url of the taxi rides csv file')
    parser.add_argument('--zones_url', required=True, help='url of the zone reference csv file')
    parser.add_argument('--use_cached', required=False, help='use already downloaded data from mounted volume')

    args = parser.parse_args()

    main(args)
