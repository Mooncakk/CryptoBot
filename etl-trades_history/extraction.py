
import shutil
import json
import os
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError
import ccxt
from ccxt import hyperliquid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pendulum

S3 = boto3.resource('s3')


def open_params(path: str = './utils/utils.json') -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Open a json file and gets different parameters"""

    with open(path, 'r') as file:
        params = json.load(file)

    crypto_wallet = params.get('crypto_wallet')
    hyperliquid_params = params.get('hyperliquid_params')
    aws_params = params.get('aws_params')

    return crypto_wallet, hyperliquid_params, aws_params


def exchange(hyperliquid_params: dict[str, str]) -> hyperliquid:

    return ccxt.hyperliquid(hyperliquid_params)


def get_positions(exchange: exchange) -> list:

    return exchange.fetch_positions()


def get_trades(exchange: exchange) -> list:

    return exchange.fetch_my_trades()


def data_to_parquet(data: list, filename: str):

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)


def save_file(bucket_name: str, data: str, object_name: str) -> Optional[bool]:
    """Save data to s3 bucket"""

    try:
        S3.Bucket(bucket_name).upload_file(data, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    return logging.info(f'{object_name} file created')


def create_temp(path: str = './temp') -> None:
    """Create a directory if not exist"""

    try:
        os.mkdir(path)
    except FileExistsError:
        shutil.rmtree(path)
        os.mkdir(path)


def remove_temp(path: str = './temp') -> None:
    """Remove a directory if exist"""

    shutil.rmtree(path)


def manage_bucket(aws_params, s3_path):

    bucket_name = aws_params.get('s3bucket_name')
    bucket = S3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=s3_path).delete()


def main():

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    wallet, hyperliquid_id, aws_params = open_params()
    ex = exchange(hyperliquid_id)
    path = 'data/bronze/trades'
    #manage_bucket(aws_params, path)
    create_temp()
    current_time = pendulum.now().format('Y_MM_D_Hms')

    data_to_parquet(get_positions(ex), f'./temp/positions_{current_time}.parquet')
    data_to_parquet(get_trades(ex), f'./temp/trades_{current_time}.parquet')

    #bucket_name = aws_params.get('s3bucket_name')
    #bucket = S3.Bucket(bucket_name)
    #bucket.objects.filter(Prefix=path).delete()


    #remove_temp()


if __name__=='__main__':

    main()