import shutil
from datetime import datetime, timedelta
import json
import os
import csv
import logging

import boto3
from botocore.exceptions import ClientError
import ccxt


S3 = boto3.resource('s3')


def get_ohlcv(hyperliquid_params: dict, coin: str) -> list:
    """Get coin's data"""

    exchange = ccxt.hyperliquid(hyperliquid_params)
    current_datetime = datetime.now()
    hours = timedelta(hours=60)
    date = current_datetime - hours
    since_date = round(date.timestamp()) * 1000

    data = exchange.fetch_ohlcv(coin, '4h', since=since_date)

    return data

def data_to_csv(data: list, crypto_name: str) -> None:
    """Save data to CSV file"""

    with open(f'./temp/{crypto_name}.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerows(data)


def save_file(s3bucket: str, data: str, object_name: str, crypto_name: str) -> None | bool:
    """Save data to s3 bucket"""

    try:
        s3bucket.upload_file(data, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    logging.info(f'{crypto_name} data file created')


def open_params(filename: str) -> json:

    with open(filename, 'r') as file:
        return json.load(file)

def create_temp(path: str) -> None:

    try:
        os.mkdir(path)
    except FileExistsError:
        shutil.rmtree(path)
        os.mkdir(path)


def remove_temp(path: str) -> None:

    shutil.rmtree(path)


def main():

    params = open_params('../utils/utils.json')
    crypto_wallet = params.get('crypto_wallet')
    hyperliquid_params = params.get('hyperliquid_params')
    aws_params = params.get('aws_params')

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    current_time = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    path = 'data/raw'
    temp_path = './temp'
    bucket = S3.Bucket(aws_params.get('s3bucket_name'))
    bucket.objects.filter(Prefix=path).delete()
    create_temp(temp_path)

    for crypto_name in crypto_wallet:
        
        coin = crypto_wallet.get(crypto_name)
        coin_ohlcv = get_ohlcv(hyperliquid_params, coin)
        symbol = coin.split('/')[0]
        filename = f'{path}/{crypto_name}_{symbol}_ohlvc_{current_time}.csv'
        data_to_csv(coin_ohlcv, crypto_name)
        save_file(bucket,f'{temp_path}/{crypto_name}.csv', filename, crypto_name.capitalize())

    remove_temp(temp_path)
    logging.info('End of extraction')


if __name__=='__main__':
    main()