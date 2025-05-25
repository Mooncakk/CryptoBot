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


def get_ohlcv(hyperliquid_params, coin):
    """Get coin's data"""

    exchange = ccxt.hyperliquid(hyperliquid_params)
    current_datetime = datetime.now()
    hours = timedelta(hours=60)
    date = current_datetime - hours
    since_date = round(date.timestamp()) * 1000

    data = exchange.fetch_ohlcv(coin, '4h', since=since_date)

    return data

def data_to_csv(data, crypto_name):
    """Save data to CSV file"""

    with open(f'./temp/{crypto_name}.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerows(data)


def save_file(data, object_name, crypto_name):
    """Save data to s3 bucket"""

    try:
        bucket.upload_file(data, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    logging.info(f'{crypto_name} data file created')


def open_json(filename):

    with open(filename, 'r') as file:
        return json.load(file)

def create_temp(path):

    os.mkdir(path)


def remove_temp(path):

    shutil.rmtree(path)


def main():

    crypto_wallet = open_json('./crypto_wallet.json')
    params = open_json('./hyperliquid_id.json')

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    current_time = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    bucket_name = 's3bucket-cryptobot'
    bucket = S3.Bucket(bucket_name)
    path = 'data/raw'
    temp_path = './temp'
    bucket.objects.filter(Prefix=path).delete()
    create_temp(temp_path)

    for crypto_name in crypto_wallet:
        
        coin = crypto_wallet.get(crypto_name)
        coin_ohlcv = get_ohlcv(params, coin)
        symbol = coin.split('/')[0]
        filename = f'{path}/{crypto_name}_{symbol}_ohlvc_{current_time}.csv'
        data_to_csv(coin_ohlcv, crypto_name)
        save_file(f'{temp_path}/{crypto_name}.csv', filename, crypto_name.capitalize())

    remove_temp(temp_path)
    logging.info('End of extraction')


if __name__=='__main__':
    main()