from pendulum import duration, now
import json
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError
import ccxt
from ccxt import hyperliquid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


S3 = boto3.resource('s3')


def open_params(filename: str = './utils/utils.json') -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    crypto_wallet = params.get('crypto_wallet')
    hyperliquid_params = params.get('hyperliquid_params')
    aws_params = params.get('aws_params')
    aws_bucket_name = aws_params.get('s3bucket_name')

    return crypto_wallet, hyperliquid_params, aws_bucket_name


def exchange(hyperliquid_params: dict[str, str]) -> hyperliquid:
    """Create an exchange object to request data from hyperliquid"""

    return ccxt.hyperliquid(hyperliquid_params)


def get_ohlcv(ex: exchange, coin: str) -> list[list]:
    """Get coin's data"""

    date = now() - duration(hours=30)
    since_date = date.int_timestamp * 1000

    data = ex.fetch_ohlcv(coin, '2h', since=since_date, limit=15)

    return data


def data_to_parquet(data: list, filename: str) -> Optional[bool]:
    """Save data to parquet file in s3 bucket"""

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    try:
        pq.write_table(table, filename)
    except ClientError as e:
        logging.error(e)
        return False

    return logging.info(f'{filename} file created')


def main() -> None:

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    crypto_wallet, hyperliquid_id, bucket_name = open_params()
    ex = exchange(hyperliquid_id)
    current_datetime = now(tz='Europe/Paris').format('Y_MM_DD_HHmmss')
    s3_path = f's3://{bucket_name}/data/bronze/etl1'

    for coin in crypto_wallet:
        
        pair = crypto_wallet.get(coin)
        coin_ohlcv = get_ohlcv(ex, pair)
        symbol = pair.split('/')[0]
        filename = f'{s3_path}/{coin}_{symbol}_ohlvc_{current_datetime}.parquet'
        data_to_parquet(coin_ohlcv, filename)

    logging.info('End of extraction')


if __name__=='__main__':

    main()