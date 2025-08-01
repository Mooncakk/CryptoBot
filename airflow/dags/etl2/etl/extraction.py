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
import pendulum

S3 = boto3.resource('s3')


def open_params(path: str = './utils/utils.json') -> tuple[dict[str, str], str]:
    """Open a json file and gets different parameters"""

    with open(path, 'r') as file:
        params = json.load(file)

    hyperliquid_params = params.get('hyperliquid_params')
    aws_params = params.get('aws_params')
    bucket_name = aws_params.get('s3bucket_name')

    return hyperliquid_params, bucket_name


def exchange(hyperliquid_params: dict[str, str]) -> hyperliquid:
    """Create an exchange object to request data from hyperliquid"""

    return ccxt.hyperliquid(hyperliquid_params)


def get_positions(ex: exchange) -> list:
    """Get list of open positions"""

    return ex.fetch_positions()


def get_trades(ex: exchange) -> list:
    """Get list of trades history"""

    return ex.fetch_my_trades()


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
    hyperliquid_id, bucket_name = open_params()
    ex = exchange(hyperliquid_id)
    current_datetime = pendulum.now().format('Y_MM_DD_HHmmss')
    positions = get_positions(ex)
    trades_history = get_trades(ex)
    s3_url = f's3://{bucket_name}/data/bronze/etl2'
    data_to_parquet(positions, f'{s3_url}/positions-{current_datetime}.parquet')
    data_to_parquet(trades_history, f'{s3_url}/trades-{current_datetime}.parquet')


if __name__=='__main__':

    main()