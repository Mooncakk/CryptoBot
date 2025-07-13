import logging
import json
from typing import Optional
from pendulum import now

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError


S3 = boto3.resource('s3')


def open_params(path: str = '../../utils/utils.json') -> tuple[dict[str, str], str]:
    """Open a json file and gets different parameters"""

    with open(path, 'r') as file:
        params = json.load(file)

    crypto_wallet = params.get('crypto_wallet')
    aws_params = params.get('aws_params')
    bucket_name = aws_params.get('s3bucket_name')

    return crypto_wallet, bucket_name


def get_bucket_file(bucket_name: str, prefix: str) -> Optional[str]:
    """Get the latest file in a bucket"""

    bucket = S3.Bucket(bucket_name)
    path = 'data/bronze/etl1/'
    response = bucket.objects.filter(Prefix=f'{path}{prefix}')
    files = [file for file in response]

    if not files:
        return logging.error(f"No files starting with '{prefix}' found")

    return files[-1].key


def process_data(file: str) -> Optional[pd.DataFrame]:
    """Process the data"""

    try:
        table = pq.read_table(file)

    except FileNotFoundError as e:
        logging.error(e)
        return None

    df = table.to_pandas()

    df = df.rename(columns=
                   {
                       0: "date",
                       1: "open",
                       2: "high",
                       3: "low",
                       4: "close",
                       5: "volume"
                   }
                )
    df["date"] = df["date"] // 1000
    df = df.astype(
        {
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "volume": "float",
        }
    )

    return df


def data_to_parquet(data: pd.DataFrame, path: str) -> Optional[bool]:
    """Save data to parquet file in s3 bucket"""

    try:
        table = pa.Table.from_pandas(data)
        pq.write_table(table, path)

    except ClientError as e:
        logging.error(e)
        logging.info('No file created')
        return False

    except AttributeError as e:
        logging.error(e)
        logging.info('No file created')
        return False

    return logging.info(f'{path} file created')


def main() -> None:

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    crypto_wallet, bucket_name = open_params()
    s3_url = f's3://{bucket_name}'
    s3_silver_url = f's3://{bucket_name}/data/silver/etl1'

    for coin in crypto_wallet:

        file = get_bucket_file(bucket_name, coin)
        df = process_data(f'{s3_url}/{file}')
        pair = crypto_wallet.get(coin)
        symbol = pair.split('/')[0]
        current_datetime = now(tz='Europe/Paris').format('Y_MM_DD_HHmmss')
        filename = f'{s3_silver_url}/{coin}_{symbol}_ohlcv_cleaned_{current_datetime}.parquet'
        data_to_parquet(df, filename)

    logging.info('Data processed')


if __name__=='__main__':

    main()
