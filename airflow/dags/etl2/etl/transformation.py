import json
import logging
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.core.interchange.dataframe_protocol import DataFrame
import boto3
from botocore.exceptions import ClientError
import pendulum


S3 = boto3.resource('s3')


def get_bucket_name(filename: str = './utils/utils.json') -> str:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    aws_params = params.get('aws_params')
    bucket_name = aws_params.get('s3bucket_name')

    return bucket_name


def get_bucket_file(bucket_name: str, prefix: str) -> Optional[str]:
    """Get the latest file in a bucket"""

    bucket = S3.Bucket(bucket_name)
    path = 'data/bronze/etl2/'
    response = bucket.objects.filter(Prefix=f'{path}{prefix}')
    files = [file for file in response]

    if not files:
        return logging.error(f"No files starting with '{prefix}' found")

    return files[-1].key


def positions_processing(file: str) -> Optional[DataFrame] :
    """Process the positions.parquet file"""

    try:
        table = pq.read_table(file)
    except FileNotFoundError as e:
        logging.error(e)
        return None

    df = table.to_pandas()

    if df.empty:
        logging.info('There is no positions, the dataframe is empty')
        return df

    df['coin'] = df['info'].apply(lambda x: x['position']['coin'])
    df = df.filter(['coin',
                   'symbol',
                    'contracts',
                    'entryPrice',
                    'notional',
                    'unrealizedPnl',
                    'percentage'])
    df[['coin', 'symbol']] = df[['coin', 'symbol']].astype('string')
    df_cleaned = df.rename(columns={'entryPrice': 'entry_price',
                                    'notional': 'position_value',
                                    'unrealizedPnl': 'unrealized_pnl'})

    return df_cleaned


def trades_processing(file: str) -> Optional[DataFrame]:
    """Process the trades_history.parquet file"""

    try:
        table = pq.read_table(file)
    except FileNotFoundError as e:
        logging.error(e)
        return None

    df = table.to_pandas()

    if df.empty:
        logging.info('There is no trades history, the dataframe is empty')
        return df

    all_keys = {'coin', 'closedPnl'}

    for key in all_keys:
        df[key] = df['info'].apply(lambda x: x[key])

    df = df.filter(['timestamp', 'coin', 'symbol',
                      'side', 'price', 'amount', 'cost', 'closedPnl'])

    df[['coin', 'symbol', 'side']] = df[['coin', 'symbol', 'side']].astype('string')
    df['closedPnl'] = df['closedPnl'].astype('float')
    df['timestamp'] = df['timestamp'] // 1000
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['timestamp'] = df['timestamp'].astype('datetime64[s]')

    df_cleaned = df.rename(columns={'timestamp': 'date',
                              'amount': 'contracts',
                              'closedPnl': 'profit_loss'})

    return df_cleaned


def data_to_parquet(data: DataFrame, path: str) -> Optional[bool]:
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


def main():

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    bucket_name = get_bucket_name()
    positions_file = get_bucket_file(bucket_name, 'positions')
    trades_file = get_bucket_file(bucket_name, 'trades')
    s3_url = f's3://{bucket_name}'
    positions = positions_processing(f'{s3_url}/{positions_file}')
    trades_history = trades_processing(f'{s3_url}/{trades_file}')
    s3_silver_url = f's3://{bucket_name}/data/silver/etl2'
    current_datetime = pendulum.now().format('Y_MM_DD_HHmmss')
    data_to_parquet(positions, f'{s3_silver_url}/positions_cleaned-{current_datetime}.parquet')
    data_to_parquet(trades_history, f'{s3_silver_url}/trades_history_cleaned-{current_datetime}.parquet')


if __name__=='__main__':

    main()
