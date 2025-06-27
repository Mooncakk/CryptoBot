import json
import logging
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.core.interchange.dataframe_protocol import DataFrame
import boto3
from botocore.exceptions import ClientError


S3 = boto3.resource('s3')


def open_params(filename: str = './utils/utils.json') -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    aws_params = params.get('aws_params')
    bucket_name = aws_params.get('s3bucket_name')

    return aws_params


def positions_processing(file: str) -> DataFrame:

    table = pq.read_table(file)
    df = table.to_pandas()
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


def trades_processing(data: str) -> DataFrame:

    table = pq.read_table(data)
    df = table.to_pandas()
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
                              'closedPnl': 'profit&loss'})

    return df_cleaned


def data_to_parquet(data: DataFrame, path: str) -> Optional[bool]:

    table = pa.Table.from_pandas(data)
    try:
        pq.write_table(table, path)
    except ClientError as e:
        logging.error(e)
        return False

    return logging.info(f'{path} file created')


def main():

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    bucket_name = open_params()
    positions = positions_processing('s3://s3bucket-cryptobot/data/bronze/positions-2025_06_27_45217.parquet')
    #trades_history = trades_processing('./temp/trades.parquet')
    data_to_parquet(positions, 's3://s3bucket-cryptobot/data/silver/positions-2025_06_27_45217.parquet')
    #data_to_parquet(trades_history, './temp/trades_cleaned.parquet')


if __name__=='__main__':

    main()
