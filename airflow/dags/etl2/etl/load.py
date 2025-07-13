import logging
import json
from typing import Optional

import boto3
import snowflake.connector


S3 = boto3.resource('s3')

WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'
STAGE = 'dataset_etl2'

USER = 'admin'


def run_query(query: str) -> None:
    """Execute a query and log the result"""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    with snowflake.connector.connect(connection_name='myconnection') as conn:
        cur = conn.cursor().execute(query)
        rows = cur.fetchall()
        for row in rows:
            logging.info(row)


def create_stage(path: str) -> None:
    """Create a stage in Snowflake"""

    run_query(f"""
                CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE}
                URL = '{path}'
                DIRECTORY = (ENABLE = TRUE)
                FILE_FORMAT = {DATABASE}.{SCHEMA}.parquet_format
                STORAGE_INTEGRATION = s3_int
                """)


def create_positions_table() -> None:
    """Create positions table in Snowflake"""

    run_query(f'TRUNCATE TABLE IF EXISTS {DATABASE}.{SCHEMA}.POSITIONS')
    run_query(f"""
                CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.POSITIONS
                (
                    coin STRING,
                    symbol STRING,
                    contracts FLOAT,
                    entry_price FLOAT,
                    position_value FLOAT,
                    unrealized_pnl FLOAT,
                    percentage FLOAT
                )
                """)


def create_trades_table() -> None:
    """Create trades_history table in Snowflake"""

    run_query(f'TRUNCATE TABLE IF EXISTS {DATABASE}.{SCHEMA}.TRADES_HISTORY')
    run_query(f"""
                CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.TRADES_HISTORY
                (
                    date TIMESTAMP_LTZ,
                    coin STRING,
                    symbol STRING,
                    side STRING,
                    price FLOAT,
                    contracts FLOAT,
                    cost FLOAT,
                    profit_loss FLOAT
                )
                """)


def get_bucket_file(bucket_name: str, prefix: str) -> Optional[str]:
    """Get the latest file from a bucket"""

    bucket = S3.Bucket(bucket_name)
    path = 'data/silver/etl2/'
    response = bucket.objects.filter(Prefix=f'{path}{prefix}')
    files = [file for file in response]
    filename = files[-1].key

    if not files:
        return logging.error(f"No files starting with '{prefix}' found")

    return filename.split('/')[-1]


def copy_into_table(table: str, filename: str) -> None:
    """Copy data from a stage to a table"""

    run_query(f"""
                COPY INTO {DATABASE}.{SCHEMA}.{table}
                FROM @{DATABASE}.{SCHEMA}.{STAGE}
                FILES=('{filename}')
                MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                """)


def get_bucket_name(filename: str = '../../utils/utils.json') -> str:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    aws_params = params.get('aws_params')
    bucket_name = aws_params.get('s3bucket_name')

    return bucket_name


def main() -> None:

    bucket_name = get_bucket_name()
    bucket_path = f's3://{bucket_name}/data/silver/etl2/'
    create_stage(bucket_path)
    create_positions_table()
    create_trades_table()
    positions_file = get_bucket_file(bucket_name, 'positions')
    copy_into_table('positions', positions_file)
    trades_history_file = get_bucket_file(bucket_name, 'trades')
    copy_into_table('trades_history', trades_history_file)


if __name__=='__main__':

    main()