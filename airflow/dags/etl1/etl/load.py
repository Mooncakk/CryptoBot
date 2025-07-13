import json
import logging
from typing import Optional

import snowflake.connector
import boto3


S3 = boto3.resource('s3')

WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'
STAGE = 'dataset_etl1'

USER = 'admin'

def open_params(filename: str = '../../utils/utils.json') -> tuple[dict[str, str], dict]:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    crypto_wallet = params.get('crypto_wallet')
    aws_params = params.get('aws_params')

    return crypto_wallet, aws_params


def run_query(query: str) -> None:
    """Execute a query and log the result"""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    with snowflake.connector.connect(connection_name='myconnection') as conn:
        cur = conn.cursor().execute(query)
        rows = cur.fetchall()
        for row in rows:
            logging.info(row)



def manage_role() -> None:
    """Manage the role in Snowflake"""

    run_query('CREATE ROLE IF NOT EXISTS cryptobot_role')

    run_query(f'GRANT usage ON WAREHOUSE {WAREHOUSE} TO ROLE cryptobot_role')

    run_query(f'GRANT ROLE cryptobot_role TO USER {USER}')

    run_query(f'GRANT ALL ON DATABASE {DATABASE} TO ROLE cryptobot_role')

    run_query('USE ROLE cryptobot_role')


def storage_integration(aws_role_arn: str, aws_external_id: str, name: str = 's3_int') -> None:
    """Create a storage integration for s3 bucket"""

    run_query(f"""CREATE STORAGE INTEGRATION IF NOT EXISTS {name}
                TYPE = EXTERNAL_STAGE
                STORAGE_PROVIDER = 'S3'
                ENABLED = TRUE
                STORAGE_AWS_ROLE_ARN = '{aws_role_arn}'
                STORAGE_AWS_EXTERNAL_ID = '{aws_external_id}'
                STORAGE_ALLOWED_LOCATIONS = ('*')
                """)


def create_stage(bucket_path: str, name: str = 's3_int') -> None:
    """Create a stage"""

    run_query(f"""CREATE OR REPLACE STAGE {DATABASE}.{SCHEMA}.{STAGE}
              STORAGE_INTEGRATION = {name}
              ENCRYPTION = (TYPE = 'AWS_SSE_S3')
              DIRECTORY = (ENABLE = true)
              FILE_FORMAT = {DATABASE}.{SCHEMA}.PARQUET_FORMAT
              URL = {bucket_path}""")


def create_table(coin: str) -> None:
    """Create a table"""

    run_query(f'TRUNCATE TABLE IF EXISTS {DATABASE}.{SCHEMA}.{coin}')
    run_query(f"""
                CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{coin}
                  (
                  date TIMESTAMP_LTZ,
                  open FLOAT,
                  high FLOAT,
                  low FLOAT,
                  close FLOAT,
                  volume FLOAT
                  )
                """
              )


def create_file_format() -> None:
    """Create parquet file format"""

    run_query(f"""
                CREATE FILE FORMAT IF NOT EXISTS {DATABASE}.{SCHEMA}.PARQUET_FORMAT
                TYPE = 'PARQUET'
                """)

def get_bucket_file(bucket_name: str, prefix: str) -> Optional[str]:
    """Get the latest file from a bucket"""

    bucket = S3.Bucket(bucket_name)
    path = 'data/silver/etl1/'
    response = bucket.objects.filter(Prefix=f'{path}{prefix}')
    files = [file for file in response]
    filename = files[-1].key

    if not files:
        return logging.error(f"No files starting with '{prefix}' found")

    return filename.split('/')[-1]


def copy_into_table(coin: str, filename: str) -> None:
    """Copy data from stage to table"""

    logging.info(f'Loading {coin} data')
    run_query(f"""COPY INTO {DATABASE}.{SCHEMA}.{coin}
                  FROM @{DATABASE}.{SCHEMA}.{STAGE}/                  
                  FILES = ('{filename}')
                  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE""")


def main() -> None:

    crypto_wallet, aws_params = open_params()
    aws_role_arn = aws_params.get('aws_role_arn')
    aws_external_id = aws_params.get('aws_external_id')
    bucket_name = aws_params.get('s3bucket_name')
    bucket_path = f's3://{bucket_name}/data/silver/etl1/'

    run_query(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WITH warehouse_size='x-small'")
    run_query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    run_query(f'CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}')
    manage_role()
    storage_integration(aws_role_arn, aws_external_id)
    create_file_format()
    create_stage(bucket_path)

    for coin in crypto_wallet:

        create_table(coin)
        file = get_bucket_file(bucket_name, coin)
        copy_into_table(coin, file)


if __name__=='__main__':

    main()
