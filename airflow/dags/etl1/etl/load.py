import json
import logging

import snowflake.connector

WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'
STAGE = 'dataset'

USER = 'admin'

def open_params(filename: str) -> tuple[dict[str, str], str]:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    crypto_wallet = params.get('crypto_wallet')
    aws_params = params.get('aws_params')
    s3bucket_name = aws_params.get('s3bucket_name')

    return crypto_wallet, s3bucket_name


def run_query(query: str) -> None:

    with snowflake.connector.connect(connection_name='myconnection') as conn:
        cur = conn.cursor().execute(query)
        rows = cur.fetchall()
        for row in rows:
            logging.info(row)



def manage_role() -> None:

    run_query('CREATE ROLE IF NOT EXISTS cryptobot_role')

    run_query(f'GRANT usage ON WAREHOUSE {WAREHOUSE} TO ROLE cryptobot_role')

    run_query(f'GRANT ROLE cryptobot_role TO USER {USER}')

    run_query(f'GRANT ALL ON DATABASE {DATABASE} TO ROLE cryptobot_role')

    run_query('USE ROLE cryptobot_role')


def storage_integration(name: str) -> None:

    run_query(f"""CREATE STORAGE INTEGRATION IF NOT EXISTS {name}
                TYPE = EXTERNAL_STAGE
                STORAGE_PROVIDER = 'S3'
                ENABLED = TRUE
                STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::535002870165:role/snow_role'
                STORAGE_AWS_EXTERNAL_ID = 'KT10521_SFCRole=2_6fPVum4m2cF+XvAefTxF1nj3Raw='
                STORAGE_ALLOWED_LOCATIONS = ('*')
                """)


def manage_stage(storage_int: str, bucket_path: str) -> None:

    run_query(f"""CREATE OR REPLACE STAGE {DATABASE}.{SCHEMA}.{STAGE}
              STORAGE_INTEGRATION = {storage_int}
              ENCRYPTION = (TYPE = 'AWS_SSE_S3')
              DIRECTORY = (ENABLE = true)
              URL = {bucket_path}""")


def create_table(wallet: dict[str, str]) -> None:

    for coin in wallet:

        run_query(f"""CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.{coin}
                  (
                  date DATETIME,
                  open FLOAT,
                  high FLOAT,
                  low FLOAT,
                  close FLOAT,
                  volume FLOAT
                  )""")


def create_file_format() -> None:

    run_query(f"""CREATE FILE FORMAT IF NOT EXISTS {DATABASE}.{SCHEMA}.CLASSIC_CSV
                    TYPE = 'CSV'
                    SKIP_HEADER = 1""")


def insert_to_table(wallet: dict[str, str]) -> None:

    for crypto_name in wallet:
        pairs = wallet.get(crypto_name)
        symbol = pairs.split('/')[0]
        run_query(f"""COPY INTO {DATABASE}.{SCHEMA}.{crypto_name}
                  FROM @{DATABASE}.{SCHEMA}.dataset/                  
                  PATTERN = '.*{symbol}.*'
                  FILE_FORMAT = {DATABASE}.{SCHEMA}.CLASSIC_CSV""")


def main() -> None:

    crypto_wallet, bucket_name = open_params('./utils/utils.json')

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    integration_name = 's3_int'
    bucket_path = f's3://{bucket_name}/data/processed/'

    run_query(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WITH warehouse_size='x-small'")
    run_query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    run_query(f'CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}')

    manage_role()
    storage_integration(integration_name)
    manage_stage(integration_name, bucket_path)
    create_table(crypto_wallet)
    create_file_format()
    insert_to_table(crypto_wallet)


if __name__=='__main__':

    main()
    #run_query(f'drop database {DATABASE}')
