import logging

import boto3
import snowflake.connector


S3 = boto3.resource('s3')

WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'
STAGE = 'dataset'

USER = 'admin'


def run_query(query: str) -> None:

    with snowflake.connector.connect(connection_name='myconnection') as conn:
        cur = conn.cursor().execute(query)
        rows = cur.fetchall()
        for row in rows:
            logging.info(row)


def create_file_format() -> None:

    run_query(f"""CREATE FILE FORMAT IF NOT EXISTS {DATABASE}.{SCHEMA}.parquet_format
                    TYPE = 'PARQUET'
                    """)


def create_stage(path) -> None:

    run_query(f"""
                CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.s3b
                URL = '{path}'
                DIRECTORY = (ENABLE = TRUE)
                FILE_FORMAT = parquet_format
                """)


def create_table():

    run_query(f"""
                CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.POSITIONS
                (
                
                )
                """)


def main():

    create_file_format()
    bucket_path = 's3://s3bucket-cryptobot/data/silver/'
    #create_stage(bucket_path)


main()