
import json

import snowflake.connector

WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'
STAGE = 'dataset'


USER = 'admin'

def run_query(query):
    with snowflake.connector.connect(connection_name='myconnection') as conn:
        cur = conn.cursor().execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)


def manage_role():

    run_query('CREATE ROLE IF NOT EXISTS cryptobot_role')

    run_query(f'GRANT usage ON WAREHOUSE {WAREHOUSE} TO ROLE cryptobot_role')

    run_query(f'GRANT ROLE cryptobot_role TO USER {USER}')

    run_query(f'GRANT ALL ON DATABASE {DATABASE} TO ROLE cryptobot_role')

    run_query('USE ROLE cryptobot_role')


def manage_stage(bucket_path):

    run_query(f"""CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE} 
              ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') 
              DIRECTORY = (ENABLE = true)""")

    run_query(f'REMOVE @{DATABASE}.{SCHEMA}.dataset/')

    run_query(f"PUT file://{bucket_path}/* @{DATABASE}.{SCHEMA}.{STAGE} "
              "auto_compress=false")


def create_table(wallet):

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


def create_file_format():

    run_query(f"""CREATE FILE FORMAT IF NOT EXISTS {DATABASE}.{SCHEMA}.CLASSIC_CSV
                    TYPE = 'CSV'
                    SKIP_HEADER = 1""")


def insert_to_table(wallet):

    for crypto_name in wallet:
        pairs = wallet.get(crypto_name)
        symbol = pairs.split('/')[0]
        run_query(f"""COPY INTO {DATABASE}.{SCHEMA}.{crypto_name}
                  FROM @{DATABASE}.{SCHEMA}.dataset/                  
                  PATTERN = '.*{symbol}.*'
                  FILE_FORMAT = {DATABASE}.{SCHEMA}.CLASSIC_CSV""")
        

def main():

    with open('./crypto_wallet.json', 'r') as file:
     crypto_wallet = json.load(file)

    bucket_path = 's3://s3bucket-cryptobot/data/processed'

    run_query(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WITH warehouse_size='x-small'")
    run_query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    run_query(f'CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}')
    
    manage_role()
    manage_stage(bucket_path)
    create_table(crypto_wallet)
    create_file_format()
    insert_to_table(crypto_wallet)
    

main()
#manage_stage()

#run_query(f'drop database {DATABASE}')
#run_query(f'DROP warehouse {WAREHOUSE}')

