import json
from datetime import date

import snowflake.connector


WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'
TABLE = 'crypto_prices'
STAGE = 'dataset'

DATE = date.today()

with open('./crypto_wallet.json', 'r') as file:
     CRYPTO_WALLET = json.load(file)

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

def manage_stage():

    run_query(f"""CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE} 
              ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') 
              DIRECTORY = (ENABLE = true)""")

    run_query(f"PUT file://data/processed/* @{DATABASE}.{SCHEMA}.{STAGE} "
          "auto_compress=false")

def create_temp_table():

    for crypto in CRYPTO_WALLET:
        run_query(f"""CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.tmp_{crypto}_price 
                    (date DATE,
                    {crypto}_price FLOAT)""")

def create_file_format():

    run_query(f"""CREATE FILE FORMAT IF NOT EXISTS {DATABASE}.{SCHEMA}.CLASSIC_CSV
                    TYPE = 'CSV'
                    SKIP_HEADER = 1""")

def insert_temp_table():

    for crypto in CRYPTO_WALLET:
        run_query(f"""COPY INTO {DATABASE}.{SCHEMA}.tmp_{crypto}_price FROM 
                  (SELECT $1 as date,
                  $2 as {crypto}_price
                  FROM @{DATABASE}.{SCHEMA}.dataset/)
                  FILES = ('cleaned_{crypto}_data_{DATE}.csv')
                  FILE_FORMAT = {DATABASE}.{SCHEMA}.CLASSIC_CSV""")


def create_table_query():
    tables = [f'{DATABASE}.{SCHEMA}.tmp_{crypto}_price' for crypto in CRYPTO_WALLET]
    columns = [f'{crypto}_price' for crypto in CRYPTO_WALLET]

    select_clause = f'SELECT {tables[0]}.date, {", ".join(columns)}\n'
    from_clause = f'FROM {tables[0]}\n'
    join_clause = [f'JOIN {table} ON {tables[0]}.date = {table}.date' for table in tables[1:]]
    query = f'CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.{TABLE} AS {select_clause} {from_clause}' + "\n".join(join_clause)
    return query


def main():

    run_query(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WITH warehouse_size='x-small'")
    run_query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    run_query(f'CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}')

    manage_role()
    manage_stage()
    create_temp_table()
    create_file_format()
    insert_temp_table()
    query = create_table_query()
    run_query(query)


main()
