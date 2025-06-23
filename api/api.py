import json
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import snowflake.connector
import ccxt

api = FastAPI(title='CryptobotAPI')

def run_query(sql: str):

    conn = snowflake.connector.connect(connection_name='myconnection')
    cur = conn.cursor().execute(sql)
    return cur.fetchone()


DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'


class MyException(Exception):

    def __init__(self, coin : str,):
        self.coin = coin


@api.exception_handler(MyException)
def my_exception_handler(request: Request, exception: MyException):
    return JSONResponse(
        status_code=404,
        content={
            'message': f'{exception.coin} not in wallet'
        }
    )


@api.get('/coins', name='coins', description='Get list of coins in the wallet')
def get_coins():

    with open('./utils.json', 'r') as file:
        crypto_wallet = json.load(file).get('crypto_wallet')

    return crypto_wallet


@api.get('/coins/{coin}', name='coin', description="Get coin's info")
def get_coin_info(coin: str):

    wallet = get_coins()

    if coin not in wallet:
        raise MyException(coin)

    pairs = wallet[coin]
    symbol = pairs.split('/')[0]
    return {'coin': coin,
            'symbol': symbol}



class Ohclv(BaseModel):

    open: float = None
    high: float = None
    low: float = None
    close: float = None
    volume: float = None


@api.get('/coins/{coin}/avg', name='coin average OHCLV',
         description="Get coin's average rates for the last 56 hours" )
def get_coin_agg(coin: str):

    avg = {}

    if coin not in get_coins():
        raise MyException(coin)

    ohclv_avg = run_query(f"""
                                SELECT AVG(OPEN), AVG(HIGH), AVG(LOW), AVG(CLOSE), AVG(VOLUME)
                                FROM {DATABASE}.{SCHEMA}.{coin}
                                """)
    for data_point, aggregate in zip(Ohclv(), ohclv_avg):
        avg[data_point[0]] = aggregate
    return avg




@api.get('/coins/{coin}/max', name='coin higher OHCLV', description="Get coin's higher rates for the last 56 hours" )
def get_coin_agg(coin: str):

    _max = {}

    if coin not in get_coins():
        raise MyException(coin)

    ohclv_max = run_query(f"""
                            SELECT MAX(OPEN), MAX(HIGH), MAX(LOW), MAX(CLOSE), MAX(VOLUME)
                            FROM {DATABASE}.{SCHEMA}.{coin}
                            """)
    for data_point, maximum in zip(Ohclv(), ohclv_max):
        _max[data_point[0]] = maximum

    return _max


@api.get('/coins/{coin}/min', name='coin lower OHCLV',
         description="Get coin's lower rates for the last 56 hours" )
def get_coin_agg(coin: str):

    _min = {}

    if coin not in get_coins():
        raise MyException(coin)

    ohclv_min = run_query(f"""
                            SELECT MIN(OPEN), MIN(HIGH), MIN(LOW), MIN(CLOSE), MIN(VOLUME)
                            FROM {DATABASE}.{SCHEMA}.{coin}
                            """)
    for data_point, aggregate in zip(Ohclv(), ohclv_min):
        _min[data_point[0]] = aggregate

    return _min


def exchange():

    with open('./utils.json', 'r') as file:
        hyperliquid_auth_object = json.load(file).get('hyperliquid_params')

    return ccxt.hyperliquid(hyperliquid_auth_object)


@api.get('/positions', name='positions', description='Get list of open positions', tags=['Hyperliquid data'])
def get_all_positions():

    wallet = get_coins()
    pairs = [wallet[coin] for coin in wallet]
    positions = exchange().fetch_positions(pairs)
    position_info = [position['info'] for position in positions]

    return position_info


@api.get('/coins/{coin}/positions', name="coin's open positions",
         description="Get coin's open positions", tags=['Hyperliquid data'])
def get_position(coin: str):

    wallet = get_coins()
    pair = wallet.get(coin)
    return exchange().fetch_position(pair)


@api.get('/coins/{coin}/trades', name="coin's trades history",
         description="Get coin's trades history", tags=['Hyperliquid data'])
def get_trades(coin: str):

    date = datetime(2025,1,1).timestamp()
    since_date = round(date)
    wallet = get_coins()
    pair = wallet.get(coin)

    return exchange().fetch_my_trades(pair, since_date)



#creer une exception handler pour ne oas a réecrire les exception dans chaque fonction
