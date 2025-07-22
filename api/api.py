import json
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import snowflake.connector


DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'

api = FastAPI(title='CryptobotAPI')

def run_query(sql: str):

    conn = snowflake.connector.connect(connection_name='myconnection')
    cur = conn.cursor().execute(sql)
    return cur.fetchall()


class Ohclv(BaseModel):

    open: float = None
    high: float = None
    low: float = None
    close: float = None
    volume: float = None


class Position(BaseModel):

    coin: str = None
    symbol: str = None
    contracts: float = None
    entry_price: float = None
    position_value: float = None
    unrealized_pnl: float = None
    percentage: float = None


class Trade(BaseModel):

    date: datetime = None
    coin: str = None
    symbol: str = None
    side: str = None
    price: float = None
    contracts: float = None
    cost: float = None
    profit_loss: float = None


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


@api.get('/', name='root')
def get_index():
    return 'Welcome to the CryptoBot API'


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


@api.get('/coins/{coin}/avg', name='coin average OHCLV',
         description="Get coin's average rates for the last 30 hours" )
def get_coin_avg(coin: str):

    avg = {}

    if coin not in get_coins():
        raise MyException(coin)

    ohclv_avg = run_query(f"""
                                SELECT AVG(OPEN), AVG(HIGH), AVG(LOW), AVG(CLOSE), AVG(VOLUME)
                                FROM {DATABASE}.{SCHEMA}.{coin}
                                """)
    for data_point, average in zip(Ohclv(), ohclv_avg[0]):
        avg[data_point[0]] = average

    return Ohclv(**avg)


@api.get('/coins/{coin}/max', name='coin higher OHCLV',
         description="Get coin's higher rates for the last 30 hours" )
def get_coin_max(coin: str):

    max_ = {}

    if coin not in get_coins():
        raise MyException(coin)

    ohclv_max = run_query(f"""
                            SELECT MAX(OPEN), MAX(HIGH), MAX(LOW), MAX(CLOSE), MAX(VOLUME)
                            FROM {DATABASE}.{SCHEMA}.{coin}
                            """)
    for data_point, maximum in zip(Ohclv(), ohclv_max[0]):
        max_[data_point[0]] = maximum

    return Ohclv(**max_)


@api.get('/coins/{coin}/min', name='coin lower OHCLV',
         description="Get coin's lower rates for the last 30 hours" )
def get_coin_min(coin: str):

    min_ = {}

    if coin not in get_coins():
        raise MyException(coin)

    ohclv_min = run_query(f"""
                            SELECT MIN(OPEN), MIN(HIGH), MIN(LOW), MIN(CLOSE), MIN(VOLUME)
                            FROM {DATABASE}.{SCHEMA}.{coin}
                            """)
    for data_point, aggregate in zip(Ohclv(), ohclv_min[0]):
        min_[data_point[0]] = aggregate

    return Ohclv(**min_)


@api.get('/positions', name='open positions', description='Get list of open positions')
def get_all_positions():

    dict_ = {}
    open_positions = []
    positions = run_query(f"""
                    SELECT *
                    FROM {DATABASE}.{SCHEMA}.POSITIONS
                    """)

    if positions is None:
        return{'No open positions'}

    for position in positions:
        for attributes, value in zip(Position(), position):
            key = attributes[0]
            dict_[key] = value

        open_positions.append(Position(**dict_))

    return open_positions


@api.get('/coins/{coin}/positions', name="coin's open positions",
         description="Get coin's open positions")
def get_coin_positions(coin: str):

    dict_ = {}
    open_positions = []

    if coin not in get_coins():
        raise MyException(coin)

    positions = run_query(f"""
                            SELECT *
                            FROM {DATABASE}.{SCHEMA}.POSITIONS
                            WHERE coin = '{coin}'
                            """)

    if not positions:
        return{f'No open positions for {coin}'}

    for position in positions:
        for attributes, value in zip(Position(), position):
            key = attributes[0]
            dict_[key] = value

        open_positions.append(Position(**dict_).model_dump())

    return open_positions


@api.get('/trades', name='trades history', description='Get trades history')
def get_trades():

    dict_ = {}
    trades_history = []
    trades = run_query(f"""
                        SELECT *
                        FROM {DATABASE}.{SCHEMA}.TRADES_HISTORY
                        """)
    if not trades:
        return{'No trades history'}

    for trade in trades:
        for attributes, value in zip(Trade(), trade):
            key = attributes[0]
            dict_[key] = value

        trades_history.append(Trade(**dict_))

    return trades_history


@api.get('/coins/{coin}/trades', name="coin's trades history",
         description="Get coin's trades history")
def get_trades_history(coin: str):

    dict_ = {}
    trades_history = []
    symbol = get_coin_info(coin).get('symbol')
    trades = run_query(f"""
                        SELECT *
                        FROM {DATABASE}.{SCHEMA}.TRADES_HISTORY
                        WHERE coin = '{symbol}'
                        """)
    if not trades:
        return{f'No trades history for {coin}'}

    for trade in trades:
        for attributes, value in zip(Trade(), trade):
            key = attributes[0]
            dict_[key] = value

        trades_history.append(Trade(**dict_))

    return trades_history

print(get_trades_history('bitcoin'))