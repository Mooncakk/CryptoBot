import asyncio
import math
import time
import json
import logging
from typing import List, Optional
from decimal import Decimal, getcontext, ROUND_DOWN

import ccxt.async_support as ccxt
import ta
import snowflake.connector
from pydantic import BaseModel


WAREHOUSE = 'cryptobot'
DATABASE = 'cryptobotdb'
SCHEMA = 'cryptobot_schema'


class UsdtBalance(BaseModel):
    total: float
    free: float
    used: float


class Info(BaseModel):
    success: bool
    message: str


class Order(BaseModel):
    id: str
    pair: str
    type: str
    side: str
    price: float
    size: float
    reduce: bool
    filled: float
    remaining: float
    timestamp: int


class TriggerOrder(BaseModel):
    id: str
    pair: str
    type: str
    side: str
    price: float
    trigger_price: float
    size: float
    reduce: bool
    timestamp: int


class Position(BaseModel):
    pair: str
    side: str
    size: float
    usd_size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    liquidation_price: float | None = None
    margin_mode: str
    leverage: int
    hedge_mode: bool
    open_timestamp: int = 0
    take_profit_price: float | None = None
    stop_loss_price: float | None = None

class Market(BaseModel):
    internal_pair: str
    base: str
    quote: str
    price_precision: float
    contract_precision: float
    contract_size: Optional[float] = 1.0
    min_contracts: float
    max_contracts: Optional[float] = float('inf')
    min_cost: Optional[float] = 0.0
    max_cost: Optional[float] = float('inf')
    coin_index: Optional[int] = 0
    market_price: Optional[float] = 0.0


def get_price_precision(price: float) -> float:
    log_price = math.log10(price)
    order = math.floor(log_price)
    precision = 10 ** (order - 4)
    return precision
    
def number_to_str(n: float) -> str:
    s = format(n, 'f')
    s = s.rstrip('0')
    if s.endswith('.'):
        s = s[:-1]
    
    return s


class PerpHyperliquid:
    def __init__(self, hyperliquid_id):
        hyperliquid_auth_object = hyperliquid_id
        self.public_adress = hyperliquid_auth_object["wallet_address"]
        getcontext().prec = 10
        if hyperliquid_auth_object["private_key"] is None:
            self._auth = False
            self._session = ccxt.hyperliquid()
        else:
            self._auth = True
            self._session = ccxt.hyperliquid(hyperliquid_auth_object)
        self.market: dict[str, Market] = {}

    async def close(self):
        await self._session.close()


    async def load_markets(self) -> dict[str, Market]:
        data = await self._session.publicPostInfo(params={
            "type": "metaAndAssetCtxs",
        })
        resp = {}
        for i in range(0,len(data[0]["universe"])):
            mark_price = float(data[1][i]["markPx"])
            object = data[0]["universe"][i]
            size_decimals = int(object["szDecimals"])
            resp[object["name"]+"/USD"] = Market(
                internal_pair=object["name"],
                base=object["name"],
                quote="USD",
                price_precision=get_price_precision(mark_price),
                contract_precision=1/(10**(size_decimals)),
                min_contracts=1/(10**(size_decimals)),
                min_cost=10,
                coin_index=i,
                market_price=mark_price,
            )
        self.market = resp
        return resp



    def ext_pair_to_pair(self, ext_pair) -> str:
        return self.market[ext_pair].internal_pair

    def pair_to_ext_pair(self, pair) -> str:
        return pair+"/USD"
    
    def ext_pair_to_base(self, ext_pair) -> str:
        return ext_pair.split("/")[0]

    def get_pair_info(self, ext_pair) -> str:
        pair = self.ext_pair_to_pair(ext_pair)
        if pair in self.market:
            return self.market[pair]
        else:
            return None
        
    def size_to_precision(self, pair: str, size: float) -> float:
        size_precision = self.market[pair].contract_precision
        decimal_precision = Decimal(str(size_precision))
        rounded_size = Decimal(str(size)).quantize(decimal_precision, rounding=ROUND_DOWN)
        return float(rounded_size)
    
    def price_to_precision(self, pair: str, price: float) -> float:
        price_precision = self.market[pair].price_precision
        price_dec = Decimal(str(price))
        precision_dec = Decimal(str(price_precision))
        
        rounded_price = (price_dec // precision_dec) * precision_dec
        
        return float(rounded_price)

    async def get_balance(self) -> UsdtBalance:
        data = await self._session.publicPostInfo(params={
            "type": "clearinghouseState",
            "user": self.public_adress,
        })
        total = float(data["marginSummary"]["accountValue"])
        used = float(data["marginSummary"]["totalMarginUsed"])
        free = total - used
        return UsdtBalance(
            total=total,
            free=free,
            used=used,
        )

    async def set_margin_mode_and_leverage(self, pair, margin_mode, leverage):
        if margin_mode not in ["cross", "isolated"]:
            raise Exception("Margin mode must be either 'cross' or 'isolated'")
        asset_index = self.market[pair].coin_index
        try:
            nonce = int(time.time() * 1000)
            req_body = {}
            action = {
                "type": "updateLeverage",
                "asset": asset_index,
                "isCross": margin_mode == "cross",
                "leverage": leverage,
            }
            signature = self._session.sign_l1_action(action, nonce)
            req_body["action"] = action
            req_body["nonce"] = nonce
            req_body["signature"] = signature
            await self._session.private_post_exchange(params=req_body)
        except Exception as e:
            raise e

        return Info(
            success=True,
            message=f"Margin mode and leverage set to {margin_mode} and {leverage}x",
        )

    async def get_open_positions(self, pairs=[]) -> List[Position]:
        data = await self._session.publicPostInfo(params={
            "type": "clearinghouseState",
            "user": self.public_adress,
        })
        # return data
        positions_data = data["assetPositions"]
        positions = []
        for position_data in positions_data:
            position = position_data["position"]
            if self.pair_to_ext_pair(position["coin"]) not in pairs and len(pairs) > 0:
                continue
            type_mode = position_data["type"]
            hedge_mode = True if type_mode != "oneWay" else False
            size = float(position["szi"])
            side = "long" if size > 0 else "short"
            size = abs(size)
            usd_size = float(position["positionValue"])
            current_price = usd_size / size
            positions.append(
                Position(
                    pair=self.pair_to_ext_pair(position["coin"]),
                    side=side,
                    size=size,
                    usd_size=usd_size,
                    entry_price=float(position["entryPx"]),
                    current_price=current_price,
                    unrealized_pnl=float(position["unrealizedPnl"]),
                    liquidation_price=position["liquidationPx"],
                    margin_mode=position["leverage"]["type"],
                    leverage=position["leverage"]["value"],
                    hedge_mode=hedge_mode,
                )
            )

        return positions

    async def place_order(
        self,
        pair,
        side,
        price,
        size,
        type="limit",
        reduce=False,
        error=True,
        market_max_spread=0.1,
    ) -> Order:
        if price is None:
            price = self.market[pair].market_price
        try:
            asset_index = self.market[pair].coin_index
            nonce = int(time.time() * 1000)
            is_buy = side == "buy"
            req_body = {}
            if type == "market":
                if side == "buy":
                    price = price * (1 + market_max_spread)
                else:
                    price = price * (1 - market_max_spread)

            print(number_to_str(self.price_to_precision(pair, price)))
            action = {
                "type": "order",
                "orders": [{
                    "a": asset_index,
                    "b": is_buy,
                    "p": number_to_str(self.price_to_precision(pair, price)),
                    "s": number_to_str(self.size_to_precision(pair, size)),
                    "r": reduce,
                    "t": {"limit":{"tif": "Gtc"}}
                }],
                "grouping": "na",
                "brokerCode": 1,
            }
            signature = self._session.sign_l1_action(action, nonce)
            req_body["action"] = action
            req_body["nonce"] = nonce
            req_body["signature"] = signature
            resp = await self._session.private_post_exchange(params=req_body)
            
            order_resp = resp["response"]["data"]["statuses"][0]
            order_key = list(order_resp.keys())[0]
            order_id = resp["response"]["data"]["statuses"][0][order_key]["oid"]

            order = await self.get_order_by_id(order_id)

            if order_key == "filled":
                order_price = resp["response"]["data"]["statuses"][0][order_key]["avgPx"]
                order.price = float(order_price)
            
            return order
        except Exception as e:
            if error:
                raise e
            else:
                print(e)
                return None


    async def get_order_by_id(self, order_id) -> Order:
        order_id = int(order_id)
        data = await self._session.publicPostInfo(params={
            "user": self.public_adress,
            "type": "orderStatus",
            "oid": order_id,
        })
        order = data["order"]["order"]
        side_map = {
            "A": "sell",
            "B": "buy",
        }
        return Order(
            id=str(order_id),
            pair=self.pair_to_ext_pair(order["coin"]),
            type=order["orderType"].lower(),
            side=side_map[order["side"]],
            price=float(order["limitPx"]),
            size=float(order["origSz"]),
            reduce=order["reduceOnly"],
            filled=float(order["origSz"]) - float(order["sz"]),
            remaining=float(order["sz"]),
            timestamp=int(order["timestamp"]),
        )

    async def cancel_orders(self, pair, ids=[]):
        try:
            asset_index = self.market[pair].coin_index
            nonce = int(time.time() * 1000)
            req_body = {}
            orders_action = []
            for order_id in ids:
                orders_action.append({
                    "a": asset_index,
                    "o": int(order_id),
                })
            action = {
                "type": "cancel",
                "cancels": orders_action,
            }
            signature = self._session.sign_l1_action(action, nonce)
            req_body["action"] = action
            req_body["nonce"] = nonce
            req_body["signature"] = signature
            resp = await self._session.private_post_exchange(params=req_body)
            return Info(success=True, message="Orders cancelled")
        except Exception :
            return Info(success=False, message="Error or no orders to cancel")
        
def query(sql: str):

    conn = snowflake.connector.connect(connection_name='myconnection')
    cur = conn.cursor().execute(sql)
    return cur.fetch_pandas_all()

def open_params(filename: str) -> tuple[dict[str, str], dict[str, str]]:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    crypto_wallet = params.get('crypto_wallet')
    hyperliquid_params = params.get('hyperliquid_params')

    return crypto_wallet, hyperliquid_params


async def main() -> None:

    crypto_wallet, hyperliquid_params = open_params('./utils/utils.json')
    ex = PerpHyperliquid(hyperliquid_params)
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    await ex.load_markets()
    usd = await ex.get_balance()

    for crypto_name in crypto_wallet:

        sql = f'select * from {DATABASE}.{SCHEMA}.{crypto_name}'
        df = query(sql)
        pairs = crypto_wallet.get(crypto_name)
        coin = pairs.split('/')[0]
        df["rsi"] = ta.momentum.rsi(df["CLOSE"], 14)
        positions = await ex.get_open_positions(pairs=[f"{coin}/USD"])
        coin_price = df.iloc[-1]["CLOSE"]
        logging.info(f"Balance: {usd.total} USD")
        logging.info(f"{coin} {coin_price} USD")
        rsi = df.iloc[-2]["rsi"]

        if len(positions) > 0:
            if rsi < 60:
                order = await ex.place_order(f"{coin}/USD", "sell", None, positions[0].size, "market", True)
                logging.info(f'Close order\n\n{order}')

        elif len(positions) == 0:
            if rsi > 60:
                order = await ex.place_order(f"{coin}/USD", "buy", None, (usd.total * 1) / coin_price, "market", False)
                logging.info(f'Buy order\n\n{order}')

            else :
                logging.info(f'No position taken or closed, RSI is under 60 ({round(rsi, 2)}) and 0 {coin} in the wallet')
            
    await ex.close()

if __name__=='__main__':
    asyncio.run(main())
