from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.live import StockDataStream
from alpaca.data.models import Quote
import requests
import asyncio
import sqlite3
from datetime import datetime

API_KEY = "your_api_key_here"
SECRET_KEY = "your_secret_key_here"

conn = sqlite3.connect("quotes.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS quotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    symbol TEXT,
    bid_price REAL,
    bid_size INTEGER,
    ask_price REAL,
    ask_size INTEGER
)
""")
conn.commit()


#please do not commit my api keys
# trading_client = TradingClient('api-key', 'secret-key', paper=True)
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
stock_stream = StockDataStream(API_KEY, SECRET_KEY)

async def startDataStream():
    # async handler
    async def quote_data_handler(data):
        # quote data will arrive here
        print(f"Storing quote: {data.symbol} | Bid: {data.bid_price}, Ask: {data.ask_price}")
    
        # Insert the quote data into the database
        cursor.execute("""
            INSERT INTO quotes (timestamp, symbol, bid_price, bid_size, ask_price, ask_size)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            data.symbol,
            data.bid_price,
            data.bid_size,
            data.ask_price,
            data.ask_size
        ))
        conn.commit()
 


    stock_stream.subscribe_quotes(quote_data_handler, "SPY", "AAPL", "MSFT", "TSLA")

    stock_stream.run()


def getBuyingPower():
    # Get our account information.
    account = trading_client.get_account()

    # Check if our account is restricted from trading.
    if account.trading_blocked:
        print('Account is currently restricted from trading.')

    # Check how much money we can use to open new positions.
    return account.buying_power


def getProfitLoss():
    # Get our account information.
    account = trading_client.get_account()

    # Check our current balance vs. our balance at the last market close
    balance_change = float(account.equity) - float(account.last_equity)

    return balance_change


def getAllAssets():
    # search for US equities
    search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)

    assets = trading_client.get_all_assets(search_params)
    return assets


def isSymbolTradable():
    # search for AAPL
    aapl_asset = trading_client.get_asset('AAPL')

    if aapl_asset.tradable:
        print('We can trade AAPL.')
        return True
    else:
        print('We cannot trade AAPL.')
        return False


def makeMarketOrder(stockSymbol, volume, orderSide, timeInForce):
    # preparing market order
    market_order_data = MarketOrderRequest(
                        symbol=stockSymbol,
                        qty=volume,
                        side=orderSide,
                        time_in_force=timeInForce
                        )
    return market_order_data


#can be used for limit orders too
def sumbitMarketOrder(market_order_data):
    # Market order
    market_order = trading_client.submit_order(
                    order_data=market_order_data
                )
    print("Market Order sent")


def makeLimitOrder(stockSymbol, limitPrice, volume, orderSide, timeInForce):
    # preparing limit order 
    limit_order_data = LimitOrderRequest(
                        symbol=stockSymbol,
                        limit_price=limitPrice,
                        qty = volume,
                        side=orderSide,
                        time_in_force=timeInForce
                    )
    return limit_order_data


def getData():
    url = "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&timeframe=1Min&start=2024-01-03T00%3A00%3A00Z&end=2024-01-04T00%3A00%3A00Z&limit=10000&adjustment=raw&feed=sip&sort=asc"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": API_KEY, #fill out
        "APCA-API-SECRET-KEY": SECRET_KEY # fill out
    }

    response = requests.get(url, headers=headers)
    print(response.text)



if __name__ == "__main__":
    stock_stream.run()