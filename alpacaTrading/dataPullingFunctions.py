from alpaca.data.live import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
import pandas as pd
from datetime import datetime
import requests
import sqlite3


API_KEY = "PK14NYIQ2DYVASM9WLKG"
SECRET_KEY = "IyaIQHpThJkY86dBLpXXlo7pYBT5zMaWLvLNPFXF"
STOCK_SYMBOL = "SMCI"
HISTORICAL_START_YEAR = 2025
HISTORICAL_START_MONTH = 7
HISTORICAL_START_DAY = 1

HISTORICAL_END_YEAR = 2025
HISTORICAL_END_MONTH = 7
HISTORICAL_END_DAY = 2

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

stock_stream = StockDataStream(API_KEY, SECRET_KEY)

# async handler


def startDataStream():

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

    stock_stream.subscribe_quotes(quote_data_handler, STOCK_SYMBOL)

    stock_stream.run()

    makeIntoCSV()


def makeIntoCSV():
    currentDate = datetime.now()
    year = currentDate.strftime("%Y")
    month = currentDate.strftime("%m")
    day = currentDate.strftime("%d")

    df = pd.read_sql_query("SELECT * FROM quotes", conn)

    # Save to CSV
    df.to_csv(f"{STOCK_SYMBOL}_{year}_{month}_{day}.csv", index=False)

    df = pd.read_csv("quotes.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])



def getData():
    url = "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&timeframe=1Min&start=2024-01-03T00%3A00%3A00Z&end=2024-01-04T00%3A00%3A00Z&limit=10000&adjustment=raw&feed=sip&sort=asc"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": API_KEY, #fill out
        "APCA-API-SECRET-KEY": SECRET_KEY # fill out
    }

    response = requests.get(url, headers=headers)
    print(response.text)


def getHistoricalData(tickerSymbol=STOCK_SYMBOL):
    client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

    # Set request parameters
    request_params = StockBarsRequest(
        symbol_or_symbols=[tickerSymbol],
        timeframe=TimeFrame.Minute,
        start=datetime(HISTORICAL_START_YEAR, HISTORICAL_START_MONTH, HISTORICAL_START_DAY),
        end=datetime(HISTORICAL_END_YEAR, HISTORICAL_END_MONTH, HISTORICAL_END_DAY),
        feed=DataFeed.IEX
    )

    # Fetch bars
    bars = client.get_stock_bars(request_params)

    # Convert to DataFrame
    df = bars.df
    print(df.head())

    # Optional: save to CSV
    df.to_csv(f"HistoricalData/{tickerSymbol}_{HISTORICAL_START_YEAR}_{HISTORICAL_START_MONTH}_{HISTORICAL_START_DAY}.csv")


def getAllSAPTickers():
    tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    print(tickers.head())

    for ticker in tickers.Symbol.to_list():
        getHistoricalData(ticker)




if __name__ == '__main__':
    getAllSAPTickers()