from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce



API_KEY = ""
SECRET_KEY = ""
STOCK_SYMBOL = "SMCI"



#please do not commit my api keys
# trading_client = TradingClient('api-key', 'secret-key', paper=True)
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)



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


