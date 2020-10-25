import bitmex as bm
import sys
import time
import utilities as utils
from binance.client import Client as bn
from bittrex.bittrex import Bittrex as bt, API_V1_1
from xmlreader import XML
from tenacity import *

class Bitmex(object):
    #auth info (key, secret)
    #0 = bennett, 1 = dehaven
    auth = [('R6R1UjARaSX71JqLmOZvC7Ur', 'tLZ9tJoIl7717SI7c--UknsJhuTzE8TYoW_5i8xC5X0SBa44'),
            ('DgthnpV3RMizE8I2-v2jFQdu', 'A7RTgxRdYV6UUv7c-V8Uf5jsabOAMjU51JEF8lwmusfQlpUh')]

    def __init__(self, auth_index):
        if auth_index >= len(self.auth):
            sys.exit('invalid Bitmex authentication index')
        
        self.client = bm.bitmex(test=False, api_key=self.auth[auth_index][0],
                                api_secret=self.auth[auth_index][1])
        self.xml = XML('portfolio.xml')

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def get_balance(self):
        bal = self.client.User.User_getMargin().result()[0]['walletBalance']
        return bal/100000000.0

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def buy(self, coin, quantity, lev):
        self.client.Order.Order_new(symbol=coin, ordType = 'Market', orderQty=quantity).result()
        self.client.Position.Position_updateLeverage(symbol=coin, leverage=lev).result()
        self.xml.remove_short(coin, 'bitmex')
        return quantity

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def sell(self, coin, quantity, lev):
        self.client.Order.Order_new(symbol=coin, ordType = 'Market', orderQty=-quantity).result()
        self.client.Position.Position_updateLeverage(symbol=coin, leverage=lev).result()
        self.xml.add_short(coin, quantity, 'bitmex')
        return -quantity

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def get_bid(self, coin):
        response = self.client.OrderBook.OrderBook_getL2(symbol=coin, depth=1).result()[0]
        for entry in response:
            if entry['side'] == 'Buy':
                bid = entry['price']
        return float(bid)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def get_ask(self, coin):
        response = self.client.OrderBook.OrderBook_getL2(symbol=coin, depth=1).result()[0]
        for entry in response:
            if entry['side'] == 'Sell':
                ask = entry['price']
        return float(ask)

class Binance(object):
    apiKey = 'jSBuluh8HDwzZtnOf0ZHX7nehWUoMsUNBb1IuH5YdaLvcaUY2bMcf3NYd7AanswV'
    apiSecret = 'c9E6Ythwhn8mSSZv6VG1LDgQifchw5CROGzEMdrrIXJiasiFey6cc2c9QBxFcX21'
    
    def __init__(self):
        self.client = bn(self.apiKey, self.apiSecret)
        self.xml = XML('portfolio.xml')

    def test(self):
        print(self.client.get_order_book(symbol='ETHBTC'))
        print(self.client.get_all_tickers())
        print(self.client.get_account())

    # round qty to the nearest accepted step size for coin
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def round_qty(self, coin, qty):
        qty_increment = 0.0
        for f in self.client.get_symbol_info(coin + 'BTC')['filters']:
            if f['filterType'] == 'LOT_SIZE':
                qty_increment = float(f['stepSize'])

        return divmod(qty, qty_increment)[0]*qty_increment

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def get_coin_balance(self, coin):
        bal = self.client.get_asset_balance(asset=coin)['free']
        return float(bal)

    def get_balance(self):
        bal = self.get_coin_balance('BTC')
        return float(bal)

    def buy(self, coin, qty):
        self.client.order_market_buy(symbol=coin+'BTC', quantity=qty)
        self.xml.add_long(coin, qty, 'binance')
        return qty

    def close(self, coin):
        qty = float(self.client.get_asset_balance(asset=coin)['free'])
        rounded = self.round_qty(coin, qty)
        self.client.order_market_sell(symbol=coin+'BTC', quantity=rounded)
        self.xml.remove_long(coin, 'binance')
        return 0

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def get_bid(self, coin):
        bid = self.client.get_orderbook_ticker(symbol=coin+'BTC')['bidPrice']
        return float(bid)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=125))
    def get_ask(self, coin):
        ask = self.client.get_orderbook_ticker(symbol=coin+'BTC')['askPrice']
        return float(ask)

class Bittrex(object):
    apiKey = '67feb8569d684259b907753f30d082b7'
    apiSecret = '54fdb82a48c0447d8e7b4779bb5d7cc2'

    def __init__(self):
        self.client = bt(self.apiKey, self.apiSecret, api_version=API_V1_1)
        self.xml = XML('portfolio.xml')
    
    def get_coin_balance(self, coin):
        tries = 0
        success = False
        while tries < 5 and success == False:
            tries += 1
            time.sleep(5*(tries**2))
            try:
                res = self.client.get_balance(currency=coin)
                bal = res['result']['Balance']
                success = True
            except:
                print('Atypical output:')
                print(res)
        if success == False:
            utils.twilio_message('Balance shit fucked up again')
            sys.exit('Shit ain\'t working')
        return bal

    def get_balance(self):
        bal = self.get_coin_balance('BTC')
        return bal

    def buy(self, coin, qty):
        ask = self.get_ask(coin)
        self.client.buy_limit(market='BTC-' + coin, quantity=qty, rate=ask*1.1)
        self.xml.add_long(coin, qty, 'bittrex')
        return qty

    def sell(self, coin, qty):
        bid = self.get_bid(coin)
        self.client.sell_limit(market='BTC-' + coin, quantity=qty, rate=bid*0.9)
        self.xml.remove_long(coin, 'bittrex')
        return qty

    def close(self, coin):
        tries = 0
        success = False
        while tries < 5 and success == False:
            tries += 1
            time.sleep(5*(tries**2))
            try:
                res = self.client.get_balance(currency=coin)
                qty = float(res['result']['Balance'])
                success = True
            except:
                print('Atypical output:')
                print(res)
        if success == False:
            utils.twilio_message('Ask shit fucked up')
            sys.exit('Shit ain\'t working')
        self.sell(coin, qty)
    
    def get_ask(self, coin):
        tries = 0
        success = False
        while tries < 5 and success == False:
            tries += 1
            time.sleep(5*(tries**2))
            try:
                res = self.client.get_ticker(market='BTC-' + coin)
                ask = res['result']['Ask']
                success = True
            except:
                print('Atypical output:')
                print(res)
        if success == False:
            utils.twilio_message('Ask shit fucked up')
            sys.exit('Shit ain\'t working')
        return ask

    def get_bid(self, coin):
        tries = 0
        success = False
        while tries < 5 and success == False:
            tries += 1
            time.sleep(5*(tries**2))
            try:
                res = self.client.get_ticker(market='BTC-' + coin)
                bid = res['result']['Bid']
                success = True
            except:
                print('Atypical output:')
                print(res)
        if success == False:
            utils.twilio_message('Bid shit fucked up')
            sys.exit('Shit ain\'t working')
        return bid


