import pandas as pd
import datetime
import math
import traceback
import time
import simplejson
import pricegetter as pg
import utilities as utils
from exchanges import Bitmex
from exchanges import Binance
from exchanges import Bittrex
from state import State
from xmlreader import XML
from tenacity import *

#get the next action for this coin and iteration
def next_action(quotes, prices, state, exchange, symbols):
    position = state.get_position(exchange, symbols[0])

    if position==False and state.get_vacancies() > 0:
        z = calculate_zscore(prices, quotes[0]['ask'], quotes[1]['bid'])
        if z < sso:
            return 'none'

        bad_spread = abs(quotes[0]['ask'] - quotes[0]['bid']) + \
                     abs(quotes[1]['ask'] - quotes[1]['bid'])
        good_spread = abs(quotes[0]['ask'] - quotes[1]['bid'])
        if bad_spread > good_spread:
            print('spread too wide like a hoe')
            utils.twilio_message('spread too wide like a hoe')
            return 'none'

        long_size, short_size = calculate_position(exchange, quotes, symbols, state)
        if short_size == 0:
            print('chigga you broke')
            utils.twilio_message('chigga you broke')
            return 'none'

        return 'open'

    elif position==True:
        z = calculate_zscore(prices, quotes[0]['bid'], quotes[1]['ask'])
        if z <= ssc:
            return 'close'

    return 'none'

#calculate zscore from given prices
def calculate_zscore(prices, p0, p1):
    latest = pd.DataFrame([[p0, p1]], index=['quote'], columns=prices.columns)
    p = prices[:-1].append(latest)
    r = p[symbols[1]]/p[symbols[0]]
    m = r.ewm(min_periods = x, span = x).mean()
    m.fillna(0)
    s = r.rolling(window = y, center = False).std()
    s.fillna(1)
    z = ((r-m)/s)[-1]
    print('Z-score:', z)
    return z

#calculate asset quantities for opening a position
def calculate_position(exchange, quotes, symbols, state):
    p0 = quotes[0]['ask']
    p1 = quotes[1]['bid']
    v = state.get_vacancies()
    if exchange == 'binance':
        cash = balance_limit*min(bitmex.get_balance()*default_leverage, binance.get_balance())/v
        short_size = math.floor(cash/p1)
        long_size = binance.round_qty(symbols[0], short_size*p1/p0)
    elif exchange == 'bittrex':
        cash = balance_limit*min(bitmex.get_balance()*default_leverage, bittrex.get_balance())/v
        short_size = math.floor(cash/p1)
        long_size = round(short_size*p1/p0, 8)
    return long_size, short_size

#do the shit
def realtime(quotes, prices, state, exchange, symbols):    
    action = next_action(quotes, prices, state, exchange, symbols)

    if action == 'open':
        #trade message
        shittosay = ('OPEN TRADE:\n',
                     'Buy', symbols[0], 'on', exchange, '\n',
                     'Short', symbols[1])
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)

        #open position
        long_size, short_size = calculate_position(exchange, quotes, symbols, state)
        if exchange == 'binance':
            binance.buy(symbols[0], long_size)
        elif exchange == 'bittrex':
            bittrex.buy(symbols[0], long_size)
        bitmex.sell(symbols[1], short_size, default_leverage)
        state.open_position(exchange, symbols[0], long_size, symbols[1], short_size)

        #confirmation message
        shittosay = 'Trade confirmed.'
        utils.twilio_message(shittosay)

    elif action == 'close':
        #trade message
        shittosay = ('CLOSE TRADE:\n',
                     'Sell', symbols[0], '\n',
                     'Buy', symbols[1])
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)

        #close position
        if exchange == 'binance':
            binance.close(symbols[0])
        if exchange == 'bittrex':
            bittrex.close(symbols[0])
        bitmex.buy(symbols[1], state.get_short_quantity(exchange, symbols[1]), default_leverage)
        state.close_position(exchange, symbols[0], symbols[1])

        balance = binance.get_balance() + bitmex.get_balance()
        print('Total balance: ', balance)
        utils.twilio_message('Total balance: ' + str(balance))

        #confirmation message
        shittosay = 'Trade confirmed.'
        utils.twilio_message(shittosay)

    return state

#update state from XML and close anomalies
def update_portfolio(old_state):
    xml = XML('portfolio.xml')
    if xml.has_anomaly():
        exchange, coin, size = xml.get_anomaly()
        if exchange == 'binance':
            binance.close(coin)
        elif exchange == 'bittrex':
            bittrex.close(coin)
        elif exchange == 'bitmex':
            bitmex.buy(coin, size, default_leverage)
    state = xml.update_state(old_state)
    return state

#fetch bids and asks for a trading pair, return as dict
def get_quotes(prices, symbols, exchange):
    quotes = [{}, {}]
    if exchange == 'binance':
        quotes[0]['bid'] = binance.get_bid(symbols[0])
        quotes[0]['ask'] = binance.get_ask(symbols[0])
    elif exchange == 'bittrex':
        quotes[0]['bid'] = bittrex.get_bid(symbols[0])
        quotes[0]['ask'] = bittrex.get_ask(symbols[0])
    quotes[1]['bid'] = bitmex.get_bid(symbols[1])
    quotes[1]['ask'] = bitmex.get_ask(symbols[1])
    return quotes

#data params
start_time = datetime.datetime(2018, 3, 30, 0, 0, 0)
end_time = datetime.datetime.utcnow()
binance_interval = '5m'
bittrex_interval = 'fiveMin'
short_interval = '5m'
pairs = [['ADA', 'ADAM18']]
long_exchanges = ['binance']
x = 288 #moving average window size
y = 288 #std dev window size

#trade params
default_leverage = 1.2
default_fiefdoms = 1
balance_limit = 0.93
sso = 2
ssc = -2

#initialize
longcoins = [symbols[0] for symbols in pairs]
shortcoins = [symbols[1] for symbols in pairs]
binance_prices = pg.get_binance(start_time, end_time, longcoins, binance_interval)
#bittrex_prices = pg.get_bittrex(longcoins, bittrex_interval)
short_prices = pg.get_bitmex(start_time, end_time, shortcoins, short_interval)

bitmex = Bitmex(1)
binance = Binance()
#bittrex = Bittrex()

#run
print('Let\'s GET it')
while True:
    state = update_portfolio(State(long_exchanges, pairs, default_fiefdoms))
    try:
        while True:
            short_prices = pg.update_bitmex(short_prices, shortcoins, short_interval)
            binance_prices = pg.update_binance(binance_prices, longcoins, binance_interval)
#            bittrex_prices = pg.update_bittrex(bittrex_prices, longcoins, bittrex_interval)

            for symbols in pairs:
                for exchange in long_exchanges:        
                    if exchange == 'binance':
                        temp = [binance_prices[symbols[0]], short_prices[symbols[1]]]
                    elif exchange == 'bittrex':
                        temp = [bittrex_prices[symbols[0]], short_prices[symbols[1]]]
                    prices = pd.concat(temp, axis=1).dropna(axis=0, how='any')
                    quotes = get_quotes(prices, symbols, exchange)

                    print('\nTime: ', datetime.datetime.utcnow())
                    print('Exchange: ', exchange)
                    print('Symbol: ', symbols[0])

                    state = realtime(quotes, prices, state, exchange, symbols)

            time.sleep(300)
    except RetryError as e:
        utils.twilio_message('CRASHED! ' + str(e))
        print(traceback.format_exc())
        time.sleep(1800)
    except simplejson.errors.JSONDecodeError as e:
        utils.twilio_message('CRASHED! ' + str(e))
        print(traceback.format_exc())
        time.sleep(60)
    except Exception as e:
        utils.twilio_message('FATAL CRASH')
        print(traceback.format_exc())
        sys.exit('fuck outta here')
