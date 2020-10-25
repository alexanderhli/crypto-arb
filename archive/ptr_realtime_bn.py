import requests as re
import numpy as np
import pandas as pd
import math
import datetime
import time
import pricegetter as pg
import utilities as utils
from exchanges import Bitmex
from exchanges import Binance

#do the shit
def realtime(spreads, prices, sbo, sso, sbc, ssc, position):
    p0 = prices[symbols[0]][-1]
    p1 = prices[symbols[1]][-1]
    beta = p1/p0    
    cash = balance_limit*min(bitmex.get_balance(), binance.get_balance())
    spread_diff = abs(p1 - p0) - abs(bitmex.get_bid(symbols[1]) - binance.get_ask(symbols[0]))

    if position == 'none' and spreads[-1] > sso and spread_diff > 0:
        #sell Y, buy beta*X
        position = 'short'
        trade_size = binance.round_qty(symbols[0], cash/p0)
        binance.buy(symbols[0], trade_size)
        bitmex.sell(symbols[1], (1/beta)*trade_size, default_leverage)

        shittosay = ('OPEN TRADE: Buy', symbols[0], 'at', p0, '. Short',
                     1/beta, symbols[1], 'at', p1, '\n')
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)

    elif position == 'short' and spreads[-1] < ssc:
        position = 'none'
        binance.close(symbols[0])
        bitmex.close(symbols[1])

        shittosay = ('CLOSE TRADE: Sell', symbols[0], 'at', p0, '. Buy',
                     symbols[1], 'at', p1, '\n')
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)

    return position

#params
start_time = datetime.datetime(2018, 3, 30, 0, 0, 0)
end_time = datetime.datetime.utcnow()
long_interval = '1h'
short_interval = '1h'
symbols = ['ETH', 'ETHM18']
x = 100 #moving average window size
y = 100 #std dev window size
default_leverage = 1
balance_limit = 0.85 #max percent of balance to use for initial position

#go
long_prices = pg.get_binance(start_time, end_time, [symbols[0]], long_interval)
short_prices = pg.get_bitmex(start_time, end_time, [symbols[1]], short_interval)
prices = pd.concat([long_prices[symbols[0]], short_prices[symbols[1]]], axis=1).dropna(axis=0, how='any')

bitmex = Bitmex(1)
binance = Binance()
#TODO: get existing portfolio
position = 'none'

print('Let\'s GET it')
while True:
    long_prices = pg.update_binance(long_prices, [symbols[0]], long_interval)
    short_prices = pg.update_bitmex(short_prices, [symbols[1]], short_interval)
    prices = pd.concat([long_prices[symbols[0]], short_prices[symbols[1]]], axis=1).dropna(axis=0, how='any')


    r = prices[symbols[1]]/prices[symbols[0]]
    m = r.ewm(min_periods = x, span = x).mean()
    m.fillna(0)
    s = r.rolling(window = y, center = False).std()
    s.fillna(1)
    z = (r-m)/s

    print('Z-score:', z[-1])

    position = realtime(z, prices, -1e8, 2, 0, 0, position)

    time.sleep(600)
