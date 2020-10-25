import requests as re
import numpy as np
import pandas as pd
import math
import datetime
import time
import pricegetter as pg
import utilities as utils
from exchanges import Bitmex

#do the shit
def realtime(spreads, prices, sbo, sso, sbc, ssc, z_thresh,
             position, portfolio, current_leverage):
    p0 = prices[symbols[0]][-1]
    p1 = prices[symbols[1]][-1]
    beta = p1/p0    
    cash = balance_limit*bitmex.get_balance()
    
    if position == 'none' and spreads[-1] < sbo:
        #buy Y, sell beta*X
        position = 'long'
        trade_size1 = round(cash/p1, 0)
        trade_size0 = round(beta*trade_size1, 0)
        portfolio[1] = bitmex.buy(symbols[1], trade_size1, default_leverage)
        portfolio[0] = bitmex.sell(symbols[0], trade_size0, default_leverage)

        shittosay = ('OPEN TRADE: Buy', symbols[1], 'at', p1, '. Short',
                     beta, symbols[0], 'at', p0, '\n')
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)
        
        z_thresh = sbo - z_increment
        current_leverage = default_leverage

    elif position == 'none' and spreads[-1] > sso:
        #sell Y, buy beta*X
        position = 'short'
        trade_size0 = round(cash/p0, 1)
        trade_size1 = round((1/beta)*trade_size0, 0)
        portfolio[0] = bitmex.buy(symbols[0], trade_size0, default_leverage)
        portfolio[1] = bitmex.sell(symbols[1], trade_size1, default_leverage)

        shittosay = ('OPEN TRADE: Buy', symbols[0], 'at', p0, '. Short',
                     1/beta, symbols[1], 'at', p1, '\n')
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)

        z_thresh = sso + z_increment
        current_leverage = default_leverage

    elif position == 'long' and spreads[-1] > sbc:
        position = 'none'
        portfolio[0] = bitmex.close(symbols[0])
        portfolio[1] = bitmex.close(symbols[1])

        shittosay = ('CLOSE TRADE: Sell', symbols[1], 'at', p1, '. Buy',
                     symbols[0], 'at', p0, '\n')
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)

    elif position == 'short' and spreads[-1] < ssc:
        position = 'none'
        portfolio[0] = bitmex.close(symbols[0])
        portfolio[1] = bitmex.close(symbols[1])

        shittosay = ('CLOSE TRADE: Sell', symbols[0], 'at', p0, '. Buy',
                     symbols[1], 'at', p1, '\n')
        shittosay = ' '.join(map(str, shittosay))
        print(shittosay)
        utils.twilio_message(shittosay)
    
    # position adjustment conditions
    elif position == 'long' and spreads[-1] < sbc:
        if spreads[-1] < z_thresh:
            z_thresh -= z_increment
            current_leverage *= elasticity
            change1 = abs((elasticity-1)*portfolio[1])
            change0 = abs((elasticity-1)*portfolio[0])
            portfolio[1] += bitmex.buy(symbols[1], change1, current_leverage)
            portfolio[0] += bitmex.sell(symbols[0], change0, current_leverage)

            shittosay = 'ENHANCE TRADE'
            print(shittosay)
            utils.twilio_message(shittosay)

    elif position == 'short' and spreads[-1] > ssc:
        if spreads[-1] > z_thresh:
            z_thresh += z_increment
            current_leverage *= elasticity
            change0 = abs((elasticity-1)*portfolio[0])
            change1 = abs((elasticity-1)*portfolio[1])
            portfolio[0] += bitmex.buy(symbols[0], change0, current_leverage)
            portfolio[1] += bitmex.sell(symbols[1], change1, current_leverage)

            shittosay = 'ENHANCE TRADE'
            print(shittosay)
            utils.twilio_message(shittosay)

    return z_thresh, position, portfolio, current_leverage

#params
start_time = datetime.datetime(2018, 3, 29, 0, 0, 0)
end_time = datetime.datetime.utcnow()
interval = '1h'
symbols = ['XRPM18', 'BCHM18']
x = 100 #moving average window size
y = 100 #std dev window size
elasticity = 2
z_increment = 2
default_leverage = 2
balance_limit = 0.85 #max percent of balance to use for initial position

#go
prices = pg.get_bitmex(start_time, end_time, symbols, interval)
bitmex = Bitmex(0)
#zthresh, position, portfolio, currlev = bitmex.get_existing_portfolio()
zthresh = 0
position = 'none'
portfolio = [0, 0]
currlev = default_leverage

print('Let\'s GET it')
while True:
    prices = pg.update_bitmex(prices, symbols, interval)
    r = prices[symbols[1]]/prices[symbols[0]]
    m = r.ewm(min_periods = x, span = x).mean()
    m.fillna(0)
    s = r.rolling(window = y, center = False).std()
    s.fillna(1)
    z = (r-m)/s

    print('Z-score:', z[-1])

    zthresh, position, portfolio, currlev = realtime(z, prices, -2, 2, 0, 0, zthresh, 
                                            position, portfolio, currlev)

    time.sleep(600)

