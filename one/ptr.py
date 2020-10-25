import requests as re
import numpy as np
import pandas as pd
import time
import datetime
import pricegetter as pg


def backtest(zscores, prices, sso, ssc):
    money = []
    trades = 0
    longs = {symbols[0]: 0, symbols[1]: 0}
    shorts = {symbols[0]: 0, symbols[1]: 0}
    cash = 100.0
    position = 'none'
    increased = False
    for t in range(0, len(zscores) - 1):
        p0 = prices[symbols[0]][t]
        p1 = prices[symbols[1]][t]
        time = start_time + t * datetime.timedelta(minutes = 5) 

        # Debug 1a
        # print(time, longs[symbols[0]], shorts[symbols[1]])

        if position == 'none' and zscores[t] > sso:
            # Debug 1b
            # print(time, 'open')
            position = 'short'
            longs[symbols[0]] = cash / 2 / p0
            shorts[symbols[1]] = cash / 2 / p1
            cash = cash - p0*longs[symbols[0]] + p1*shorts[symbols[1]]
            trades += 1
            cash -= cash * fee

        elif position == 'short' and zscores[t] < ssc:
            # Debug 1b
            # print(time, 'close')
            position = 'none'
            cash = cash + p0 * longs[symbols[0]] - p1 * shorts[symbols[1]]
            shorts[symbols[1]] = 0
            longs[symbols[0]] = 0
            increased = False
            trades += 1
            cash -= cash * fee

        elif position == 'short' and zscores[t] > ssc:
            if zscores[t] > z_threshold and increased == False:
                cash = (cash -
                        (elasticity - 1) * p0 * longs[symbols[0]] +
                        (elasticity - 1) * p1 * shorts[symbols[1]])
                longs[symbols[0]] = longs[symbols[0]] * elasticity
                shorts[symbols[1]] = shorts[symbols[1]] * elasticity
                trades += 1
                cash -= cash * fee
                increased = True

        longtotal = longs[symbols[0]]*p0 + longs[symbols[1]]*p1
        shorttotal = shorts[symbols[0]]*p0 + shorts[symbols[1]]*p1
        
        # Debug 1c
        # if len(money) > 1:
        #     print(time, cash+longtotal-shorttotal-money[-1])

        money.append(cash+longtotal-shorttotal)

    pd.Series(money)[x:].to_csv('money.csv')
    print('\n' + str((money[-1]-money[0])/money[0]*100) + '% return')
    print(str(trades) + ' trades made')


start_time = datetime.datetime(2018, 4, 1, 0, 0, 0)
end_time = datetime.datetime(2018, 5, 24, 0, 0, 0)
long_interval = '5m'
short_interval = '5m'
symbols = ['ADA', 'ADAM18']  # first is long, second is short
x = 288  # moving average window size
elasticity = 2
z_threshold = 1000
fee = 0.00

#long_prices = pg.get_okex_spots_simple([symbols[0]], long_interval)
#long_prices = pg.get_bitfinex([symbols[0]], long_interval)
#long_prices = pg.get_bittrex([symbols[0]], long_interval)
long_prices = pg.get_binance(start_time, end_time, [symbols[0]], long_interval)
short_prices = pg.get_bitmex(start_time, end_time, [symbols[1]], short_interval)
#short_prices = pg.get_bitfinex([symbols[0]], long_interval)
prices = pd.concat([long_prices[symbols[0]], short_prices[symbols[1]]],
                   axis=1).dropna(axis=0, how='any')


r = prices[symbols[1]]/prices[symbols[0]]
m = r.rolling(window=x).mean()
#m = r.ewm(span=x, min_periods=x).mean()
s = r.rolling(window=x, center=False).std()
z = (r-m)/s

backtest(z, prices, 2, -2)
