import requests as re
import numpy as np
import pandas as pd
import sys
import math
import datetime
import time
import matplotlib.pyplot as plt
import statsmodels.tsa.stattools as ts
import pricegetter as pg

#params
long_interval = '1h'
short_interval = '1h'
long_symbols = pg.get_binance_btc_market_tokens()
short_symbols = ['ADAH18', 'BCHH18', 'ETHH18', 'LTCH18', 'XRPH18']
start_time = datetime.datetime(2018, 1, 26, 0, 0, 0)
end_time = datetime.datetime(2018, 3, 28, 0, 0, 0)



long_prices = pg.get_binance(start_time, end_time, long_symbols, long_interval)
print('asdf')
short_prices = pg.get_bitmex(start_time, end_time, short_symbols, short_interval)

for long_coin in long_prices.columns:
    for short_coin in short_prices.columns:
        temp = pd.concat([long_prices[long_coin], short_prices[short_coin]], axis=1).dropna(axis=0, how='any')
        try:
            c = ts.coint(temp[long_coin], temp[short_coin])
        except ValueError:
            pass
        if c[1] < 0.05:
            print('\nSUCCESSFUL PAIR:', long_coin, short_coin)
            print('p-value:', c[1])
