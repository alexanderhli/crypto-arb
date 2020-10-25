import requests as re
import numpy as np
import pandas as pd
import sys
import math
import datetime
import time
import matplotlib.pyplot as plt
import statsmodels.tsa.stattools as ts
from sklearn.linear_model import LinearRegression
import pricegetter as pg

#get beta as a constant value based on the first quarter of the prices
def get_beta(prices):
    training_prices_x = prices[symbols[0]].to_frame()
    training_prices_y = prices[symbols[1]].to_frame()
    training_prices_x = training_prices_x[:len(training_prices_x)/4]
    training_prices_y = training_prices_y[:len(training_prices_y)/4]
    ols = LinearRegression()
    ols.fit(training_prices_x, training_prices_y)

    beta = ols.coef_[0][0]

    return beta

#spreads based on beta and prices
def get_spreads(beta, prices, graph):
    spreads = []
    for t in prices.index:
        spread = prices.loc[t][symbols[1]] - beta*prices.loc[t][symbols[0]]
        spreads.append(spread)

    print '\nAugmented Dickey-Fuller results:'
    print ts.adfuller(spreads, 1)

    fig = graph.add_subplot(231)
    fig.title.set_text("Spread")
    fig.plot(spreads)

    return spreads

#zscores for microtrades
#x is ma window
#y is standard deviation window
def get_microtrades(spreads, graph, x, y):
    a = pd.DataFrame(spreads)

    m = a.ewm(min_periods = x, span=x).mean()
    m.fillna(0)
    fig = figure.add_subplot(232)
    fig.title.set_text('Spread moving average')
    plt.plot(m)

    s = a.rolling(window=y, center=False).std()
    s.fillna(1)
    fig = figure.add_subplot(233)
    fig.title.set_text('Spread standard dev')
    plt.plot(s)

    z = (a-m)/s
    fig = figure.add_subplot(234)
    fig.title.set_text('Spread z score')
    plt.plot(z)

    return z.iloc[:,0].values.tolist()

#backtest with macrotrades only
def backtest(spreads, beta, prices, sbo, sso, sbc, ssc, graph):
    money = []
    trades = 0
    longs = {symbols[0]:0, symbols[1]:0}
    shorts = {symbols[0]:0, symbols[1]:0} 
    cash = 100
    position = 'none'
    for t in range(0, len(spreads) - 1):
        p0 = prices[symbols[0]][t]
        p1 = prices[symbols[1]][t]

        if position == 'none' and spreads[t] < sbo:
            #buy Y, sell beta*X
            position = 'long'
            longs[symbols[1]] = cash*leverage/p1
            shorts[symbols[0]] = beta*longs[symbols[1]]
            cash = cash - p1*longs[symbols[1]] + p0*shorts[symbols[0]]
            trades += 1
        elif position == 'none' and spreads[t] > sso:
            #sell Y, buy beta*X
            position = 'short'
            longs[symbols[0]] = cash*leverage/p0
            shorts[symbols[1]] = (1/beta)*longs[symbols[0]]
            cash = cash - p0*longs[symbols[0]] + p1*shorts[symbols[1]]
            trades += 1
        elif position == 'long' and spreads[t] > sbc:
            position = 'none'
            cash = cash + p1*longs[symbols[1]] - p0*shorts[symbols[0]]
            longs[symbols[1]] = 0
            shorts[symbols[0]] = 0
            trades += 1
        elif position == 'short' and spreads[t] < ssc:
            position = 'none'
            cash = cash + p0*longs[symbols[0]] - p1*shorts[symbols[1]]
            shorts[symbols[1]] = 0
            longs[symbols[0]] = 0
            trades += 1


        longtotal = longs[symbols[0]]*p0 + longs[symbols[1]]*p1
        shorttotal = shorts[symbols[0]]*p0 + shorts[symbols[1]]*p1        

        money.append(cash+longtotal-shorttotal)

    print '\n' + str((money[-1]-money[0])/money[0]*100) + '% return'
    print str(trades) + ' trades made'
    fig = graph.add_subplot(235)
    fig.title.set_text("Money")
    fig.plot(money)

#backtest with both micro and macro trades
def double_backtest(spreads, zscores, beta, prices, macro_cutoffs, micro_cutoffs, graph):
    macro_sbo = macro_cutoffs[0]
    macro_sso = macro_cutoffs[1]
    macro_sbc = macro_cutoffs[2]
    macro_ssc = macro_cutoffs[3]

    micro_sbo = micro_cutoffs[0]
    micro_sso = micro_cutoffs[1]
    micro_sbc = micro_cutoffs[2]
    micro_ssc = micro_cutoffs[3]

    money = []
    trades = 0
    longs = {symbols[0]:0, symbols[1]:0}
    shorts = {symbols[0]:0, symbols[1]:0} 
    cash = 100
    position = 'none'

    for t in range(0, len(spreads) - 1):
        p0 = prices[symbols[0]][t]
        p1 = prices[symbols[1]][t]

        #open
        if position == 'none' and spreads[t] < macro_sbo:
            #buy Y, sell beta*X
            position = 'macro long'
            longs[symbols[1]] = cash*leverage/p1
            shorts[symbols[0]] = beta*longs[symbols[1]]
            cash = cash - p1*longs[symbols[1]] + p0*shorts[symbols[0]]
            trades += 1
        elif position == 'none' and spreads[t] > macro_sso:
            #sell Y, buy beta*X
            position = 'macro short'
            longs[symbols[0]] = cash*leverage/p0
            shorts[symbols[1]] = (1/beta)*longs[symbols[0]]
            cash = cash - p0*longs[symbols[0]] + p1*shorts[symbols[1]]
            trades += 1
        elif position == 'none' and zscores[t] < micro_sbo:
            position = 'micro long'
            longs[symbols[1]] = cash*leverage/p1
            shorts[symbols[0]] = beta*longs[symbols[1]]
            cash = cash - p1*longs[symbols[1]] + p0*shorts[symbols[0]]
            trades += 1
        elif position == 'none' and zscores[t] > micro_sso:
            position = 'micro short'
            longs[symbols[0]] = cash*leverage/p0
            shorts[symbols[1]] = (1/beta)*longs[symbols[0]]
            cash = cash - p0*longs[symbols[0]] + p1*shorts[symbols[1]]
            trades += 1

        #close
        elif position == 'macro long' and spreads[t] > macro_sbc:
            position = 'none'
            cash = cash + p1*longs[symbols[1]] - p0*shorts[symbols[0]]
            longs[symbols[1]] = 0
            shorts[symbols[0]] = 0
            trades += 1
        elif position == 'macro short' and spreads[t] < macro_ssc:
            position = 'none'
            cash = cash + p0*longs[symbols[0]] - p1*shorts[symbols[1]]
            shorts[symbols[1]] = 0
            longs[symbols[0]] = 0
            trades += 1
        elif position == 'micro long' and zscores[t] > micro_sbc:
            position = 'none'
            cash = cash + p1*longs[symbols[1]] - p0*shorts[symbols[0]]
            longs[symbols[1]] = 0
            shorts[symbols[0]] = 0
            trades += 1
        elif position == 'micro short' and zscores[t] < micro_ssc:
            position = 'none'
            cash = cash + p0*longs[symbols[0]] - p1*shorts[symbols[1]]
            shorts[symbols[1]] = 0
            longs[symbols[0]] = 0
            trades += 1


        longtotal = longs[symbols[0]]*p0 + longs[symbols[1]]*p1
        shorttotal = shorts[symbols[0]]*p0 + shorts[symbols[1]]*p1        

        money.append(cash+longtotal-shorttotal)

    print '\n' + str((money[-1]-money[0])/money[0]*100) + '% return'
    print str(trades) + ' trades made'
    fig = graph.add_subplot(236)
    fig.title.set_text("Money")
    fig.plot(money)

#params
start_time = datetime.datetime(2018, 1, 26, 0, 0, 0)
end_time = datetime.datetime(2018, 3, 23, 0, 0, 0)
interval = '1h'
symbols = ['XRPH18', 'BCHH18']
leverage = 1
x = 500
y = 1500


figure = plt.figure()

prices = pg.get_bitmex(start_time, end_time, symbols, interval)
beta = get_beta(prices)
print beta
spreads = get_spreads(beta, prices, figure)
zscores = get_microtrades(spreads, figure, x, y)

#backtest(spreads, beta, prices, -0.000018, -0.000008, -0.000013, -0.000013, figure)
#backtest(zscores, beta, prices, -2, 2, 0, 0, figure)

macrocutoffs = [-0.000018, -0.000008, -0.000013, -0.000013]
microcutoffs = [-2, 2, 0, 0]
#double_backtest(spreads, zscores, beta, prices, macrocutoffs, microcutoffs, figure)

plt.show()
