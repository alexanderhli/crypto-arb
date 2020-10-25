import numpy as np
import pandas as pd
import requests as re
import numpy as np
import sys
import math
import datetime
import time
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

def get_bitmex(start_time, end_time):
    prices = []
    requests = 0
    for symbol in symbols:
        page_start_time = start_time
        current_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
        temp_prices = []
        temp_times = []

        while True:
            #fetch data
            url = 'https://www.bitmex.com/api/v1/trade/bucketed?binSize=' + interval \
                  + '&partial=false&symbol=' + symbol + '&count=500&startTime=' \
                  + datetime.datetime.strftime(page_start_time, '%Y-%m-%d%%20%H%%3A%M')
            complete_data = re.get(url).json()
            requests += 1
            if requests > 140:
                requests -= 30
                time.sleep(60)
                print 'fuck'

            #put data into temporary arrays
            for datum in complete_data:
                current_time = datetime.datetime.strptime(datum['timestamp'], '%Y-%m-%dT%H:%M:%S.000Z')
                if current_time > end_time:
                    break
                temp_prices.append(float(datum['open']))
                temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

            #prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < datetime.timedelta(hours=1):
            	break

            temp_prices.pop()
            last_time = temp_times.pop()
            

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        if symbol == 'XBTUSD':
            series = 1.0/series
        prices.append(series)

    price_data = pd.concat(prices, axis=1)
    return price_data

def update_bitmex(old_prices):
    start = datetime.datetime.strptime(old_prices.index[-1], '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.utcnow()
    new_prices = get_bitmex(start, end)
    updated = pd.concat([old_prices, new_prices], axis=0).drop_duplicates()
    return updated

def get_beta(prices):
    training_prices_x = prices[symbols[0]].to_frame()
    training_prices_y = prices[symbols[1]].to_frame()
    training_prices_x = training_prices_x[:len(training_prices_x)/2]
    training_prices_y = training_prices_y[:len(training_prices_y)/2]
    ols = LinearRegression()
    ols.fit(training_prices_x, training_prices_y)

    beta = ols.coef_[0][0]

    return beta

def get_spreads(beta, prices, graph):
    spreads = []
    for t in prices.index:
        spread = prices.loc[t][symbols[1]] - beta*prices.loc[t][symbols[0]]
        spreads.append(spread)

    fig = graph.add_subplot(231)
    fig.title.set_text("Spread")
    fig.plot(spreads)

    return spreads

def get_microtrades(spreads, graph):
    x = pd.DataFrame(spreads)

    #m = x.rolling(window=100, center=False).mean()
    m = x.ewm(min_periods = 500, span=500).mean()
    m.fillna(0)
    fig = figure.add_subplot(232)
    fig.title.set_text('Spread moving average')
    plt.plot(m)

    s = x.rolling(window=1500, center=False).std()
    s.fillna(1)
    fig = figure.add_subplot(233)
    fig.title.set_text('Spread standard dev')
    plt.plot(s)

    z = (x-m)/s
    fig = figure.add_subplot(234)
    fig.title.set_text('Spread z score')
    plt.plot(z)

    return z.iloc[:,0].values.tolist()

def realtime(spreads, zscores, beta, prices, macro_cutoffs, micro_cutoffs, position):
    macro_sbo = macro_cutoffs[0]
    macro_sso = macro_cutoffs[1]
    macro_sbc = macro_cutoffs[2]
    macro_ssc = macro_cutoffs[3]

    micro_sbo = micro_cutoffs[0]
    micro_sso = micro_cutoffs[1]
    micro_sbc = micro_cutoffs[2]
    micro_ssc = micro_cutoffs[3]

    p0 = prices[symbols[0]][-1]
    p1 = prices[symbols[1]][-1]

    #open
    if position == 'none' and spreads[-1] < macro_sbo:
        #buy Y, sell beta*X
        position = 'macro long'
        print 'OPEN MACROTRADE:'
        print 'buy', symbols[1], 'at', p1
        print 'short', beta, symbols[0], 'at', p0
    elif position == 'none' and spreads[-1] > macro_sso:
        #sell Y, buy beta*X
        position = 'macro short'
        print 'OPEN MACROTRADE:'
        print 'buy', symbols[0], 'at', p0
        print 'short', 1/beta, symbols[1], 'at', p1
    elif position == 'none' and zscores[-1] < micro_sbo:
        position = 'micro long'
        print 'OPEN MICROTRADE:'
        print 'buy', symbols[1], 'at', p1
        print 'short', beta, symbols[0], 'at', p0
    elif position == 'none' and zscores[-1] > micro_sso:
        position = 'micro short'
        print 'OPEN MACROTRADE:'
        print 'buy', symbols[0], 'at', p0
        print 'short', 1/beta, symbols[1], 'at', p1

    #close
    elif position == 'macro long' and spreads[-1] > macro_sbc:
        position = 'none'
        print 'CLOSE MACROTRADE:'
        print 'sell', symbols[1], 'at', p1
        print 'buy', beta, symbols[0], 'at', p0
        print ''
    elif position == 'macro short' and spreads[-1] < macro_ssc:
        position = 'none'
        print 'CLOSE MACROTRADE:'
        print 'sell', symbols[0], 'at', p0
        print 'buy', 1/beta, symbols[1], 'at', p1
        print ''
    elif position == 'micro long' and zscores[-1] > micro_sbc:
        position = 'none'
        print 'CLOSE MICROTRADE:'
        print 'sell', symbols[1], 'at', p1
        print 'buy', beta, symbols[0], 'at', p0
        print ''
    elif position == 'micro short' and zscores[-1] < micro_ssc:
        position = 'none'
        print 'CLOSE MICROTRADE:'
        print 'sell', symbols[0], 'at', p0
        print 'buy', 1/beta, symbols[1], 'at', p1
        print ''

    return position

#params
start_time = datetime.datetime(2018, 1, 26, 0, 0, 0)
end_time = datetime.datetime.utcnow()
interval = '5m'
symbols = ['XLMH18', 'ADAH18']
leverage = 1



figure = plt.figure()

prices = get_bitmex(start_time, end_time)
beta = get_beta(prices)
spreads = get_spreads(beta, prices, figure)
zscores = get_microtrades(spreads, figure)

macrocutoffs = [-0.000018, -0.000008, -0.000013, -0.000013]
microcutoffs = [-2, 2, 0, 0]
position = 'none'

print 'Let\'s GET it'
while True:
    prices = update_bitmex(prices)
    beta = get_beta(prices)
    spreads = get_spreads(beta, prices, figure)
    zscores = get_microtrades(spreads, figure)

    print 'Spread:', spreads[-1]
    print 'Z-score:', zscores[-1]

    position = realtime(spreads, zscores, beta, prices, macrocutoffs, microcutoffs, position)
    
    time.sleep(300)
