import requests as re
import numpy as np
import pandas as pd
import sys
import math
import datetime
import time
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

#params
start_time = datetime.datetime(2018, 1, 26, 0, 0, 0)
end_time = datetime.datetime(2018, 3, 1, 0, 0, 0)
interval = '1h'
n_factors = 3
training_size = 1000
#symbols = ['ADAH18', 'BCHH18', 'DASHH18', 'ETC7D', 'ETHH18', 'LTCH18', 
#               'XBTUSD', 'XLMH18', 'XMRH18', 'XRPH18', 'ZECH18']

symbols = ['XRPH18', 'XMRH18', 'XLMH18']
#temporarily unused
k = 8             #mean reversion speed (don't worry until data is here)
num_periods = 288 #data points within "unit" of trade ie one day



#############DATA GETTING/TRAINING METHODS###############

#fetch training and testing data
def get_bitmex(_start_time, _end_time):
    prices = [] #hold the series
    requests = 0
    for symbol in symbols:
        #put price data into series
        page_start_time = _start_time
        current_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
        temp_prices = []
        temp_times = []
        while page_start_time < _end_time:
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
            temp_prices.pop()
            temp_times.pop()

        #put times and prices into series; take reciprocal if it's bitcoin
        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        if symbol == 'XBTUSD':
            series = 1.0/series
        prices.append(series)

    price_data = pd.concat(prices, axis=1)
    returns = (price_data - price_data.shift(1))/price_data
    returns = returns.drop(returns.index[0])
    return returns

#perform pca on training data with n_factors number of components
def get_factors(training_data, num_factors, training_size):
    mean = training_data.mean(axis=0)
    std = training_data.std(axis=0)
    std_ret = (training_data-mean)/std

    pca = PCA(n_components=num_factors)
    pca.fit(std_ret)
    print pca.explained_variance_ratio_
    weight = pd.DataFrame(pca.components_, columns=std.index)
    weight = weight / std
    print weight
    return weight

###################TESTING HELPER METHODS##############################

#calculate returns of eigenportfolios as applied to testing data
def get_factor_returns(returns, weights):
    return pd.DataFrame(np.dot(returns, weights.transpose()), index=returns.index)

#calculate the residuals given factor returns
def get_residuals(returns, factor_returns):
    res = pd.DataFrame(columns = returns.columns, index = returns.index)
    coef = pd.DataFrame(columns = returns.columns, index = range(n_factors))
    ols = LinearRegression()
    for i in returns.columns:
        ols.fit(factor_returns, returns[i])
        res[i] = returns[i]-ols.intercept_-np.dot(factor_returns, ols.coef_)
        coef[i] = ols.coef_
    return res,coef

#calculate the sscore of each coin given residuals data
def find_sscore(res):
    cum_res = res.cumsum()
    m = pd.Series(index = cum_res.columns)
    sigma_eq = pd.Series(index = cum_res.columns)
    for i in cum_res.columns:
        b = cum_res[i].autocorr()
        temp = (cum_res[i]-cum_res[i].shift(1)* b)[1:]
        a = temp.mean()
        cosi =temp - a
        m[i] = a/(1-b)
        sigma_eq[i]=math.sqrt(cosi.var()/(1-b*b))
    m = m.dropna()
    m = m - m.mean()
    sigma_eq = sigma_eq.dropna()
    s_score = -m/sigma_eq
    return s_score

###################TESTING METHODS##############################

#graph the sscores of all coins given returns data
def graph_sscores(returns, factor_rets, window):
    temp = []
    for t in range(training_size, len(returns.index) - window - 1):
        returns_t = returns[t:(t+window)]
        factor_rets_t = factor_rets[t:(t+window)]
        residuals_t, coefs_t = get_residuals(returns_t, factor_rets_t)

        ss = find_sscore(residuals_t)
        ss.name = returns.index[t]
        temp.append(ss)

    sscores = pd.concat(temp, axis=1).T
    for i in sscores.columns:
        plt.figure()
        plt.plot(sscores[i])

    plt.show()

#backtest for individual coins
def backtest_separate(returns, factor_rets, window, sbo, sso, sbc, ssc):
    temp = []
    temp.append(pd.Series(100, index=symbols))
    coin_positions = pd.Series(0, index=symbols)
    hedge_positions = pd.DataFrame(0, index=range(n_factors), columns=symbols)
    for t in range(training_size, len(returns.index) - window - 1):
        returns_t = returns[t:(t+window)] #leading up to current element
        factor_rets_t = factor_rets[t:(t+window)]
        residuals_t, coefs_t = get_residuals(returns_t, factor_rets_t)
        ss = find_sscore(residuals_t)
        print ss

        #take positions
        for coin in coin_positions.index:
            if not coin in ss.index:
                coin_positions[coin] = 0
                hedge_positions[coin] = 0
            else:
                if coin_positions[coin] == 0:
                    if ss[coin] < -sbo:
                        coin_positions[coin] = 1
                        hedge_positions[coin] = -1*coefs_t[coin]
                    elif ss[coin] > sso:
                        coin_positions[coin] = -1
                        hedge_positions[coin] = coefs_t[coin]
                elif coin_positions[coin] > 0 and ss[coin] > -sbc:
                    coin_positions[coin] = 0
                    hedge_positions[coin] = 0
                elif coin_positions[coin] < 0 and ss[coin] < ssc:
                    coin_positions[coin] = 0
                    hedge_positions[coin] = 0

        #calculate and record returns
        r = coin_positions*returns_t.iloc[-1] + np.dot(factor_rets_t.iloc[-1], hedge_positions)
        new_money = temp[-1]*(1+r)
        new_money.name = returns.index[t+window-1]
        temp.append(new_money)

    money = pd.concat(temp, axis=1).T

    print money.iloc[-1]

    for i in money.columns:
        plt.figure()
        plt.plot(money[i])

    plt.show()

#vary window, sbo=sso, sbc, ssc
def experiment(rets, fac_rets):
    for _window in range(20, 360, 20):
        window = _window
        for so in np.arange(1.5, 3, 0.25):
            sbo = so
            sso = so
            for sc in np.arange(0.5, 2, 0.25):
                sbc = sc
                ssc = sc
                print 'window: ' + str(_window)
                print 'so: ' + str(so)
                print 'sc: ' + str(sc)
                backtest_separate(rets, fac_rets, window, sbo, sso, sbc, ssc)

rets = get_bitmex(start_time, end_time)
wts = get_factors(rets, n_factors, training_size)
fac_rets = get_factor_returns(rets, wts)
#backtest_separate(rets, fac_rets, 20, 7, 7, 4, 4)
graph_sscores(rets, fac_rets, 20)

#print 'commencing experiment'
#experiment(rets, fac_rets)
