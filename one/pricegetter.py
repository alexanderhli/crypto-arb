import pandas as pd
import requests as re
import datetime
import calendar
import time

def get_bitmex(start_time, end_time, symbols, interval):
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
                print('fuck')

            #put data into temporary arrays
            for datum in complete_data:
                current_time = datetime.datetime.strptime(datum['timestamp'], '%Y-%m-%dT%H:%M:%S.000Z')
                if current_time > end_time:
                    break
                temp_prices.append(float(datum['close']))
                temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

            #prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < datetime.timedelta(hours=1):
            	break

            temp_prices.pop()
            temp_times.pop()

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        if symbol == 'XBTUSD':
            series = 1.0/series
        prices.append(series)

    price_data = pd.concat(prices, axis=1)
    return price_data

# Usage:
# start_time, end_time - datetime
# symbols - str (ltc, xrp, etc.)
# interval - str (1/3/5/15/30min, 1/2/4/6hour)
def get_okex_spots(start_time, end_time, symbols, interval):
    prices = []
    requests = 0
    for symbol in symbols:
        page_start_time = start_time
        current_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
        temp_prices = []
        temp_times = []

        while True:
            #fetch data
            url = 'https://www.okex.com/api/v1/kline.do?' \
                  + 'symbol=' + symbol.lower() + '_btc' \
                  + '&type=' + interval \
                  + '&size=500' \
                  + '&since=' + str(int(calendar.timegm(page_start_time.timetuple()))*1000)

            complete_data = re.get(url).json()
            requests += 1
            if requests > 140:
                requests -= 30
                time.sleep(60)
                print('fuck')

            #put data into temporary arrays
            for datum in complete_data:
                current_time = datetime.datetime.utcfromtimestamp(int(datum[0])/1000)
                if current_time > end_time:
                    break
                temp_prices.append(float(datum[1]))
                temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

            #prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < datetime.timedelta(hours=1):
                break

            temp_prices.pop()
            temp_times.pop()

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        prices.append(series)

    price_data = pd.concat(prices, axis=1)
    return price_data


def get_okex_spots_simple(symbols, interval):
    prices = []
    requests = 0
    for symbol in symbols:
        temp_prices = []
        temp_times = []

        #fetch data
        url = 'https://www.okex.com/api/v1/kline.do?' \
              + 'symbol=' + symbol.lower() + '_btc' \
              + '&type=' + interval \

        complete_data = re.get(url).json()

        #put data into temporary arrays
        for datum in complete_data:
            current_time = datetime.datetime.utcfromtimestamp(int(datum[0])/1000)          
            temp_prices.append(float(datum[1]))
            temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        if len(series) == 2000:
            prices.append(series.groupby(series.index).first()) #remove duplicates

    price_data = pd.concat(prices, axis=1).dropna(axis=0, how='any')
    return price_data

# Usage:
# start_time, end_time - datetime
# symbols - str (btc, ltc, eth, etc, bch)
# interval - str (1/3/5/15/30min, 1/2/4/6hour)
def get_okex_futures(start_time, end_time, symbols, interval):
    prices = []
    requests = 0
    for symbol in symbols:
        page_start_time = start_time
        current_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
        temp_prices = []
        temp_times = []

        while True:
            #fetch data
            url = 'https://www.okex.com/api/v1/future_kline.do?' \
                  + 'symbol=' + symbol.lower() + '_usd' \
                  + '&type=' + interval \
                  + '&contract_type=quarter'\
                  + '&size=500' \
                  + '&since=' + str(int(calendar.timegm(page_start_time.timetuple()))*1000)

            complete_data = re.get(url).json()
            requests += 1
            if requests > 140:
                requests -= 30
                time.sleep(60)
                print('fuck')

            #put data into temporary arrays
            for datum in complete_data:
                current_time = datetime.datetime.utcfromtimestamp(int(datum[0])/1000)
                if current_time > end_time:
                    break
                temp_prices.append(float(datum[1]))
                temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

            #prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < datetime.timedelta(hours=1):
                break

            temp_prices.pop()
            temp_times.pop()

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        prices.append(series)

    price_data = pd.concat(prices, axis=1)
    return price_data

def get_binance(start_time, end_time, symbols, interval):
    prices = []
    requests = 0
    for symbol in symbols:
        page_start_time = start_time
        current_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
        temp_prices = []
        temp_times = []

        while True:
            #fetch data
            url = 'https://www.binance.com/api/v1/klines?' \
                  + 'symbol=' + symbol.upper() + 'BTC' \
                  + '&interval=' + interval \
                  + '&startTime=' + str(int(calendar.timegm(page_start_time.timetuple()))*1000)

            complete_data = re.get(url).json()
            requests += 1
            if requests > 140:
                requests -= 30
                time.sleep(60)
                print('fuck')


            #put data into temporary arrays
            for datum in complete_data:
                current_time = datetime.datetime.utcfromtimestamp(int(datum[0])/1000)
                if current_time > end_time:
                    break
                temp_prices.append(float(datum[1]))
                temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

            #prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < datetime.timedelta(hours=1):
                break

            temp_prices.pop()
            temp_times.pop()

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        prices.append(series)

    price_data = pd.concat(prices, axis=1)
    return price_data

#bitfinex api only provides last 1000
def get_bitfinex(symbols, interval):
    prices = []
    for symbol in symbols:
        temp_prices = []
        temp_times = []

        url = 'https://api.bitfinex.com/v2/candles/trade:'  \
              + interval \
              + ':t' + symbol + 'BTC' \
              + '/hist?limit=1000'

        complete_data = re.get(url).json()

        #put data into temporary arrays
        for datum in complete_data:
            current_time = datetime.datetime.utcfromtimestamp(int(datum[0])/1000)          
            temp_prices.append(float(datum[1]))
            temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        prices.append(series.groupby(series.index).first()) #remove duplicates

    price_data = pd.concat(prices, axis=1).dropna(axis=0, how='any')
    return price_data

#interval = 'hour'
#bittrex only return the last two months of data
def get_bittrex(symbols, interval):
    prices = []
    for symbol in symbols:
        temp_prices = []
        temp_times = []

        url = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?' \
              + 'marketName=BTC-' + symbol \
              + '&tickInterval=' + interval \
              + '&_=1500915289433'

        complete_data = re.get(url).json()

        #put data into temporary arrays
        for datum in complete_data['result']:
            current_time = datetime.datetime.strptime(datum['T'], '%Y-%m-%dT%H:%M:%S')       
            temp_prices.append(float(datum['O']))
            temp_times.append(datetime.datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S'))

        series = pd.Series(temp_prices, index=temp_times, name=symbol)
        prices.append(series.groupby(series.index).first()) #remove duplicates

    price_data = pd.concat(prices, axis=1).dropna(axis=0, how='any')
    return price_data

def update_bitmex(old_prices, symbols, interval):
    start = datetime.datetime.strptime(old_prices.index[-2], '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.utcnow()
    symbols = old_prices.columns
    new_prices = get_bitmex(start, end, symbols, interval)
    updated = pd.concat([old_prices, new_prices], axis=0)
    updated = updated[~updated.index.duplicated(keep='first')] #remove duplicates
    return updated

def update_okex_spots(old_prices, symbols, interval):
    start = datetime.datetime.strptime(old_prices.index[-2], '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.utcnow()
    symbols = old_prices.columns
    new_prices = get_okex_spots(start, end, symbols, interval)
    updated = pd.concat([old_prices, new_prices], axis=0)
    updated = updated[~updated.index.duplicated(keep='first')] #remove duplicates
    return updated

def update_okex_futures(old_prices, symbols, interval):
    start = datetime.datetime.strptime(old_prices.index[-2], '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.utcnow()
    symbols = old_prices.columns
    new_prices = get_okex_futures(start, end, symbols, interval)
    updated = pd.concat([old_prices, new_prices], axis=0)
    updated = updated[~updated.index.duplicated(keep='first')] #remove duplicates
    return updated

def update_binance(old_prices, symbols, interval):
    start = datetime.datetime.strptime(old_prices.index[-2], '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.utcnow()
    symbols = old_prices.columns
    new_prices = get_binance(start, end, symbols, interval)
    updated = pd.concat([old_prices, new_prices], axis=0)
    updated = updated[~updated.index.duplicated(keep='first')] #remove duplicates
    return updated

def update_bittrex(old_prices, symbols, interval):
    start = datetime.datetime.strptime(old_prices.index[-2], '%Y-%m-%d %H:%M:%S')
    end = datetime.datetime.utcnow()
    symbols = old_prices.columns
    new_prices = get_bittrex(symbols, interval)
    updated = pd.concat([old_prices, new_prices], axis=0)
    updated = updated[~updated.index.duplicated(keep='first')] #remove duplicates
    return updated

def get_okex_spot_tokens():
    return ['ltc', 'eth', 'etc', 'bch', '1st', 'aac', 'abt', 
            'ace', 'act', 'aidoc', 'amm', 'ark', 'ast', 'auto', 
            'avt', 'bcd', 'bcx', 'bec', 'bkx', 'bnt', 'brd', 
            'bt2', 'btg', 'btm', 'can', 'cbt', 'chat', 'cic', 
            'cmt', 'cvc', 'dadi', 'dash', 'dat', 'dent', 'dgb', 
            'dgd', 'dna', 'dnt', 'dpy', 'edo', 'elf', 'eng', 
            'enj', 'eos', 'evx', 'fun', 'gas', 'gnt', 'gnx', 
            'gsc', 'gtc', 'gto', 'hot', 'hsr', 'icn', 'icx', 
            'ins', 'insur', 'int', 'iost', 'iota', 'ipc', 'itc', 
            'kcash', 'knc', 'lend', 'lev', 'light', 'link', 
            'lrc', 'mana', 'mco', 'mdt', 'mith', 'mkr', 'mof', 
            'mth', 'mtl', 'nano', 'nas', 'neo', 'nuls', 'oax', 
            'of', 'okb', 'omg', 'ont', 'ost', 'pay', 'poe', 
            'ppt', 'pra', 'pst', 'qtum', 'qun', 'r', 'rcn', 
            'rdn', 'ref', 'ren', 'req', 'rfr', 'rnt', 'salt', 
            'san', 'sbtc', 'smt', 'snc', 'sngls', 'snm', 'snt', 
            'spf', 'ssc', 'storj', 'sub', 'swftc', 'tct', 
            'theta', 'tio', 'tnb', 'topc', 'tra', 'trio', 'true', 
            'trx', 'ubtc', 'uct', 'ugc', 'utk', 'vee', 'vib', 
            'viu', 'wbtc', 'wtc', 'xem', 'xlm', 'xmr', 'xrp', 
            'xuc', 'yee', 'yoyo', 'zec', 'zen', 'zip', 'zrx']

def get_okex_futures_tokens():
    return ['btc', 'ltc', 'eth', 'etc', 'bch']

def get_binance_btc_market_tokens():
    return ['ADA', 'ADX', 'AE', 'AION', 'AMB', 'APPC', 'ARK', 
            'ARN', 'AST', 'BAT', 'BCC', 'BCD', 'BCPT', 'BLZ', 
            'BNB', 'BNT', 'BQX', 'BRD', 'BTG', 'BTS', 'CDT', 
            'CHAT', 'CMT', 'CND', 'DASH', 'DGD', 'DLT', 'DNT', 
            'EDO', 'ELF', 'ENG', 'ENJ', 'EOS', 'ETC', 'ETH', 
            'EVX', 'FUEL', 'FUN', 'GAS', 'GRS', 'GTO', 'GVT', 
            'GXS', 'HSR', 'ICN', 'ICX', 'INS', 'IOST', 'IOTA', 
            'KMD', 'KNC', 'LEND', 'LINK', 'LRC', 'LSK', 'LTC', 
            'LUN', 'MANA', 'MCO', 'MDA', 'MOD', 'MTH', 'MTL', 
            'NANO', 'NAV', 'NCASH', 'NEBL', 'NEO', 'NULS', 
            'OAX', 'OMG', 'ONT', 'OST', 'PIVX', 'POA', 'POE', 
            'POWR', 'PPT', 'QLC', 'QSP', 'QTUM', 'RCN', 'RDN', 
            'REQ', 'RLC', 'RPX', 'SALT', 'SNGLS', 'SNM', 'SNT', 
            'STEEM', 'STORJ', 'STORM', 'STRAT', 'SUB', 'SYS', 
            'TNB', 'TNT', 'TRIG', 'TRX', 'VEN', 'VIA', 'VIB', 
            'VIBE', 'WABI', 'WAN', 'WAVES', 'WINGS', 'WPR', 
            'WTC', 'XEM', 'XLM', 'XMR', 'XRP', 'XVG', 'XZC', 
            'YOYO', 'ZEC', 'ZIL', 'ZRX']
