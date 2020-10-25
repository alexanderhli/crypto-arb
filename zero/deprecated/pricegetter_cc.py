import pandas as pd
import requests as re
import time
import calendar
import yaml
import os
import bitmex
from dateutil import parser
from datetime import datetime, timedelta, timezone
import pytz

import utilities as utils


class PriceGetter(object):

    ########################
    #    PUBLIC METHODS    #
    ########################

    def __init__(self, params_file='params.yaml'):
        """Initialize a PriceGetter object with specified parameters file."""
        self.apiKey = utils.load_auth('coinapi', 'li')['key']
        self.params = utils.load_params(params_file)
        self.BITMEX_EXPIRATION = self.params['bitmex_exp_code']
        self.interval = self.params['interval']
        if self.interval == '1MIN':
            self.timestep = timedelta(minutes=1)
            self.freq = '1min'
            self.aggregate = 1
        elif self.interval == '5MIN':
            self.timestep = timedelta(minutes=5)
            self.freq = '5min'
            self.aggregate = 5
        elif self.interval == '1HRS':
            self.timestep = timedelta(hours=1)
            self.freq = '1h'
            self.aggregate = 60

        self.filepath = 'prices_' + self.interval + '.csv'
        if os.path.isfile(self.filepath):
            self.prices = pd.read_csv(self.filepath, index_col='Time',
                                      parse_dates=True, date_parser=parser.parse)
        else:
            self.prices = pd.DataFrame(columns=['default'])
            self.prices.index.name = 'Time'

    def get_exchanges(self):
        """Return all exchanges supported by CrytoCompare."""
        url = 'https://min-api.cryptocompare.com/data/all/exchanges'
        response = re.get(url).json()
        return response.keys()

    def hist_opens(self, start_time, end_time, coin_dict):
        """Return a DataFrame containing historical open prices.
           Format for coin_dict: {exchange: [coins]}
        """
        data = pd.DataFrame()
        for exchange in coin_dict.keys():
            for coin in coin_dict[exchange]:
                column = self.assure_column(start_time, end_time, exchange, coin)
                data[column.name] = column

        self.save()
        return data

    def hist_n_opens(self, end_time, n, coin_dict):
        """Return a DataFrame containing the last n opens up to and including at end_time."""
        start_time = end_time - n*self.timestep
        return self.hist_opens(start_time, end_time, coin_dict)

    def get_bid(self, exchange, coin):
        """Return the bid price of a coin on an exchange."""
        if exchange == 'bitmex':
            instrument = coin + self.BITMEX_EXPIRATION
            bm = bitmex.bitmex(test=False)
            bid = bm.Quote.Quote_get(symbol=instrument, reverse=True,
                                     count=1).result()[0][0]['bidPrice']
            return float(bid)

        symbol_id = self.generate_symbol_id(exchange, coin)
        url = ('https://rest.coinapi.io/v1/quotes/' + symbol_id +
               '/current?apiKey=' + self.apiKey)
        print(url)

        response = re.get(url).json()
        return response['bid_price']

    def get_ask(self, exchange, coin):
        """Return the ask price of a coin on an exchange."""
        if exchange == 'bitmex':
            instrument = coin + self.BITMEX_EXPIRATION
            bm = bitmex.bitmex(test=False)
            ask = bm.Quote.Quote_get(symbol=instrument, reverse=True,
                                     count=1).result()[0][0]['askPrice']
            return float(ask)

        symbol_id = self.generate_symbol_id(exchange, coin)
        url = 'https://rest.coinapi.io/v1/quotes/' + symbol_id +\
              '/current?apiKey=' + self.apiKey

        response = re.get(url).json()
        return response['ask_price']

    def last_n_with_bids(self, n, coin_dict):
        """Return the n most recent opens, concatenated with most recent bids."""
        current_time = datetime.now(timezone.utc).replace(microsecond=0)
        p = self.hist_n_opens(current_time, n, coin_dict)
        bids = pd.Series()
        for exchange in coin_dict.keys():
            for coin in coin_dict[exchange]:
                bids.loc[utils.encode_symbol(exchange, coin)] = self.get_bid(exchange, coin)
        p.loc[current_time] = bids
        return p

    def last_n_with_asks(self, n, coin_dict):
        """Return the n most recent opens, concatenated with most recent asks."""
        current_time = datetime.now(timezone.utc).replace(microsecond=0)
        p = self.hist_n_opens(current_time, n, coin_dict)
        asks = pd.Series()
        for exchange in coin_dict.keys():
            for coin in coin_dict[exchange]:
                asks.loc[utils.encode_symbol(exchange, coin)] = self.get_ask(exchange, coin)
        p.loc[current_time] = asks
        return p

    def last_n_with_spread(self, n, exchange_a, coin_a, exchange_b, coin_b):
        """Return the n most recent opens, concatenated with ask on exchange a and bid on
           exchange b.
        """
        start_time = self.prices.index[-1] - n*self.timestep
        current_time = datetime.now(timezone.utc).replace(microsecond=0)
        self.assure_column(start_time, current_time, exchange_a, coin_a)
        self.assure_column(start_time, current_time, exchange_b, coin_b)

        column_a = utils.encode_symbol(exchange_a, coin_a)
        column_b = utils.encode_symbol(exchange_b, coin_b)
        p = self.prices.iloc[-n:][[column_a, column_b]]

        ask = self.get_ask(exchange_a, coin_a)
        bid = self.get_bid(exchange_b, coin_b)
        p.loc[current_time] = [ask, bid]

        self.save()
        return p

    ########################
    #    HELPER METHODS    #
    ########################

    def assure_column(self, start_time, end_time, exchange, coin):
        # Return a Series of opens for a coin, polling data and updating prices when necessary
        name = utils.encode_symbol(exchange, coin)
        if name not in self.prices.columns:
            column = self.fetch_opens(start_time, end_time, exchange, coin)

        else:
            column = self.prices[name].dropna()
            earliest_time = column.index[0]
            latest_time = column.index[-1]

            # check start
            if start_time < earliest_time:
                earlier_data = self.fetch_opens(start_time, earliest_time, exchange, coin)
                column = pd.concat([earlier_data, column])
                column = column[~column.index.duplicated(keep='first')]
            # check end
            if end_time > latest_time:
                later_data = self.fetch_opens(latest_time, end_time, exchange, coin)
                column = pd.concat([column, later_data])
                column = column[~column.index.duplicated(keep='last')]
            # check contiguity
            expected_times = pd.date_range(earliest_time, latest_time, freq=self.freq, tz='UTC')
            actual_times = column[earliest_time: latest_time].index
            if not expected_times.equals(actual_times):
                middle_data = self.fetch_opens(earliest_time, latest_time, exchange, coin)
                column.loc[earliest_time:latest_time] = middle_data

        self.add_column(self.prices, column)
        self.save()
        return self.prices.loc[start_time:end_time, name]

    def add_column(self, dataframe, column):
        # Add or replace a column to dataframe, filling in extra rows with NaN
        column.sort_index(inplace=True)
        for row in column.index:
            if row not in dataframe.index:
                dataframe.loc[row] = pd.Series(index=dataframe.columns)
        dataframe.sort_index(inplace=True)
        dataframe[column.name] = column

    def generate_symbol_id(self, exchange, coin):
        # Generate a symbol id from an exchange and coin name
        return '_'.join([exchange, 'SPOT', coin, 'BTC']).upper()

    def save(self):
        # Write self.prices to csv
        self.prices.to_csv(self.filepath)

    def fetch_opens(self, start_time, end_time, exchange, coin):
        # Fetch OHLCV for one coin from CryptoCompare and return a Series of opening prices
        print('Fetching opens for ' + coin + ' on ' + exchange)
        if exchange == 'bitmex':
            return self.fetch_opens_bitmex(start_time, end_time, coin)

        start_time = calendar.timegm(start_time.timetuple())
        end_time = calendar.timegm(end_time.timetuple())
        data = pd.Series(name=utils.encode_symbol(exchange, coin))

        while True:
            temp_data = pd.Series(name=utils.encode_symbol(exchange, coin))
            url = ('https://min-api.cryptocompare.com/data/histominute?fsym=' + coin +
                   '&tsym=BTC&limit=2000&aggregate=' + str(self.aggregate) +
                   '&e=' + exchange +
                   '&tsTo=' + str(end_time))
            response = re.get(url).json()
            for candle in response['Data']:
                try:
                    timestamp = datetime.fromtimestamp(candle['time'], tz=pytz.utc)
                    data.loc[timestamp] = float(candle['open'])
                except TypeError:
                    print(response)
                    raise TypeError('Issue with request response')
            data = pd.concat([temp_data, data], axis=0)
            end_time = response['Data'][0]['time']
            if end_time <= start_time:
                break

        return data

    def fetch_opens_bitmex(self, start_time, end_time, coin):
        # Fetch opens for bitmex (temp)
        page_start_time = start_time
        current_time = start_time
        if self.interval == '5MIN':
            bm_interval = '5m'
        elif self.interval == '1MIN':
            bm_interval = '1m'
        elif self.interval == '1HRS':
            bm_interval = '1h'
        else:
            raise RuntimeError('interval unavailable on bitmex')
        temp_prices = []
        temp_times = []
        requests = 0

        while True:
            # fetch data
            url = ('https://www.bitmex.com/api/v1/trade/bucketed?binSize=' + bm_interval
                   + '&partial=false&symbol=' 
                   + coin + self.BITMEX_EXPIRATION 
                   + '&count=500&startTime='
                   + datetime.strftime(page_start_time, '%Y-%m-%d%%20%H%%3A%M'))

            complete_data = re.get(url).json()
            requests += 1
            if requests > 140:
                requests -= 30
                time.sleep(60)
                print('Waiting to avoid BitMEX rate limit')

            # put data into temporary arrays
            for datum in complete_data:
                current_time = parser.parse(datum['timestamp'])
                if current_time > end_time:
                    break
                temp_prices.append(float(datum['close']))
                temp_times.append(current_time)

            # prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < timedelta(hours=1):
                break

            temp_prices.pop()
            temp_times.pop()

        data = pd.Series(temp_prices, index=temp_times, name='bitmex|' + coin)
        return data
