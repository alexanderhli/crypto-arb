import time
import calendar
import yaml
import os
from dateutil import parser
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests as re
import numpy as np

import utilities as utils


class PriceGetter(object):

    ########################
    #    PUBLIC METHODS    #
    ########################

    def __init__(self, params_file='params.yaml'):
        """Initialize a PriceGetter object with specified parameters file."""
        self.apiKey = utils.load_auth('coinapi', 'li')['key']
        self.params = utils.load_params(params_file)
        self.BITMEX_EXPIRATION = self.params['bitmex_exp_date']
        self.BITMEX_CODE = self.params['bitmex_exp_code']
        self.IMPUTATION_THRESHOLD = self.params['imputation_threshold']
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

        self.filepath_p = 'prices_' + self.interval + '.csv'
        if os.path.isfile(self.filepath_p):
            self.prices = pd.read_csv(self.filepath_p, index_col='Time',
                                      parse_dates=True, date_parser=parser.parse)
        else:
            self.prices = pd.DataFrame(columns=['default'])
            self.prices.index.name = 'Time'

        self.filepath_v = 'volume_' + self.interval + '.csv'
        if os.path.isfile(self.filepath_v):
            self.volumes = pd.read_csv(self.filepath_v, index_col='Time',
                                       parse_dates=True, date_parser=parser.parse)
        else:
            self.volumes = pd.DataFrame(columns=['default'])
            self.volumes.index.name = 'Time'

    def get_exchanges(self):
        """Return all exchanges supported by CoinAPI."""
        exchanges = []
        url = 'https://rest.coinapi.io/v1/exchanges?apiKey=' + self.apiKey
        response = re.get(url).json()
        for item in response:
            exchanges.append(item['exchange_id'].lower())
        return exchanges

    def hist_opens(self, start_time, end_time, coin_dict):
        """Return a DataFrame containing historical open prices.
           Format for coin_dict: {exchange: [coins]}
        """
        data = pd.DataFrame()
        for exchange in coin_dict.keys():
            for coin in coin_dict[exchange]:
                column = self.assure_column('price', start_time, end_time, exchange, coin)
                data[column.name] = column

        self.save()
        return data

    def hist_volumes(self, start_time, end_time, coin_dict):
        """Return a DataFrame containing historical volumes.
           Format for coin_dict: {exchange: [coins]}
        """
        data = pd.DataFrame()
        for exchange in coin_dict.keys():
            for coin in coin_dict[exchange]:
                column = self.assure_column('volume', start_time, end_time, exchange, coin)
                data[column.name] = column

        self.save()
        return data

    def hist_n_opens(self, end_time, n, coin_dict):
        """Return a DataFrame containing n historical opens up to and including at end_time."""
        start_time = end_time - n*self.timestep
        return self.hist_opens(start_time, end_time, coin_dict)

    def get_bid(self, exchange, coin):
        """Return the bid price of a coin on an exchange."""
        symbol_id = self.generate_symbol_id(exchange, coin)
        url = 'https://rest.coinapi.io/v1/quotes/' + symbol_id +\
              '/current?apiKey=' + self.apiKey

        response = re.get(url).json()
        return response['bid_price']

    def get_ask(self, exchange, coin):
        """Return the ask price of a coin on an exchange."""
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

    ########################
    #    HELPER METHODS    #
    ########################

    def assure_column(self, datatype, start_time, end_time, exchange, coin):
        # Return a Series of data for a coin, polling and updating prices/volumes when necessary
        # 'datatype' specifies price or volume data
        name = utils.encode_symbol(exchange, coin)
        if datatype == 'price':
            data = self.prices
        elif datatype == 'volume':
            data = self.volumes
        if name not in data.columns:
            column = self.fetch_hist(datatype, start_time, end_time, exchange, coin)

        else:
            column = data[name].dropna()
            earliest_time = column.index[0]
            latest_time = column.index[-1]

            if start_time < earliest_time:
                earlier_data = self.fetch_hist(datatype, start_time, earliest_time, exchange, coin)
                column = pd.concat([earlier_data, column])
                column = column[~column.index.duplicated(keep='first')]

            if end_time > latest_time:
                later_data = self.fetch_hist(datatype, latest_time, end_time, exchange, coin)
                column = pd.concat([column, later_data])
                column = column[~column.index.duplicated(keep='last')]

            expected_times = pd.date_range(start_time, end_time, freq=self.freq, tz='UTC')
            actual_times = column[start_time: end_time].index
            if not len(expected_times) == len(actual_times):
                missing_times = expected_times[~expected_times.isin(actual_times)]
                if not self.impute(column, missing_times):
                    # Try fetching if too much missing data to impute
                    middle_data = self.fetch_hist(
                        datatype, earliest_time, latest_time, exchange, coin)
                    column.loc[earliest_time:latest_time] = middle_data

        self.add_column(data, column)
        self.save()
        return data.loc[start_time:end_time, name]

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
        if exchange == 'bitmex':
            return '_'.join([exchange, 'FTS', coin, 'BTC', self.BITMEX_EXPIRATION]).upper()
        return '_'.join([exchange, 'SPOT', coin, 'BTC']).upper()

    def save(self):
        # Write self.prices to csv
        self.prices.to_csv(self.filepath_p)
        self.volumes.to_csv(self.filepath_v)

    def fetch_hist(self, datatype, start_time, end_time, exchange, coin):
        # Fetch OHLCV for one coin from CoinAPI and return a Series of opening prices
        # Type specifies what sort of data (open or volume)
        print('Fetching opens for', exchange, coin)
        if exchange == 'bitmex':
            return self.fetch_hist_bitmex(datatype, start_time, end_time, coin)

        end_time = end_time + self.timestep  # get one more since coinapi is upper bound exclusive
        symbol_id = self.generate_symbol_id(exchange, coin)
        url = ('https://rest.coinapi.io/v1/ohlcv/' + symbol_id +
               '/history?apiKey=' + self.apiKey +
               '&period_id=' + self.interval +
               '&time_start=' + start_time.isoformat().replace('+00:00', 'Z') +
               '&time_end=' + end_time.isoformat().replace('+00:00', 'Z') +
               '&limit=100000')
        response = re.get(url).json()

        data = pd.Series(name=utils.encode_symbol(exchange, coin))
        for candle in response:
            try:
                timestamp = parser.parse(candle['time_period_start'])
                if datatype == 'price':
                    data.loc[timestamp] = float(candle['price_open'])
                elif datatype == 'volume':
                    data.loc[timestamp] = float(candle['volume_traded'])
            except TypeError:
                print(response)
                raise TypeError('Issue with request response')

        return data

    def fetch_hist_bitmex(self, datatype, start_time, end_time, coin):
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
                   + coin + self.BITMEX_CODE
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
                if datatype == 'price':
                    temp_prices.append(float(datum['close']))
                    temp_times.append(current_time)
                elif datatype == 'volume':
                    temp_prices.append(float(datum['volume']))
                    temp_times.append(current_time)

            # prepare for next iteration
            page_start_time = current_time

            if end_time - page_start_time < timedelta(hours=1):
                break

            temp_prices.pop()
            temp_times.pop()

        data = pd.Series(temp_prices, index=temp_times, name='bitmex|' + coin)
        return data

    def impute(self, column, missing_times):
        # Fill in missing data at the given times if its impact is less than the threshold
        # Uses random values, bounded by prices just before start and just after end
        # Return True if imputation was performed, False if too many blanks to impute
        if len(missing_times) / len(column) > self.IMPUTATION_THRESHOLD:
            print('Too much missing data to impute:', column.name)
            print('Impact ratio:', len(missing_times) / len(column))
            return False
        print('Imputing', column.name)
        while len(missing_times) > 0:
            start_time = missing_times[0] - self.timestep
            end_time = missing_times[0]
            while end_time in missing_times:
                end_time += self.timestep
            lbound = column[start_time]
            ubound = column[end_time]

            chunk = (t for t in missing_times if (t > start_time and t < end_time))
            for time in chunk:
                column[time] = np.random.uniform(lbound, ubound)
            missing_times = missing_times[missing_times > end_time]
        column.sort_index(inplace=True)
        return True
