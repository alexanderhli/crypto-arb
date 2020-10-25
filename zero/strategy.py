import pandas as pd

import utilities as utils

class Strategy(object):
   
    ########################
    #    PUBLIC METHODS    #
    ########################
    
    def __init__(self, bid_prices, ask_prices, volumes, params_file='params.yaml'):
        """Initialize a Strategy object with z-scores from state and params file.
        
        Args:
            bid_prices: DataFrame of bid prices by symbol
            ask_prices: DataFrame of ask prices by symbol
            volumes: DataFrame of volumes by symbol
            params_file: Filepath to use for parameters

        Raise:
            ValueError if bid_prices, ask_prices, and volumes columns do not match

        """
        if bid_prices.columns != ask_prices.columns:
            raise ValueError('Bid price and ask price columns mismatch')
        if volumes.columns != bid_prices.columns:
            raise ValueError('Price and volumes columns mismatch')

        self.bid_prices = bid_prices
        self.ask_prices = ask_prices
        self.volumes = volumes

        self.params = utils.load_params(params_file)
        self.WINDOW = self.params['window']
        self.ma_type = self.params['moving_average']

        self.zso = self.params['zso']
        self.zsc = self.params['zsc']
        self.zse = self.params['zse']

        self.moving_averages = {}
        self.moving_average_consts = {}
        self.open_zscores = pd.DataFrame()
        self.close_zscores = pd.DataFrame()
        self.set_zscores()
        self.pairs = self.generate_pairs()


    def update(self, new_bid_prices, new_ask_prices, new_volumes):
        """Update instance moving average and z-score DataFrames.
        
        Args:
            new_bid_prices: New bid prices to append to existing
            new_ask_prices: New ask prices to append to existing
            new_volumes: New volumes to append to existing


        Raise:
            ValueError if bid_prices and ask_prices columns mismatch

        """
        if new_bid_prices.columns != new_ask_prices.columns:
            raise ValueError('Column mismatch between new bid and ask DataFrames')
        if new_bid_prices.columns != self.bid_prices.columns:
            raise ValueError('Column mismatch between old and new price DataFrames')

        self.bid_prices = pd.concat([self.bid_prices, new_bid_prices])
        self.bid_prices = self.bid_prices[~self.bid_prices.duplicated(keep='first')]
        self.ask_prices = pd.concat([self.ask_prices, new_ask_prices])
        self.ask_prices = self.ask_prices[~self.ask_prices.duplicated(keep='first')]

        self.set_zscores()


    def next_actions(self, time, current_positions):
        """Calculates next actions to take at a given time with certain existing positions.

        Args:
            time: Datetime object (UTC timezone) for time to get actions
            current_positions: Series of current positions by pair

        Return:
            List of dicts, each specifying an action type, hedge ratio, exchanges, and pairs.
        """

        actions = []
        zscores_open = self.open_zscores.loc[time]
        zscores_close = self.close_zscores.loc[time]
        for pair in zscores_open.index:
            long_symbol, short_symbol = utils.parse_pair(pair)
            long_exchange, long_coin = utils.parse_symbol(long_symbol)
            short_exchange, short_coin = utils.parse_symbol(short_symbol)

            if current_positions[pair] == 'closed' and zscores_open[pair] > self.zso:
                action = {'type': 'open_hedged',
                          'hedge_ratio': (self.bid_prices[short_symbol].iloc[-1] /
                                          self.ask_prices[long_symbol].iloc[-1]),
                          'long_exchange': long_exchange, 'long_coin': long_coin,
                          'short_exchange': short_exchange, 'short_coin': short_coin,
                          'pair': pair
                          }
                actions.append(action)

            elif current_positions[pair] == 'open' and zscores_open[pair] > self.zse:
                action = {'type': 'enhance_hedged',
                          'hedge_ratio': (self.bid_prices[short_symbol].iloc[-1] /
                                          self.ask_prices[long_symbol].iloc[-1]),
                          'long_exchange': long_exchange, 'long_coin': long_coin,
                          'short_exchange': short_exchange, 'short_coin': short_coin,
                          'pair': pair
                          }
                actions.append(action)

            elif ((current_positions[pair] == 'open' or current_positions[pair] == 'open+') and
                       zscores_close[pair] < self.zsc):
                action = {'type': 'close_hedged',
                          'long_exchange': long_exchange, 'long_coin': long_coin,
                          'short_exchange': short_exchange, 'short_coin': short_coin,
                          'pair': pair
                          }
                actions.append(action)

        return actions
        

    ########################
    #    HELPER METHODS    #
    ########################

    def set_zscores(self):
        """Sets up or updates instance z-scores and moving average DataFrames based on instance price
           DataFrames. Calls set_mas.
        """
        pass


    # TODO: add more moving averages with error handling
    def set_ma(self, name, data, **kwargs):
        """Sets or updates instance moving average DataFrame and relevant constants.
        
        Args:
            name: identifier for type of data (e.g. prices, ratios, spreads)
            data: DataFrame on which to calculate moving average
            kwargs: Additional arguments pertaining to moving average, e.g.:
                -volumes for VWMA

        Raises:
            ValueError if moving average type not supported, or kwargs invalid
        """
        if self.ma_type not in ['simple']:
            raise ValueError('Moving average type not supported')
        if self.ma_type == 'simple':
            if name in self.moving_averages:
                start_time = self.moving_averages[name].index[-1]
                new_ma = data.loc[start_time:].rolling(window=self.WINDOW,
                                                       min_periods=self.WINDOW).mean()
                ma = pd.concat([self.moving_averages[name], new_ma])
                ma = ma[~ma.duplicated(keep='first')]
                self.moving_averages[name] = ma
            else:
                return data.rolling(window=self.WINDOW, min_periods=self.WINDOW).mean()

    
    def generate_pairs(self):
        """Generate list of pairs from a dictionary of coins.

        Returns:
            pairs: list of string each representing a pair of symbols
        """

        self.coin_dict = self.params['coins']
        self.short_dict = self.params['shortable_coins']

        pairs = []
        for long_ex, long_coins in self.coin_dict.items():
            for short_ex, short_coins in self.short_dict.items():
                for long_coin in long_coins:
                    for short_coin in short_coins:
                        if long_ex != short_ex and long_coin == short_coin:
                            long_symbol = utils.encode_symbol(long_ex, long_coin)
                            short_symbol = utils.encode_symbol(short_ex, short_coin)
                            pair_id = utils.encode_pair(long_symbol, short_symbol)
                            pairs.append(pair_id)
        return pairs