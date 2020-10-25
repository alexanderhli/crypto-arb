from scipy.stats import linregress
import pandas as pd

from strategy import Strategy
import utilities as utils

class PairsStrategy(Strategy):
     
    ########################
    #    PUBLIC METHODS    #
    ########################

    def next_actions(self, prices_asks, prices_bids):
        """Return a list of next actions given prices leading up to the most recent.

           Sample return format:
           [{'type':'open_hedged',
             'long_exchange': 'binance',
             'long_coin:': 'ADA',
             'long_qty': 0,
             'short_exchange': 'bitmex',
             'short_coin:': 'XRP',
             'short_qty': 0}]
        """

        actions = []
        zscores_open = self.calculate_zscores(prices_bids, prices_asks)
        zscores_close = self.calculate_zscores(prices_asks, prices_bids)
        for pair in zscores_open.index:
            long_symbol, short_symbol = utils.parse_pair(pair)
            long_exchange, long_coin = utils.parse_symbol(long_symbol)
            short_exchange, short_coin = utils.parse_symbol(short_symbol)
            position = self.state.get_position(long_exchange, long_coin, short_exchange, short_coin)

            if position == 'closed' and zscores_open[pair] > self.zso:
                action = {'type': 'open_hedged',
                          'hedge_ratio': (prices_bids[short_symbol].iloc[-1] /
                                          prices_asks[long_symbol].iloc[-1]),
                          'long_exchange': long_exchange, 'long_coin': long_coin,
                          'short_exchange': short_exchange, 'short_coin': short_coin,
                          'pair': pair
                          }
                actions.append(action)

            elif position == 'open' and zscores_open[pair] > self.zse:
                action = {'type': 'enhance_hedged',
                          'hedge_ratio': (prices_bids[short_symbol].iloc[-1] /
                                          prices_asks[long_symbol].iloc[-1]),
                          'long_exchange': long_exchange, 'long_coin': long_coin,
                          'short_exchange': short_exchange, 'short_coin': short_coin,
                          'pair': pair
                          }
                actions.append(action)

            elif position == 'open' or position == 'open+' and zscores_close[pair] < self.zsc:
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

    def calculate_zscores(self, prices_short, prices_long):
        # Return a series of zscores, each corresponding to a pair of coins
        spreads = pd.DataFrame()
        for pair in self.pairs:
            long_symbol, short_symbol = utils.parse_pair(pair)
            if long_symbol in prices_long.columns and short_symbol in prices_short.columns:
                resids = self.ols(prices_long[long_symbol], prices_short[short_symbol])
                spreads[pair] = resids
        std_devs = spreads.std(axis=0)
        zscores = spreads.iloc[-1] / std_devs
        return zscores

    def ols(self, series1, series2):
        # Return the results of linear regression between series1 (x) and series2 (y)
        ols = linregress(series1, series2)
        resids = series2 - ols.intercept - series1 * ols.slope
        return resids
