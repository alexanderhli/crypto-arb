from scipy.stats import linregress
import pandas as pd

from strategy import Strategy
import utilities as utils

class PairsStrategy(Strategy):
 
    ########################
    #    PUBLIC METHODS    #
    ########################



 

    ########################
    #    HELPER METHODS    #
    ########################

    # TODO: handle updating
    def set_zscores(self):
        """Sets up or updates instance z-scores and moving average DataFrames based on instance price
           DataFrames. Calls set_mas.
        """
        open_ratios = pd.DataFrame()
        close_ratios = pd.DataFrame()
        for pair in self.pairs:
            long_symbol, short_symbol = utils.parse_pair(pair)
            if short_symbol in self.bid_prices.columns and long_symbol in self.ask_prices.columns:
                open_ratios[pair] = self.bid_prices[short_symbol] / self.ask_prices[long_symbol]
            else:
                raise ValueError('Long or short symbol not in price DataFrame')
            if long_symbol in self.bid_prices.columns and short_symbol in self.ask_prices.columns:
                close_ratios[pair] = self.bid_prices[short_symbol] / self.ask_prices[long_symbol]
            else:
                raise ValueError('Long or short symbol not in price DataFrame')

        self.set_ma('open_ratios', open_ratios)
        self.set_ma('close_ratios', close_ratios)

        open_averages = self.moving_averages['open_ratios']
        open_stds = open_ratios.rolling(window=self.WINDOW)
        self.open_zscores = ((open_ratios - open_averages) / open_stds).dropna()

        close_averages = self.moving_averages['close_ratios']
        close_stds = close_ratios.rolling(window=self.WINDOW)
        self.close_zscores = ((close_ratios - close_averages) / close_stds).dropna()


