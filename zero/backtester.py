import importlib

import pandas as pd
import bt
from dateutil import parser

import pricegetter
import state
import utilities as utils


class Backtester(object):

    ########################
    #    PUBLIC METHODS    #
    ########################

    def __init__(self, params_file='params.yaml'):
        """Perform a backtest."""

        self.params = utils.load_params(params_file)
        self.FEE = self.params['fee']
        self.WINDOW = self.params['window']
        self.DEFAULT_FIEFDOMS = self.params['default_fiefdoms']

        self.coin_dict = self.params['coins']
        self.start_time = parser.parse(self.params['start_time'])
        self.end_time = parser.parse(self.params['end_time'])

        self.pg = pricegetter.PriceGetter(params_file)
        self.timestep = self.pg.timestep

        print('\nGetting prices...')
        self.prices = self.pg.hist_opens(self.start_time, self.end_time, self.coin_dict)
        self.volumes = self.pg.hist_volumes(self.start_time, self.end_time, self.coin_dict)

        print('\nLoading strategy...')
        module = importlib.import_module('strategy_' + self.params['strategy'])
        self.strateg = module.PairsStrategy(self.prices, self.volumes, params_file)

    def go(self):
        print('\nExecuting backtest...')
        if self.params['backtest_mode'] == 'together':
            self.results = self.backtest_together()
        elif self.params['backtest_mode'] == 'separate':
            self.results = self.backtest_separate()
        else:
            raise ValueError('Invalid backtesting mode') 

    def print_results(self):
        """Print backtesting results in DataFrame form."""
        print(self.results)

    def save_results(self, filename):
        """Save backtesting results to the given file."""
        self.results.to_csv(filename)

    ########################
    #    HELPER METHODS    #
    ########################

    def backtest_separate(self):
        return

    def backtest_together(self):
        return

    def calculate_positions(self):
        return