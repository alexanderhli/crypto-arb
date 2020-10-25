import os
import importlib
import time

import pricegetter
import utilities as utils
import state
import trader

# TODO: handle cases where order size is 0
# TODO: better error handling for enhance and close 
# TODO: prioritize more profitable trades
# TODO: juggle multiple strategies


class Realtime(object):

    ########################
    #    PUBLIC METHODS    #
    ########################

    def __init__(self, params_file='params.yaml'):
        """Initialize a realtime object using the given parameters."""
        if os.path.isfile('savedstate.pkl'):
            os.remove('savedstate.pkl')

        self.params = utils.load_params(params_file)

        self.coin_dict = self.params['coins']
        self.WINDOW = self.params['window']
        self.BLACKLIST_PERIODS = self.params['blacklist_periods']
        if self.params['interval'] == '5MIN':
            self.INTERVAL_SECONDS = 300
        elif self.params['interval'] == '1MIN':
            self.INTERVAL_SECONDS = 60
        elif self.params['interval'] == '1HRS':
            self.INTERVAL_SECONDS = 3600

        self.state = state.State(self.params['fiefdoms'])
        self.trader = trader.Trader(self.state, params_file)
        self.pg = pricegetter.PriceGetter(params_file)

        strategy = importlib.import_module('strategy' + self.params['strategy'])
        self.strategy = strategy.Strategy(self.state, params_file)

        self.blacklist = {}

    def run_pairs_trade(self):
        """Run realtime using a pairs-based trading strategy."""
        while True:
            prices_bids = self.pg.last_n_with_bids(self.WINDOW, self.coin_dict)
            prices_asks = self.pg.last_n_with_asks(self.WINDOW, self.coin_dict)
            actions = self.strategy.next_actions(prices_bids, prices_asks)
            self.make_trades(actions, prices_bids, prices_asks)

            time.sleep(self.INTERVAL_SECONDS)

    ########################
    #    HELPER METHODS    #
    ########################

    def check_blacklist(self, exchanges):
        # Update the blacklist and check whether any of the given exchanges are blacklisted
        # If so, raise BadExchangeError
        for exchange in self.blacklist:
            if self.blacklist[exchange] > 0:
                self.blacklist[exchange] -= 1
        for exchange in exchanges:
            if exchange in self.blacklist and self.blacklist[exchange] > 0:
                raise BadExchangeError()

    def get_balances(self, action):
        # Return balances given in action.
        # If an exchange doesn't work, add it to the blacklist and raise BadExchangeError
        try:
            long_balance = self.trader.get_balance(action['long_exchange'], 'BTC')
        except trader.ServerError:
            self.blacklist[action['long_exchange']] = self.BLACKLIST_PERIODS
            raise BadExchangeError()
        try:
            short_balance = self.trader.get_balance(action['short_exchange'], 'BTC')
        except trader.ServerError:
            self.blacklist[action['short_exchange']] = self.BLACKLIST_PERIODS
            raise BadExchangeError()

        return long_balance, short_balance

    def make_trades(self, actions, prices_bids, prices_asks):
        # Make all trades that are appropriate.
        for action in actions:
            long_exchange = action['long_exchange']
            short_exchange = action['short_exchange']
            long_coin = action['long_coin']
            short_coin = action['short_coin']
            long_vacancies = self.state.get_vacancies(long_exchange)
            short_vacancies = self.state.get_vacancies(short_exchange)
            try:
                self.check_blacklist([long_exchange, short_exchange])
                long_balance, short_balance = self.get_balances(action)
            except BadExchangeError:
                continue

            if action['type'] == 'open_hedged' and long_vacancies > 0 and short_vacancies > 0:
                btc = 0.95 * min(long_balance / long_vacancies, short_balance / short_vacancies)
                long_qty = btc / (prices_asks[long_exchange].iloc[-1])
                short_qty = btc / (prices_bids[short_exchange].iloc[-1])
                try:
                    self.trader.open_hedged_trade(long_exchange, long_coin, long_qty,
                                                  short_exchange, short_coin, short_qty)
                    utils.broadcast_trade('Open', long_exchange, long_coin,
                                          short_exchange, short_coin)
                except trader.ServerError:
                    self.trader.close_anomalies()

            elif action['type'] == 'enhance_hedged' and long_vacancies > 0 and short_vacancies > 0:
                btc = 0.95 * min(long_balance / long_vacancies, short_balance / short_vacancies)
                long_qty = btc / (prices_asks[long_exchange].iloc[-1])
                short_qty = btc / (prices_bids[short_exchange].iloc[-1])
                try:
                    self.trader.enhance_hedged_trade(long_exchange, long_coin, long_qty,
                                                     short_exchange, short_coin, short_qty)
                    utils.broadcast_trade('Enhance', long_exchange, long_coin,
                                          short_exchange, short_coin)
                except trader.ServerError:
                    utils.twilio_message('URGENT: Enhance error: ' +
                                         long_exchange + ', ' + short_exchange)

            elif action['type'] == 'close_hedged':
                try:
                    self.trader.close_hedged_trade(long_exchange, long_coin,
                                                   short_exchange, short_coin)
                    utils.broadcast_trade('Close', long_exchange, long_coin,
                                          short_exchange, short_coin)
                except trader.ServerError:
                    utils.twilio_message('URGENT: Close error: ' +
                                         long_exchange + ', ' + short_exchange)


#####################################################
class BadExchangeError(Exception):
    pass
