import ccxt
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

import utilities as utils


class Trader(object):

    ########################
    #    PUBLIC METHODS    #
    ########################

    def __init__(self, state, params_file='params.yaml'):
        """Initialize a Trader object given a dict of exchanges and usernames,
           and a State object.
        """
        self.state = state  # reference to passed State object
        self.exchanges = {}  # dict of exchange ids and corresponding ccxt clients

        self.params = utils.load_params(params_file)
        self.BITMEX_EXPIRATION = self.params['bitmex_exp_code']
        exchange_info = self.params['exchange_info']

        for exchange_id in exchange_info.keys():
            username = exchange_info[exchange_id]
            api = utils.load_auth(exchange_id, username)
            self.exchanges[exchange_id] = self.setup_exchange(
                exchange_id, api['key'], api['secret'])
            self.exchanges[exchange_id].load_markets()

        self.close_anomalies()

    def get_balance(self, exchange_id, coin):
        """Get balance of coin on exchange."""
        self.check_arguments(exchange_id, coin)

        try:
            balance = self.get_balance_helper(exchange_id, coin)
            return balance
        except RetryError:
            raise ServerError(exchange_id)

    def open_hedged_trade(self, l_exchange_id, l_coin, l_qty, s_exchange_id, s_coin, s_qty):
        """Open a hedged trade."""
        self.check_arguments(l_exchange_id, l_coin)
        self.check_arguments(s_exchange_id, s_coin)
        self.check_vacancies(l_exchange_id)
        self.check_vacancies(s_exchange_id)

        try:
            self.buy(l_exchange_id, l_coin, l_qty)
            self.state.open_long(l_exchange_id, l_coin, s_exchange_id, s_coin, l_qty)
        except RetryError:
            raise ServerError(l_exchange_id)

        try:
            self.sell(s_exchange_id, s_coin, s_qty)
            self.state.open_short(l_exchange_id, l_coin, s_exchange_id, s_coin, s_qty)
        except RetryError:
            raise ServerError(s_exchange_id)

    def enhance_hedged_trade(self, l_exchange_id, l_coin, l_qty, s_exchange_id, s_coin, s_qty):
        """Increase the size of a hedged trade."""
        self.check_arguments(l_exchange_id, l_coin)
        self.check_arguments(s_exchange_id, s_coin)

        try:
            self.buy(l_exchange_id, l_coin, l_qty)
            self.state.enhance_long(l_exchange_id, l_coin, s_exchange_id, s_coin, l_qty)
        except RetryError:
            raise ServerError(l_exchange_id)

        try:
            self.sell(s_exchange_id, s_coin, s_qty)
            self.state.enhance_short(l_exchange_id, l_coin, s_exchange_id, s_coin, s_qty)
        except RetryError:
            raise ServerError(s_exchange_id)

    def close_hedged_trade(self, l_exchange_id, l_coin, s_exchange_id, s_coin):
        """Close a hedged trade."""
        self.check_arguments(l_exchange_id, l_coin)
        self.check_arguments(s_exchange_id, s_coin)
        l_symbol = utils.encode_symbol(l_exchange_id, l_coin)
        s_symbol = utils.encode_symbol(s_exchange_id, s_coin)

        if self.state.check_overlap('long', l_exchange_id, l_coin):
            long_amt = self.state.longs.loc[l_symbol, s_symbol]
        else:
            long_amt = self.get_balance(l_exchange_id, l_coin)
        try:
            self.sell(l_exchange_id, l_coin, long_amt)
            self.state.close_long(l_exchange_id, l_coin, s_exchange_id, s_coin)
        except RetryError:
            raise ServerError(l_exchange_id)

        if self.state.check_overlap('short', s_exchange_id, s_coin):
            short_amt = self.state.shorts.loc[l_symbol, s_symbol]
        else:
            short_amt = self.get_balance(s_exchange_id, s_coin)
        try:
            self.buy(s_exchange_id, s_coin, short_amt)
            self.state.close_short(l_exchange_id, l_coin, s_exchange_id, s_coin)
        except RetryError:
            raise ServerError(s_exchange_id)

    def close_anomalies(self):
        """Close unhedged positions."""
        long_anomalies, short_anomalies = self.state.find_anomalies()
        for anomaly in long_anomalies:
            try:
                self.sell(anomaly['long_exchange'], anomaly['long_coin'], anomaly['quantity'])
                self.state.close_long(anomaly['long_exchange'], anomaly['long_coin'],
                                      anomaly['short_exchange'], anomaly['short_coin'])
            except RetryError:
                raise ServerError(anomaly['long_exchange'])

        for anomaly in short_anomalies:
            try:
                self.buy(anomaly['short_exchange'], anomaly['short_coin'], anomaly['quantity'])
                self.state.close_short(anomaly['long_exchange'], anomaly['long_coin'],
                                       anomaly['short_exchange'], anomaly['short_coin'])
            except RetryError:
                raise ServerError(anomaly['short_exchange'])

    ########################
    #    HELPER METHODS    #
    ########################

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=160))
    def get_balance_helper(self, exchange_id, coin):
        # helper for getting balance (so that you can retry)
        if exchange_id == 'bitmex' and coin != 'BTC':
            instrument = coin + self.BITMEX_EXPIRATION
            response = self.exchanges[exchange_id].private_get_position({'symbol': instrument})
            return response[0]['currentQty']
        return self.exchanges[exchange_id].fetch_balance()['free'][coin]

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=160))
    def buy(self, exchange_id, coin, qty):
        # purchase qty of coin on exchange
        if exchange_id == 'bitmex':
            instrument = coin + self.BITMEX_EXPIRATION
        else:
            instrument = coin + '/BTC'
        if self.exchanges[exchange_id].has['createMarketOrder']:
            self.exchanges[exchange_id].create_market_buy_order(instrument, qty)
        else:
            ask = self.exchanges[exchange_id].fetch_ticker(instrument)['ask']
            self.exchanges[exchange_id].create_limit_buy_order(instrument, qty, ask*1.1)
        utils.log_trade(('buy', exchange_id, coin, qty))

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=5, max=160))
    def sell(self, exchange_id, coin, qty):
        # sell qty of coin on exchange
        if exchange_id == 'bitmex':
            instrument = coin + self.BITMEX_EXPIRATION
        else:
            instrument = coin + '/BTC'
        if self.exchanges[exchange_id].has['createMarketOrder']:
            self.exchanges[exchange_id].create_market_sell_order(instrument, qty)
        else:
            bid = self.exchanges[exchange_id].fetch_ticker(instrument)['bid']
            self.exchanges[exchange_id].create_limit_sell_order(instrument, qty, bid*0.9)
        utils.log_trade(('sell', exchange_id, coin, qty))

    def setup_exchange(self, exchange_id, key, sec):
        # return an authenticated exchange object given exchange name and auth info
        return getattr(ccxt, exchange_id)({'apiKey': key, 'secret': sec})

    def check_arguments(self, exchange_id, coin):
        # check if exchange is instantiated and has the specified currency
        if exchange_id not in self.exchanges:
            raise NoInstanceError(exchange_id)

        if exchange_id == 'bitmex':
            instrument = coin + self.BITMEX_EXPIRATION
        else:
            instrument = coin + '/BTC'
        if coin != 'BTC' and instrument not in self.exchanges[exchange_id].symbols:
            raise CoinError(exchange_id, coin)

    def check_vacancies(self, exchange_id):
        # check if there are sufficient vacancies
        if self.state.get_vacancies(exchange_id) < 1:
            raise VacancyError('Not enough vacancies')

###############################################################################


class ServerError(Exception):
    def __init__(self, exchange_id):
        self.exchange_id = exchange_id


class NoInstanceError(Exception):
    def __init__(self, exchange_id):
        self.exchange_id = exchange_id


class CoinError(Exception):
    def __init__(self, exchange_id, coin):
        self.exchange_id = exchange_id
        self.coin = coin


class VacancyError(Exception):
    pass
