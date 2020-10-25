from trader import Trader
from state import State
import os.path
import time

if os.path.isfile('savedstate.pkl'):
    os.remove('savedstate.pkl')


# test anomaly removal
exchange_info = {'bitmex': 'bennett', 'binance': 'bennett'}
state = State('params_testing.yaml')
trader = Trader(state, 'params_testing.yaml')

trader.buy('binance', 'XRP', 4)
state.open_long('binance', 'XRP', 'bitmex', 'XRP', 4)

del trader
trader = Trader(state, 'params_testing.yaml')
if trader.get_balance('binance', 'XRP') > 1:
    raise Exception
if state.get_position('binance', 'XRP', 'bitmex', 'XRP') != 'closed':
    raise Exception

# statement testing
exchange_info = {'bitmex': 'bennett', 'binance': 'bennett'}
state = State('params_testing.yaml')
trader = Trader(state, 'params_testing.yaml')

print(trader.get_balance('bitmex', 'BTC'))
print(trader.get_balance('binance', 'BTC'))

trader.open_hedged_trade('binance', 'XRP', 10, 'bitmex', 'XRP', 9)
time.sleep(30)
if trader.get_balance('binance', 'XRP') < 9:
    raise Exception
if trader.get_balance('bitmex', 'XRP') != -9:
    raise Exception
if state.get_position('binance', 'XRP', 'bitmex', 'XRP') != 'open':
    raise Exception

trader.enhance_hedged_trade('binance', 'XRP', 2, 'bitmex', 'XRP', 2)
time.sleep(30)
if trader.get_balance('binance', 'XRP') < 11:
    raise Exception
if trader.get_balance('bitmex', 'XRP') != -11:
    raise Exception
if state.get_position('binance', 'XRP', 'bitmex', 'XRP') != 'open':
    raise Exception


trader.close_hedged_trade('binance', 'XRP', 'bitmex', 'XRP')
time.sleep(30)
if trader.get_balance('binance', 'XRP') > 1:
    raise Exception
if trader.get_balance('bitmex', 'XRP') != 0:
    raise Exception
if state.get_position('binance', 'XRP', 'bitmex', 'XRP') != 'closed':
    raise Exception

# test overlap handling
trader.open_hedged_trade('binance', 'XRP', 2, 'bitmex', 'XRP', 2)
trader.open_hedged_trade('binance', 'ADA', 1, 'bitmex', 'XRP', 2)
time.sleep(30)
trader.close_hedged_trade('binance', 'XRP', 'bitmex', 'XRP')
trader.close_hedged_trade('binance', 'ADA', 'bitmex', 'XRP')
time.sleep(30)
if trader.get_balance('binance', 'XRP') > 1:
    raise Exception
if trader.get_balance('binance', 'ADA') > 1:
    raise Exception
if trader.get_balance('bitmex', 'XRP') != 0:
    raise Exception
if state.get_position('binance', 'XRP', 'bitmex', 'XRP') != 'closed':
    raise Exception
if state.get_position('binance', 'ADA', 'bitmex', 'XRP') != 'closed':
    raise Exception


# exception testing
exchange_info = {'bitmex': 'bennett', 'binance': 'bennett'}
state = State('params_testing.yaml')
trader = Trader(state, 'params_testing.yaml')

trader.open_hedged_trade('bitmex', 'XRP', 5, 'binance', 'SHITCOIN', 5)

# exception testing
exchange_info = {'bitmex': 'bennett', 'binance': 'bennett'}
state = State('params_testing.yaml')
trader = Trader(state, 'params_testing.yaml')

trader.open_hedged_trade('bitmex', 'XRP', 5, 'asdf', 'fdsa', 5)
