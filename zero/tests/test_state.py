import state
import os.path

if os.path.isfile('savedstate.pkl'):
    os.remove('savedstate.pkl')
s = state.State('params_testing.yaml')

s.open_long('binance', 'btc', 'bitmex', 'XBTUSD', 100)
s.open_short('binance', 'btc', 'bitmex', 'XBTUSD', 98)
s.open_long('bittrex', 'eth', 'bitmex', 'ETHM18', 3)
s.open_short('okex', 'eth', 'bitfinex', 'ETHM18', 2)

s.enhance_long('binance', 'btc', 'bitmex', 'XBTUSD', 2)
s.enhance_short('binance', 'btc', 'bitmex', 'XBTUSD', 3)

if s.get_position('binance', 'btc', 'bitmex', 'XBTUSD') != 'open+':
    raise Exception
if s.get_position('bittrex', 'eth', 'bitmex', 'ETHM18') != 'longonly':
    raise Exception
if s.get_position('okex', 'eth', 'bitfinex', 'ETHM18') != 'shortonly':
    raise Exception
if s.get_position('yobit', 'eth', 'bitfinex', 'ETHM18') != 'closed':
    raise Exception

long_anomalies, short_anomalies = s.find_anomalies()
print(long_anomalies)
print(short_anomalies)

s = state.State(10000)
if s.get_vacancies('binance') != 0:
    raise Exception
if s.get_vacancies('bittrex') != 1:
    raise Exception
if s.get_vacancies('bitmex') != 1:
    raise Exception
if s.get_vacancies('okex') != 2:
    raise Exception
if s.get_vacancies('bitfinex') != 5:
    raise Exception
if s.get_vacancies('gdax') != 2:
    raise Exception

s.close_short('binance', 'btc', 'bitmex', 'XBTUSD')
s.close_long('bittrex', 'eth', 'bitmex', 'ETHM18')
s.close_short('okex', 'eth', 'bitfinex', 'ETHM18')
if s.get_position('binance', 'btc', 'bitmex', 'XBTUSD') != 'longonly-':
    raise Exception

s.close_long('binance', 'btc', 'bitmex', 'XBTUSD')

if s.get_position('binance', 'btc', 'bitmex', 'XBTUSD') != 'closed':
    raise Exception
if s.get_position('bittrex', 'eth', 'bitmex', 'ETHM18') != 'closed':
    raise Exception
if s.get_position('okex', 'eth', 'bitfinex', 'ETHM18') != 'closed':
    raise Exception

if s.get_vacancies('binance') != 2:
    raise Exception
if s.get_vacancies('bittrex') != 2:
    raise Exception
if s.get_vacancies('bitmex') != 3:
    raise Exception
if s.get_vacancies('okex') != 2:
    raise Exception
if s.get_vacancies('bitfinex') != 6:
    raise Exception
if s.get_vacancies('gdax') != 2:
    raise Exception

s.open_long('bittrex', 'eth', 'bitmex', 'XBTUSD', 10)
s.open_short('bittrex', 'eth', 'bitmex', 'XBTUSD', 10)
s.open_long('bittrex', 'eth', 'bitmex', 'ETHM18', 10)
s.open_short('okex', 'eth', 'bitmex', 'XBTUSD', 5)

if s.check_overlap('long', 'okex', 'eth') != False:
    raise Exception
if s.check_overlap('short', 'bitmex', 'XBTUSD') != True:
    raise Exception
if s.check_overlap('long', 'bittrex', 'eth') != True:
    raise Exception

print('\n------------------------------------\n')
print(s)
