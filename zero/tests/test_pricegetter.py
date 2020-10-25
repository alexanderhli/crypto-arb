from pricegetter import PriceGetter
from dateutil import parser


start_time = parser.parse('05-19-2018 00:00:00Z')
end_time = parser.parse('05-20-2018 16:00:00Z')
coin_dict = {'bitmex': ['ETH', 'LTC'], 'binance': ['ETH', 'LTC']}

# Test public methods
pg = PriceGetter('params_testing.yaml')

print('Testing get opens')
print(pg.hist_opens(start_time, end_time, coin_dict))

print('\nTesting get bid and ask')
print('bitmex bid: ', pg.get_bid('bitmex', 'ETH'))
print('bitmex ask: ', pg.get_ask('bitmex', 'ETH'))
print('binance bid: ', pg.get_bid('binance', 'ETH'))
print('binance ask: ', pg.get_ask('binance', 'ETH'))

print('\nTesting get n historical opens')
print('Last 20 historical at end time:\n', pg.hist_n_opens(end_time, 20, coin_dict))

print('\nTesting get last 20')
print('Last 20 with bids:\n', pg.last_n_with_bids(20, coin_dict))
print('Last 20 with asks:\n', pg.last_n_with_asks(20, coin_dict))

# Test saving and loading
del pg
pg = PriceGetter('params_testing.yaml')
print(pg.prices)
