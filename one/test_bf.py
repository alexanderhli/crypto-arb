import pricegetter as pg

symbols = ['ETHBTC', 'LTCBTC']
interval = '1h'

print(pg.get_bitfinex(symbols, interval))
