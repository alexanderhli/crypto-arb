import pricegetter as pg
import datetime

start_time = datetime.datetime(2018, 1, 26, 0, 0, 0)
end_time = datetime.datetime(2018, 3, 23, 0, 0, 0)
interval = '1hour'
symbols = ['ltc', 'eth']

print(pg.get_okex_spots_simple(symbols, interval))
