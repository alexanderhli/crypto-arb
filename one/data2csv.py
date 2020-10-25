import pricegetter as pg
import pandas as pd
import datetime

start_time = datetime.datetime(2015, 3, 30, 0, 0, 0)
end_time = datetime.datetime.utcnow()

binance_symbols = pg.get_binance_btc_market_tokens()

binance_prices = pg.get_binance(start_time, end_time, binance_symbols, '5m')

binance_prices.to_csv('binance_data', sep='\t', encoding='utf-8')