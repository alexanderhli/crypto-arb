import os
from twilio.rest import Client
import csv

def twilio_message(text):
    #auth info redacted

def log_trade(trade_type, exchange, coin0, coin1, amt0='', amt1=''):
    if not os.path.isfile('trades.csv'):
        write_header = True
    else:
        write_header = False
    with open('trades.csv', 'w') as log:
        writer = csv.writer(log)
        if write_header:
            writer.writerow(['type', 'exchange', 'l_coin', 's_coin', 'l_amt', 's_amt'])
        writer.writerow([trade_type, exchange, coin0, coin1, amt0, amt1])

