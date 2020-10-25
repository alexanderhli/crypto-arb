import ccxt
import yaml
import csv
from twilio.rest import Client

########################
#   LOADING METHODS    #
########################


def load_params(filename):
    """Return parameters in dictionary form."""
    with open(filename, 'r') as f:
        params = yaml.safe_load(f)
    return params


def load_auth(site, user):
    """Return API info for the given user and site."""
    with open('auth.yaml', 'r') as f:
        auth = yaml.safe_load(f)
    return auth[site][user]


########################
#   PARSING METHODS    #
########################


def parse_pair(pair_string):
    """Convert a string representation of a pair to a list."""
    long_symbol = pair_string.split('//')[0]
    short_symbol = pair_string.split('//')[1]
    return long_symbol, short_symbol


def parse_symbol(symbol_string):
    """Convert a string representation of a symbol to a list."""
    exchange = symbol_string.split('|')[0]
    coin = symbol_string.split('|')[1]
    return exchange, coin


def encode_pair(symbol1, symbol2):
    """Create a string representation of a pair."""
    return symbol1 + '//' + symbol2


def encode_symbol(exchange, coin):
    """Create a string representation of a symbol."""
    return exchange + '|' + coin


########################
#   TEXTING METHODS    #
########################


def broadcast_trade(type_, l_exchange, l_coin, s_exchange, s_coin):
    """Send a text and print information regarding trade made."""
    text = ('Trade: ' + type_ +
            '\nLong: ' + l_coin + ' on ' + l_exchange +
            '\nShort: ' + s_coin + ' on ' + s_exchange)
    print(text)
    twilio_message(text)


def twilio_message(text):
    """Send a text through the Twilio API."""
    auth = load_auth('twilio', 'li')
    params = load_params('params.yaml')

    key = auth['key']
    secret = auth['secret']

    from_number = params['sms']['from']
    client = Client(key, secret)
    for recipient in params['sms']['to'].keys():
        to_number = params['sms']['to'][recipient]
        client.messages.create(from_=from_number, to=to_number, body=text)

########################
#    OTHER METHODS     #
########################

def generate_coin_dict(params):
    """Generate a dictionary of all coins when shortable coins is set in params."""
    coin_dict = {}
    params = load_params(params)
    shortable_coins = []
    exchanges_supported = params['exchanges_supported']
    for e in params['shortable_coins']:
        shortable_coins += params['shortable_coins'][e]

    for exchange_id in exchanges_supported:
        try:
            exchange = getattr(ccxt, exchange_id)()
            exchange.load_markets()
        except:
            continue
        for symbol in exchange.symbols:
            if '/' not in symbol:
                continue
            coin = symbol.split('/')[0]
            quote = symbol.split('/')[1]
            if coin in shortable_coins and quote == 'BTC':
                if exchange_id in coin_dict:
                    coin_dict[exchange_id].append(coin)
                else:
                    coin_dict[exchange_id] = [coin]
        print('----------------------')
        print(coin_dict)

    return coin_dict

def log_trade(data):
    """Write trade info to csv, data format = ('buy/sell', exchange, coin, qty)"""
    with open('trade_log.csv', 'a') as fp:
        writer = csv.writer(fp, delimiter=', ')
        writer.writerow(data)
