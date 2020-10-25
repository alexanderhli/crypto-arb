import pandas as pd
import utilities as utils

class State(object):
    def __init__(self, _positions, _longs, _shorts, _vacancies):
        self.positions = _positions #boolean (True = open, False = closed)
        self.longs = _longs         #dataframe (column major order)
        self.shorts = _shorts       #dataframe
        self.vacancies = _vacancies #integer

    def __init__(self, exchanges, pairs, num_fiefdoms):
        self.longcoins = []
        self.shortcoins = []
        for pair in pairs:
            self.longcoins.append(pair[0])
            self.shortcoins.append(pair[1])

        self.positions = pd.DataFrame(False, columns=exchanges, index=self.longcoins)
        self.longs = pd.DataFrame(0.0, columns=exchanges, index=self.longcoins)
        self.shorts = pd.DataFrame(0.0, columns=exchanges, index=self.shortcoins)
        self.vacancies = num_fiefdoms

    #getter methods 
    def get_position(self, exchange, coin):
        return self.positions[exchange][coin]

    def get_long_quantity(self, exchange, coin):
        return self.longs[exchange][coin]

    def get_short_quantity(self, exchange, coin):
        return self.shorts[exchange][coin]    

    def get_vacancies(self):
        return self.vacancies

    #change position methods
    def open_position(self, exchange, coin0, amt0, coin1, amt1):
        self.positions[exchange][coin0] = True
        self.longs[exchange][coin0] = amt0
        self.shorts[exchange][coin1] = amt1
        self.vacancies -= 1
        utils.log_trade('buy', exchange, coin0, coin1, amt0, amt1)

    def close_position(self, exchange, coin0, coin1):
        self.positions[exchange][coin0] = False
        self.longs[exchange][coin0] = 0.0
        self.shorts[exchange][coin1] = 0.0
        self.vacancies += 1
        utils.log_trade('sell', exchange, coin0, coin1, 0, 0)
    
    def __str__(self):
        str = self.positions.to_string() + '\n'
        str += self.longs.to_string() + '\n'
        str += self.shorts.to_string() + '\n'
        str += 'vacancies: ' + self.vacancies
        return str

