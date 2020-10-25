import pickle
import os.path

import pandas as pd

import utilities as utils

class State(object):

    ########################
    #    PUBLIC METHODS    #
    ########################

    def __init__(self, params_file='params.yaml', inf_fiefdoms=False):
        """Initialize a new State object."""
        if os.path.isfile('savedstate.pkl'):
            with open('savedstate.pkl', 'rb') as f:
                saved_dict = pickle.load(f)
            self.__dict__.update(saved_dict)
        else:
            self.positions = pd.DataFrame('closed', index=['default'], columns=['default'])
            self.longs = pd.DataFrame(0.0, index=['default'], columns=['default'])
            self.shorts = pd.DataFrame(0.0, index=['default'], columns=['default'])
            self.params = utils.load_params(params_file)
            if inf_fiefdoms:
                self.DEFAULT_FIEFDOMS = 10000000
                self.vacancies = {}
            else:
                self.DEFAULT_FIEFDOMS = self.params['default_fiefdoms']
                self.vacancies = self.params['fiefdoms']

    def open_long(self, long_ex, long_coin, short_ex, short_coin, qty):
        """Update the state to open a long for given coin pair."""
        if self.get_vacancies(long_ex) <= 0:
            raise RuntimeError('Insufficient vacancies')
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        position = self.assure_value(self.positions, row, column, 'closed')

        if position == 'closed':
            self.positions.loc[row][column] = 'longonly'
            self.assure_value(self.longs, row, column, 0.0)
            self.longs.loc[row][column] = qty
            self.vacancies[long_ex] -= 1
            self.save()
        elif position == 'shortonly':
            self.positions.loc[row][column] = 'open'
            self.assure_value(self.longs, row, column, 0.0)
            self.longs.loc[row][column] = qty
            self.vacancies[long_ex] -= 1
            self.save()
        else:
            raise RuntimeError('Must open long from closed or shortonly')

    def open_short(self, long_ex, long_coin, short_ex, short_coin, qty):
        """Update the state to open a short for given coin pair."""
        if self.get_vacancies(short_ex) <= 0:
            raise RuntimeError('Insufficient vacancies')
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        position = self.assure_value(self.positions, row, column, 'closed')

        if position == 'closed':
            print('Warning: short opened before long')
            self.positions.loc[row][column] = 'shortonly'
            self.assure_value(self.shorts, row, column, 0.0)
            self.shorts.loc[row][column] = qty
            self.vacancies[short_ex] -= 1
            self.save()
        elif position == 'longonly':
            self.positions.loc[row][column] = 'open'
            self.assure_value(self.shorts, row, column, 0.0)
            self.shorts.loc[row][column] = qty
            self.vacancies[short_ex] -= 1
            self.save()
        else:
            raise RuntimeError('Must open short from closed or longonly')

    def enhance_long(self, long_ex, long_coin, short_ex, short_coin, qty):
        """Increase long by qty for given coin pair."""
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        position = self.assure_value(self.positions, row, column, 'closed')

        if position == 'open':
            self.positions.loc[row][column] = 'longonly+'
            self.longs.loc[row][column] += qty
            self.vacancies[long_ex] -= 1
            self.save()
        elif position == 'shortonly+':
            self.positions.loc[row][column] = 'open+'
            self.longs.loc[row][column] += qty
            self.vacancies[long_ex] -= 1
            self.save()
        else:
            raise RuntimeError('Can only enhance long from open or short-enhanced position')

    def enhance_short(self, long_ex, long_coin, short_ex, short_coin, qty):
        """Increase short by qty for given coin pair."""
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        position = self.assure_value(self.positions, row, column, 'closed')

        if position == 'open':
            print('Warning: short enhanced before long')
            self.positions.loc[row][column] = 'shortonly+'
            self.shorts.loc[row][column] += qty
            self.vacancies[short_ex] -= 1
            self.save()
        if position == 'longonly+':
            self.positions.loc[row][column] = 'open+'
            self.shorts.loc[row][column] += qty
            self.vacancies[short_ex] -= 1
            self.save()
        else:
            raise RuntimeError('Can only enhance short from open or long-enhanced position')

    def close_long(self, long_ex, long_coin, short_ex, short_coin):
        """Update the state to close the long for given coin pair."""
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        position = self.assure_value(self.positions, row, column, 'closed')

        if position == 'open':
            self.positions.loc[row][column] = 'shortonly'
            self.longs.loc[row][column] = 0.0
            self.vacancies[long_ex] += 1
            self.save()
        elif position == 'open+':
            self.positions.loc[row][column] = 'shortonly-'
            self.longs.loc[row][column] = 0.0
            self.vacancies[long_ex] += 2
            self.save()
        elif position == 'longonly':
            self.positions.loc[row][column] = 'closed'
            self.longs.loc[row][column] = 0.0
            self.vacancies[long_ex] += 1
            self.save()
        elif position == 'longonly-':
            self.positions.loc[row][column] = 'closed'
            self.longs.loc[row][column] = 0.0
            self.vacancies[long_ex] += 2
            self.save()
        else:
            raise RuntimeError('Cannot close nonexistent long')

    def close_short(self, long_ex, long_coin, short_ex, short_coin):
        """Update the state to close the short for given coin pair."""
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        position = self.assure_value(self.positions, row, column, 'closed')

        if position == 'open':
            self.positions.loc[row][column] = 'longonly'
            self.shorts.loc[row][column] = 0.0
            self.vacancies[short_ex] += 1
            self.save()
        elif position == 'open+':
            self.positions.loc[row][column] = 'longonly-'
            self.longs.loc[row][column] = 0.0
            self.vacancies[short_ex] += 2
            self.save()
        elif position == 'shortonly':
            self.positions.loc[row][column] = 'closed'
            self.shorts.loc[row][column] = 0.0
            self.vacancies[short_ex] += 1
            self.save()
        elif position == 'shortonly-':
            self.positions.loc[row][column] = 'closed'
            self.shorts.loc[row][column] = 0.0
            self.vacancies[short_ex] += 2
            self.save()
        else:
            raise RuntimeError('Cannot close nonexistent short')

    def get_position(self, long_ex, long_coin, short_ex, short_coin):
        """Return the position (open, closed, long/short only) for given pair."""
        row = utils.encode_symbol(long_ex, long_coin)
        column = utils.encode_symbol(short_ex, short_coin)
        if row not in self.positions.index or column not in self.positions.columns:
            return 'closed'
        return self.positions.loc[row][column]

    def get_vacancies(self, exchange):
        """Return the number of vacant fiefdoms for exchange."""
        if exchange not in self.vacancies:
            self.vacancies[exchange] = self.DEFAULT_FIEFDOMS
        return self.vacancies[exchange]

    def check_overlap(self, type, exchange, coin):
        """Return True if more than one position is open for coin on exchange.
           Raise ValueError if 'type' is not 'long' or 'short'.
        """
        symbol = utils.encode_symbol(exchange, coin)
        count = 0
        if type == 'long':
            position_counts = self.positions.loc[symbol].value_counts()
            for position_type in ['longonly', 'longonly+', 'longonly-', 'open', 'open+']:
                if position_type in position_counts.index:
                    count+= position_counts[position_type]

        elif type == 'short':
            position_counts = self.positions[symbol].value_counts()
            for position_type in ['shortonly', 'shortonly+', 'shortonly-', 'open', 'open+']:
                if position_type in position_counts.index:
                    count+= position_counts[position_type]

        else:
            raise ValueError('Invalid overlap type!')
        if count > 1:
            return True
        return False

    def find_anomalies(self):
        """Return a list of 'longonly' and 'shortonly' positions, each entry a
           dict containing long/short exchanges, coins, and quantities.
        """
        long_only = []
        short_only = []
        for row in self.positions.index:
            for column in self.positions.columns:
                position = self.positions.loc[row][column]
                long_anoms = ['longonly, longonly+, longonly-']
                short_anoms = ['shortonly, shortonly+, shortonly-']
                if position in (long_anoms + short_anoms):
                    anomaly = {}
                    long_ex, long_coin = utils.parse_symbol(row)
                    anomaly['long_exchange'] = long_ex
                    anomaly['long_coin'] = long_coin
                    short_ex, short_coin = utils.parse_symbol(column)
                    anomaly['short_exchange'] = short_ex
                    anomaly['short_coin'] = short_coin

                    if position in long_anoms:
                        anomaly['quantity'] = self.longs.loc[row][column]
                        long_only.append(anomaly)
                    elif position in short_anoms:
                        anomaly['quantity'] = self.shorts.loc[row][column]
                        short_only.append(anomaly)

        return long_only, short_only

    def __str__(self):
        """Print the string representation of this State."""
        string = 'POSITIONS\n' + self.positions.to_string() + '\n\n'
        string += 'LONGS\n' + self.longs.to_string() + '\n\n'
        string += 'SHORTS\n' + self.shorts.to_string() + '\n\n'
        string += 'VACANCIES: ' + str(self.vacancies)
        return string

    ########################
    #    HELPER METHODS    #
    ########################

    def assure_value(self, df, row, column, default_value):
        # Return value at row, column in df; if not in df, add it and set to default_value
        if row not in df.index:
            df.loc[row] = pd.Series(default_value, df.columns)
        if column not in df.columns:
            df.loc[:, column] = pd.Series(default_value, df.index)
        return df.loc[row][column]

    def save(self):
        # Save variables to a dump file, overwriting the original file
        with open('savedstate.pkl', 'wb') as f:
            pickle.dump(self.__dict__, f)
