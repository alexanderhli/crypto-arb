from exchanges import Bittrex
import pricegetter as pg

bittrex = Bittrex()

print(bittrex.get_balance())
#print(bittrex.get_ask('XRP'))

#print(pg.get_bittrex(['XRP'], 'hour'))
