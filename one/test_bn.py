from exchanges import Binance

bn = Binance()

#print(bn.test())
#print(bn.get_balance())
#print(bn.buy('ETH', 0.1))
#print(bn.close('ETH'))
print(bn.get_ask('LTC'))