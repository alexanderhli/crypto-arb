from exchanges import Binance
from exchanges import Bitmex

binance = Binance()
bitmex = Bitmex(0)

binance.buy('XRP', 1)
bitmex.sell('XRPM18', 1, 2)
