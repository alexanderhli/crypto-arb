from xmlreader import XML

x = XML('portfolio.xml')

#x.add_long('xrp', 123, 'binance')
x.add_short('xrp', 321, 'bitmex')
#x.add_long('ltc', 444, 'bittrex')

print(x.has_anomaly())

#y = XML('portfolio.xml')

#y.add_long('piss', 222, 'shit')