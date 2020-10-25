from exchanges import Bitmex

#apiKey = 'R6R1UjARaSX71JqLmOZvC7Ur'
#apiSecret = 'tLZ9tJoIl7717SI7c--UknsJhuTzE8TYoW_5i8xC5X0SBa44'

#client = bm.bitmex(test=False, api_key=apiKey, api_secret=apiSecret)

#bch_price = client.Quote.Quote_get(symbol='BCHM18', reverse=True, count=1).result()[0][0]['askPrice']
#xrp_price = client.Quote.Quote_get(symbol='XRPM18', reverse=True, count=1).result()[0][0]['bidPrice']

#print(bch_price)
#print(xrp_price)

#print(client.User.User_getMargin().result()[0]['walletBalance'])

bitmex = Bitmex(0)

print(bitmex.get_balance())

#bitmex.buy('BCHM18', 1, 2)

#print(client.Position.Position_updateLeverage(symbol='XRPM18', leverage=3).result())

#client.Order.Order_new(symbol='ADAM18', execInst='Close').result()

#client.Order.Order_new(symbol='ADAM18', ordType = 'Market', orderQty=1).result()
#client.Order.Order_new(symbol='XRPM18', ordType = 'Market', orderQty=-1).result()

#print(client.Position.Position_updateLeverage(symbol='XRPM18', leverage=2.35).result())

print(bitmex.get_bid('ADAM18'))
print(bitmex.get_ask('ADAM18'))