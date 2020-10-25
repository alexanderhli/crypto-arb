import os
from state import State
from lxml import etree as ET

class XML:
    def __init__(self, filename):
        self.filename = filename
        self.build_tree()

    def build_tree(self):
        if os.path.exists(self.filename):
            self.tree = ET.parse(self.filename)
            self.root = self.tree.getroot()
        else:
            self.root = ET.Element('portfolio')
            self.tree = ET.ElementTree(self.root)
            self.tree.write(self.filename)

    def add_long(self, coin, quantity, exchange):
        self.build_tree()
        pair = ET.Element('pair')
        self.root.append(pair)
        long_trade = ET.Element('long', symbol=coin, size=str(quantity), exchange=exchange)
        pair.append(long_trade)
        self.tree.write(self.filename)

    # call add_long before add_short
    def add_short(self, coin, quantity, exchange):
        self.build_tree()
        pair = None
        for child in self.root:
        	if len(child.getchildren()) == 1:
        		pair = child
        short_trade = ET.SubElement(pair, 'short', symbol=coin, size=str(quantity), exchange=exchange)
        self.tree.write(self.filename)

    def remove_long(self, coin, exchange):
        self.build_tree()
        for child in self.root:
            long_element = child.find('long')
            if long_element.get('symbol') == coin and long_element.get('exchange') == exchange:
                child.remove(long_element)
                if len(child.getchildren()) == 0:
                    self.root.remove(child)
        self.tree.write(self.filename)
    
    # call remove_long before remove_short
    def remove_short(self, coin, exchange):
        self.build_tree()
        for child in self.root:
            if len(child.getchildren()) == 1:
                self.root.remove(child)
        self.tree.write(self.filename)

    def update_state(self, state):
        self.build_tree()
        for child in self.root:
            if len(child.getchildren()) == 2:
	            long_element = child.find('long')
	            exchange = long_element.get('exchange')
	            long_coin = long_element.get('symbol')
	            long_size = float(long_element.get('size'))
	            short_element = child.find('short')
	            short_coin = short_element.get('symbol')
	            short_size = float(short_element.get('size'))
	            state.open_position(exchange, long_coin, long_size, short_coin, short_size)
        return state

    def has_anomaly(self):
        self.build_tree()
        boolean = False
        for child in self.root:
            if len(child.getchildren()) == 1:
                boolean = True
        return boolean
    
    def get_anomaly(self):
        self.build_tree()
        anomaly = None
        for child in self.root:
            if len(child.getchildren()) == 1:
                anomaly = list(child)[0]
        exchange = anomaly.get('exchange')
        coin = anomaly.get('symbol')
        size = float(anomaly.get('size'))
        return exchange, coin, size
