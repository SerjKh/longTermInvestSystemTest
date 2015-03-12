# Starting gathering code here
#Example using ystockquote lib
#import ystockquote


#pprint(ystockquote.get_historical_prices('GOOG', '2013-01-03', '2013-01-08'))  #Does not work, has no valid url
#pprint(ystockquote.get_all('GOOG'))

#Example using yahoo_finance lib
from pprint import pprint

from yahoo_finance import *

#yahoo = Share('AAPL')
#print yahoo.get_open()
#pprint(yahoo.get_historical('2000-09-27', '2000-09-27')[0]['Close'])
#yahoo.printSymbol()
# #print yahoo.get_open()
# #print yahoo.get_trade_datetime()
#pprint(yahoo.get_historical('2014-04-25', '2014-04-29'))

#Example using quandle
import Quandl
#mydata = Quandl.get("RAYMOND/AAPL_REVENUE_A") #using Raymond
#mydata = Quandl.get("SEC/AAPL_REVENUE_A") #using SEC
#Using DMDRN it is possible to get PE/
# mydata = Quandl.get("DMDRN/AAPL_ROC") #using DMDRN
#mydata = Quandl.get("DMDRN/AAPL_PE_CURR") #using DMDRN
#mydata = Quandl.get("DMDRN/INTC_ROE") #using DMDRN
mydata = Quandl.get("WIKI/AAPL.11", collapse='annual')  # Column 11 is for Adj. closed price
pprint(mydata)
#mydata = Quandl.get("DMDRN/INTC_PE_CURR") #using DMDRN on active stock
#pprint(mydata)
#mydata = Quandl.get("SF1/IBM_REVENUE_ART") #using SF1 premium database
#mydata = Quandl.get("DMDRN/AABC_PE_CURR") #using DMDRN on inactive stock
#pprint(mydata)