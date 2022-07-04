import requests
import pandas as pd
import time


class binanceTickers:
    
    def __init__(self, apiUrl):
        self.apiUrl = apiUrl
        

    def topQuoteAsset(self, asset, value,  out=False):

        path = "/v3/ticker/24hr"
        getResults = requests.get( self.apiUrl + path )
        dataSet = pd.DataFrame(getResults.json())
        # Reconfiguring the columns 
        dataSet = dataSet[['symbol', value]]
        # Filtering the Asset
        dataSet = dataSet[dataSet['symbol'].str.contains((r'{}$').format(asset))]
        dataSet[value] = dataSet[value].astype(float)
        # Sorting by Decsending Volume 
        dataSet = dataSet.sort_values(by=value, ascending=False).head(5)
        if out:
            print("\ntop 5 symbols with quote asset %s and the highest %s over the last 24 hours in descending order\n" %(asset, value))
            print(dataSet)
        return dataSet


    def notionVal(self, asset, value, top_num, out=False):

        path = "/v3/depth"

        n_list = {}
        
        top_sym = self.topQuoteAsset(asset, value)["symbol"]

        for symb in top_sym:
            payload = { 'symbol' : symb, 'limit' : top_num }
            res = requests.get( self.apiUrl + path, params=payload )
            for col in ["bids", "asks"]:
                n = pd.DataFrame(data=res.json()[col], columns=["price", "volume"], dtype=float)
                n = n.sort_values(by="price", ascending=False).head(200)
                n["notion_value"] = n["price"] * n["volume"]
                n_list[ symb + "_" + col ] =  n["notion_value"].sum() 
            
        if out:
            print( "\nThe notional value for top %s bids for quote asset %s by %s is - \n" %( top_num, asset, value ))
            print(n_list)
            
        return(n_list)

    
    def priceSpread(self, asset, value, out=False):

        path = "/v3/ticker/bookTicker"
        price_spread = {}

        top = self.topQuoteAsset(asset, value)
        for symb in top["symbol"]:
            parameters = { "symbol" : symb }
            req = requests.get( apiUrl + path, params=parameters )
            
            req = pd.DataFrame([req.json()])
            cols = ["bidPrice", "askPrice"]
            req[cols] = req[cols].astype(float)
            price_spread[ symb ] = float(req["askPrice"] - req["bidPrice"])
        
        if out:
            print("\nThe price spread for %s by %s is - \n" %(asset, value))
            print("%s\n" %(price_spread))
 

        return price_spread


    def Delta(self, out=False):

        delta = {}
        old = self.priceSpread("USDT", 'count', out=False)
        time.sleep(10)
        new = self.priceSpread("USDT", 'count', out=False)
        
        for key in old:
            delta[key] = float(abs(old[key] - new[key]))

        if out:
            print("\nThe absolute delta - ")
            print(delta)

        return(delta)







if __name__ == "__main__":

    
    apiUrl = "https://api.binance.com/api"
    binanceTic = binanceTickers(apiUrl)


    #print("\n Q1 - Print the top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order.\n")
    binanceTic.topQuoteAsset("BTC", "volume", True)

    #print("\n Q2 - Print the top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order.\n")
    binanceTic.topQuoteAsset("USDT", "count", True)
    
    #print("\n Q3 - Using the symbols from Q1, what is the total notional value of the top 200 bids and asks currently on each order book?\n")
    binanceTic.notionVal("BTC", "volume", 200, True)
    
    
    #print("\n Q4 - What is the price spread for each of the symbols from Q2?\n")
    binanceTic.priceSpread("USDT", "count", True)

    
    #print("\n Q5 - Every 10 seconds print the result of Q4 and the absolute delta from the previous value for each symbol.\n")
    while True:
        binanceTic.Delta(out=True)
    
