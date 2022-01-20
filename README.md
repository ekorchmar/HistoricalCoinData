# HistoricalCoinData
A short script to extract weekly historical data using coinmarketcap.api. 

API credit calculation not included. You will need to provide your own API key in keys.json file.
If your API access tier allows for more than 60 calls/min you may want to remove time.sleep() calls and actualy use multithreading to make API calls simultaneously.
