# -*- coding: utf-8 -*-
"""
Description: This script fetches the stock information from Alpha Vantage site using the TimeSeries API
             The data is then sent as json records to Kafka topics. 
Prequisites: Ensure Kafka brokers are running and the parameters passed correctly.
Usage: python RetrieveStockDataSentToKafka.py --apikey <key> [--interval <seconds>] [--kafkabootstrapserver <server:port>] [--kafkatopic <topicname>] 
@author: Shyamal Akruvala
"""
from alpha_vantage.timeseries import TimeSeries
import time, json, argparse, sys, os
import pandas as pd
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='Stock Data Fetching Program')
parser.add_argument('--apikey', dest='apikey', type=str, required=True, help='API Key from Alpha Vantage')
parser.add_argument('--interval', dest='interval', type=int, required=False, help='Interval in seconds between API calls')
parser.add_argument('--kafkabootstrapserver', dest='kafkabootstrapserver', type=str, required=False, help='Kafka Bootstrap Server details and port')
parser.add_argument('--kafkatopic', dest='kafkatopic', type=str, required=False, help='Kafka topic to send the data')

parser.set_defaults(interval=100, kafkabootstrapserver='localhost:9092', kafkatopic='stocks')
args = parser.parse_args()

# Create a TimeSeries object to fetch data via API call. Please visit Alpha Vantage to get your API Key (https://www.alphavantage.co/support/#api-key)
# Check documentation on https://alpha-vantage.readthedocs.io/en/latest/index.html to understand the various output_formats 
ts = TimeSeries(key=args.apikey, output_format='pandas')

def fetchStockData(ticker):
    """
    This method takes a string as an input and fetches data from Alpha Vantage site for the same ticker name
    e.g. MSFT for Microsoft
    """
    data, meta_data = ts.get_intraday(symbol=ticker, interval='1min', outputsize='compact') # compact outputsize to get latest 100 records
    print("Data fetched for ticker:" + ticker)
    data = data[:1] # Taking the first row only for analysis and processing
    data['ticker'] = ticker # adding the ticker details to the dataframe
    data = data.reset_index()
    data['date'] = (data['date'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s') # converting the datetime to epoch timestamp  
    data = data.rename(columns = {'date':'timestamp',
                             '1. open':'open',
                             '2. high':'high',
                             '3. low':'low',
                             '4. close':'close',
                             '5. volume':'volume'
                             }) # renaming the columns for simplicity
    cols = ['ticker','timestamp','open','high','low','close','volume']
    data = data.reindex(columns=cols) # rearranging the columns
    #print(data)
    #return data.to_json(orient='columns', date_format='iso')
    return data.to_dict(orient='list')


print("   _____ _             _      _____      _            _____        _          ______   _       _     ")
print("  / ____| |           | |    |  __ \    (_)          |  __ \      | |        |  ____| | |     | |    ")
print(" | (___ | |_ ___   ___| | __ | |__) | __ _  ___ ___  | |  | | __ _| |_ __ _  | |__ ___| |_ ___| |__  ")
print("  \___ \| __/ _ \ / __| |/ / |  ___/ '__| |/ __/ _ \ | |  | |/ _` | __/ _` | |  __/ _ \ __/ __| '_ \ ")
print("  ____) | || (_) | (__|   <  | |   | |  | | (_|  __/ | |__| | (_| | || (_| | | | |  __/ || (__| | | |")
print(" |_____/ \__\___/ \___|_|\_\ |_|   |_|  |_|\___\___| |_____/ \__,_|\__\__,_| |_|  \___|\__\___|_| |_|")
                                                                                                     
if __name__ == "__main__":
    """
    This is the main method.
    """
    try:
        while True:  # This constructs an infinite loop. The script will keep executing and fetching the stock data until interrupted.
            print("Retrieving stock information from site.....")
            print("###########################################")
            
            # Initialize the Kafka Producer
            kProducer = KafkaProducer(
                    bootstrap_servers=[args.kafkabootstrapserver],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

            # Below is the list of stock tickers for which stock information would be fetched.
            stockTickerList = ['CTSH', 'MSFT', 'GOOGL']
            
            for ticker in stockTickerList:
                stockInfoDF = fetchStockData(ticker)
                #print(stockInfoDF)
                for k,v in stockInfoDF.items():
                    stockInfoDF[k] = str(stockInfoDF[k]).replace('[','').replace(']','').replace("'",'') # remove brackets and quotes
                if stockInfoDF is not None:
                    kProducer.send(args.kafkatopic, stockInfoDF)

            print("Script going to sleep for " + str(args.interval) + " seconds")
            time.sleep(args.interval)
            print("###########################################")
    except KeyboardInterrupt:
        print('''Program execution interrupted. Good Bye and have a nice day''')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)