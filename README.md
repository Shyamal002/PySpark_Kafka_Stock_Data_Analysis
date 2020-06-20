# PySpark_Kafka_Stock_Data_Analysis
My experiments with PySpark Structured Streaming and Kafka to process near real-time data of stocks

## Source:
The TimeSeries object of alpha_vantage package is used to fetch data via API calls.
An API key is needed to access the data which can be obtained at [Alpha Vantage API Site](https://www.alphavantage.co/support/#api-key)
Details on the alpha_vantage package can be reviewed at [Alpha Vantage Documentation Site](https://alpha-vantage.readthedocs.io/en/latest/index.html)

Once **Zookeeper** and **Kafka Servers** are up and running, the following command can be fired to start the script to fetch the data
> python RetrieveStockDataSentToKafka.py --apikey <key> [--interval <seconds>] [--kafkabootstrapserver <server:port>] [--kafkatopic <topicname>] 

e.g. python RetrieveStockDataToKafka.py --apikey ZZZZZZZZZZZZZZZZZZZZ --interval 100
Defaults are -> interval=100, kafkabootstrapserver='localhost:9092', kafkatopic='stocks'
