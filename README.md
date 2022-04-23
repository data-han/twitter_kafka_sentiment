# Twitter Kafka Real-Time Streaming

## Background
It's the time of the year and the NBA playoffs are happening right now. While I was watching the highlights on YouTube, I often find myself scrolling down to see what kind of observations do others have. Interestingly enough, there are some people rooting for their favorite teams but there are also comments criticising certain players or plays.

This has led me to think if I could obtain real-time data from Twitter on this topic. Therefore, I wanted to build a pipeline to obtain real-time Tweets on #NBA while learning about a few new technologies including Kafka and Spark streaming. Here, I managed to build a real-time dashboard using InfluxDB & Grafana, performing a simple sentiment analysis and showing the polarity (positive/negative/neutral) of each tweet over time. Pretty interesting to see what people are saying especially during game-time.

## Introduction/ Set up
In this set-up, I am using the following tools:

1. tweepy python library
   1. Purpose: to obtain streaming Tweets
   2. Obtain Twitter Developer credentials including consumer key, consumer secret, access token, access token secret
2. Kafka
   1. Purpose: store streaming data while waiting to be processed by producer
   2. Info
      1. Event: an event records the fact that 'something happened'
      2. Topic: a particular stream of data, like a 'table' in a database or 'folder' in filesystem
      3. Producers: client applications writing data to Kafka
      4. Consumers: client applications reading data from Kafka
         1. Consumer offset: Kafka stores offsets at which consumer group has been reading (checkpoint which offset has been read by consumer)
         2. when a consumer dies, it should be able to read off from where it left off
         3. Current offset: current record the consumer reads
         4. Log-end offset: record which has been published by producer to Kafka
         5. Lag: how lag is the consumer reading the Kafka messages (how many more to go?)
      5. Brokers: 
         1. A Kafka cluster is comprised of at least 3 brokers
         2. Each broker contains certain topic partitions - distributed processing
      6. Zookeeper:
         1. manages brokers together
         2. helps in performing leader election for partitions
   3. Set-up
      1. Start Zookeeper first
         1. bin/zookeeper-server-start.sh config/zookeeper.properties
      2. Start Kafka Broker service
         1. bin/kafka-server-start.sh config/server.properties
      3. Create topic
         1. bin/kafka-topics.sh --create --topic twitterdata --bootstrap-server localhost:9092
3. Structured Streaming via Pyspark
   1. pip install pyspark
   2. when running Kafka integration with structured streaming, need to use:
      1. spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 consumer_or_producer.py
4. InfluxDB
   1. Purpose: store time-series data
   2. Info
      1. Bucket: named location where time series data is stored
      2. Field: key-value pair data structure that stores meta data and actual data value. - not indexed
      3. Measurement: loosely equivalent to concept of table in database. Consists of 3 types of columns - time, tag, fields
      4. Time: track timestamp 
      5. Tag: optional - like categories or usually commonly-queried columns
   3. Set-up
      1. Brew install influxdb-cli
      2. create config - influx config create --config <name> --host-url <localhost_port> --org <name> --token <token> --active

5. Grafana
   1. Purpose: monitor data in real-time, usually for monitoring operational activity/ logs
      1. Set-up
         1. brew install grafana
         2. brew services start grafana

# Credits
https://datasciencechalktalk.wordpress.com/2019/07/17/streaming-analysis-with-kafka-influxdb-and-grafana/
https://towardsdatascience.com/twitter-sentiment-analysis-a-tale-of-stream-processing-8fd92e19a6e6
https://blaqfireroundup.wordpress.com/2021/11/01/kafka-and-spark-structured-streaming/