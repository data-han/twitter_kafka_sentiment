import tweepy
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging

load_dotenv()

# Twitter credentials
consumer_key = os.getenv('twitter_consumer_key')
consumer_secret = os.getenv('twitter_consumer_secret')
access_token = os.getenv('twitter_access_token')
access_token_secret = os.getenv('twitter_access_token_secret')

logger = logging.getLogger(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server

topic_name = "twitterdata"
search_input = "nba, #NBA, NBA, #nba, Nba"


class TwitterStreamer():
    def stream_data(self):
        logger.info(f"{topic_name} Stream starting for {search_input}...")

        twitter_stream = MyListener(consumer_key, consumer_secret, access_token, access_token_secret)
        twitter_stream.filter(track=[search_input])


class MyListener(tweepy.Stream):
    def on_data(self, data):
        try:
            logger.info(f"Sending data to kafka...")
            producer.send(topic_name, data)
            print(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_data()
