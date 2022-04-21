import tweepy
import os
from kafka import KafkaProducer
import json
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()

# Twitter credentials
consumer_key = os.getenv('twitter_consumer_key')
consumer_secret = os.getenv('twitter_consumer_secret')
access_token = os.getenv('twitter_access_token')
access_token_secret = os.getenv('twitter_access_token_secret')

topic_name = "twitterdata"
logger = logging.getLogger(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


def normalize_timestamp(time):
    mytime = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    return mytime.strftime('%Y-%m-%d %H:%M:%S')


class TwitterStreamer():
    def stream_data(self):
        self.logger.info(f"{topic_name} Stream starting...")

        twitter_stream = MyListener(consumer_key, consumer_secret, access_token, access_token_secret)
        twitter_stream.filter(track=["nba"])


class MyListener(tweepy.Stream):
    def on_data(self, data):
        try:
            data_json = json.loads(data)
            id = data_json['id']
            text = data_json['text'].encode('utf-8')
            user = data_json['user']
            reply_count = data_json['reply_count']
            retweet_count = data_json['retweet_count']
            lang = data_json['lang']
            timestamp = data_json['timestamp_ms']

            # data = {
            #     'id' : id,
            #     'text': text,
            #     'user': user,
            #     'reply_count': reply_count,
            #     'retweet_count': retweet_count,
            #     'lang': lang,
            #     'timestamp': pd.to_datetime(float(timestamp), unit='ms')
            # }
            # print(data_json.keys())

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

