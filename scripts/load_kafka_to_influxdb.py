import os
from kafka import KafkaConsumer
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()

topic_name = "twitterdata-clean"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='twitter-group'
    # value_deserializer=lambda x: loads(x.decode('utf-8'))
)

token = os.getenv('influxdb_token')
org = os.getenv('influxdb_org')
bucket = os.getenv('influxdb_bucket')

for message in consumer:

    mvalue = json.loads(message.value.decode('utf-8'))
    mkey = message.key
    mpart = message.partition
    moffset = message.offset

    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print(message)
    dataPoint = Point("nba_tweet")\
        .tag("polarity", mvalue['polarity']) \
        .field("polarity_v", mvalue['polarity_v']) \
        .field('subjectivity_v', mvalue['subjectivity_v']) \
        .field('text', mvalue['text'])\
        .time(mvalue['ts'])

    print(dataPoint)

    write_api.write(bucket=bucket, record=dataPoint)
