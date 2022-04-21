from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import datetime

topic_name = "twitterdata"

consumer = KafkaConsumer(
    topic_name,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='twitter-group'
     # value_deserializer=lambda x: loads(x.decode('utf-8'))
)

#client = MongoClient('localhost:27017')
#collection = client.numtest.numtest

for message in consumer:
    message = message.value
#    collection.insert_one(message)
#    print('{} added to {}'.format(message, collection))
    print(f'Message received: {message}')
    
    # data_num = message['number']
    # data_ms = message['timestamp']
    # data_ts = datetime.datetime.fromtimestamp(data_ms/1000.0)

    # if data_ts > datetime.datetime.now() - datetime.timedelta(minutes=5):
    #     print(data_num,data_ts)

#    print('Convert ms to timestamp: ' + str(datetime.datetime.fromtimestamp(message['timestamp']/1000.0)))
