from time import sleep
from json import dumps
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

def current_milli_time():
    return round(time.time() * 1000)

for e in range(1000):
    data = {'number' : e, 'timestamp': current_milli_time() }
    producer.send('numtest', value=data)
    print('Message produced: ' + str(data))
    sleep(5)


