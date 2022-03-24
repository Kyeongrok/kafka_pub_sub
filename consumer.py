
from confluent_kafka import Consumer
import time

consumer = Consumer({
    'bootstrap.servers': 'kb-broker01:9092,kb-broker02:9092,kb-broker03:9092',
    'group.id': 'foo',
    # "enable.auto.commit":False,
    'auto.offset.reset': 'earliest'})

topic_name = 'test_log'
consumer.subscribe([topic_name])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg != None:
        print(msg.value())
    else:
        print('Msg is none.')

    time.sleep(0.1)