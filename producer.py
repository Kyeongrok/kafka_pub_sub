import json, time
from confluent_kafka import Producer
from data import get_resistered_user

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


conf = {'bootstrap.servers': 'kb-broker01:9092,kb-broker02:9092,kb-broker03:9092',
        'client.id': 'foo'}

producer = Producer(conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_resistered_user()
        # print(registered_user)
        # producer.produce("test_log", 'eee', registered_user['name'])

        producer.produce('test_log', key="key", value="value", callback=acked)
        producer.poll(2)
        time.sleep(1)


