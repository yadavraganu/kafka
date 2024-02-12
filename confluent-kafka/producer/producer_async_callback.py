from faker import Faker
import time
from confluent_kafka import Producer, KafkaException

cf = Faker()
kafka_conf = {'bootstrap.servers': 'localhost:8098', 'acks': 'all','compression.type':'gzip'}


def callback(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

try:
    producer = Producer(kafka_conf)
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')
producer.poll(0)
# Sending Asynchronously with callback
try:
    for i in range(0, 10):
        producer.produce(topic='test', key=cf.name().encode('utf-8'), value=cf.address().encode('utf-8'), callback=callback)
        producer.poll(0)
except Exception as e:
    print(f'Error while sending message.Error is : {e}')
finally:
    producer.flush()
