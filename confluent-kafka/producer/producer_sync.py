from faker import Faker
import time
from confluent_kafka import Producer, KafkaException

cf = Faker()
kafka_conf = {'bootstrap.servers': 'localhost:8098', 'acks': 'all', 'compression.type': 'gzip'}

try:
    producer = Producer(kafka_conf)
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')
producer.poll(0)
# Sending Asynchronously with callback
for i in range(0, 10):
    future = producer.produce(topic='test', key=cf.name().encode('utf-8'), value=cf.address().encode('utf-8'))
    try:
        producer.flush()
    except Exception as e:
        print(f'Error while sending message.Error is : {e}')