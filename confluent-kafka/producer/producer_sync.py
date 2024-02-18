import sys
sys.path.append("..")
from config.config_parser import get_config
from faker import Faker
from confluent_kafka import Producer, KafkaException


cf = Faker()
kafka_conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS'), 'acks': 'all', 'compression.type': 'gzip'}

try:
    producer = Producer(kafka_conf)
    producer.poll(0)
    # Sending Asynchronously with callback
    for i in range(0, 10):
        future = producer.produce(topic='Test', key=cf.name(), value=cf.address())
        try:
            producer.flush()
        except Exception as e:
            print(f'Error while sending message.Error is : {e}')
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')

