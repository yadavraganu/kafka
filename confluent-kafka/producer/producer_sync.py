from faker import Faker
import os
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException
dotenv_path = os.path.join(os.path.dirname(__file__).split('producer')[0], '.env')
load_dotenv(dotenv_path, verbose=True)

cf = Faker()
kafka_conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'], 'acks': 'all', 'compression.type': 'gzip'}

try:
    producer = Producer(kafka_conf)
    producer.poll(0)
    # Sending Asynchronously with callback
    for i in range(0, 10):
        future = producer.produce(topic='test', key=cf.name(), value=cf.address())
        try:
            producer.flush()
        except Exception as e:
            print(f'Error while sending message.Error is : {e}')
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')

