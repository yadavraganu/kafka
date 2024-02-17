from confluent_kafka import KafkaException, Consumer, KafkaError
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__).split('consumer')[0], '.env')
load_dotenv(dotenv_path, verbose=True)

kafka_conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'], 'group.id': 'Test_App', 'auto.offset.reset': 'latest'}
consumer = Consumer(kafka_conf)
consumer.subscribe(['test'])

try:
    Flag = True
    while Flag:
        msg = consumer.poll(100)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            if msg.error():
                raise KafkaException(msg.error())
        else:
            print(msg.offset(), msg.key(), msg.value())
finally:
    consumer.close()
