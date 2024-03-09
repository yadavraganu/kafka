import sys
sys.path.append("..")
from config.config_parser import get_config
from confluent_kafka import KafkaException, Consumer, KafkaError

kafka_conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS'), 'group.id': 'Test_App',
              'auto.offset.reset': 'latest'}
consumer = Consumer(kafka_conf)
consumer.subscribe(['Test'])

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
