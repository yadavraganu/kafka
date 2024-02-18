import sys
sys.path.append("..")
from config.config_parser import get_config
from confluent_kafka import KafkaException, Consumer, KafkaError


def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        for partition in partitions:
            print('Committed offset {} for partition {}'.format(partition.offset, partition.partition))


kafka_conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS'), 'group.id': 'Test_App', 'auto.offset.reset': 'earliest',
              'on_commit': commit_completed}
consumer = Consumer(kafka_conf)
consumer.subscribe(['Test'])

try:
    Flag = True
    while Flag:
        msg = consumer.poll(10)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            if msg.error():
                raise KafkaException(msg.error())
        else:
            print(msg.offset(), msg.key(), msg.value())
            consumer.commit(asynchronous=True)
finally:
    consumer.close()
