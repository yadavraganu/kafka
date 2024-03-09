import sys
from config_parser import get_config
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS')}
topic = NewTopic(topic='Test', num_partitions=int(get_config('TOPIC_PARTITION_NUM')),
                 replication_factor=int(get_config('REPLICATION_FACTOR')), config={'retention.ms': '360000'})
try:
    admin = AdminClient(conf)
    tc = admin.create_topics([topic])
    for i, j in tc.items():
        try:
            j.result()
            print(f'Topic {tc} Created')
        except Exception as e:
            print("Failed to create topic {}: {}".format(i, e))
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')
