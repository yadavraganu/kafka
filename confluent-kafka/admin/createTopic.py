from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from pprint import pprint

conf = {'bootstrap.servers': 'localhost:8098'}
topic = NewTopic(topic='Test1', num_partitions=3, replication_factor=3)
try:
    admin = AdminClient(conf)
    tc = admin.create_topics([topic])
    for i, j in tc.items():
        try:
            j.result()
            pprint(f'Topic {tc} Created')
        except Exception as e:
            print("Failed to create topic {}: {}".format(i, e))
except KafkaException as err:
    pprint(f'Error occurred while connecting broker - {err}')
