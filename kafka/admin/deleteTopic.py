import sys
sys.path.append("..")
from config.config_parser import get_config
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS')}
topics = ['Test1']
try:
    admin = AdminClient(conf)
    tc = admin.delete_topics(topics)
    for i, j in tc.items():
        try:
            j.result()
            print(f'Topic {tc.i} deleted')
        except Exception as e:
            print("Failed to delete topic {}: {}".format(i, e))
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')
