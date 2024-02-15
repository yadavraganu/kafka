from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__).split('admin')[0], '.env')
load_dotenv(dotenv_path, verbose=True)
conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']}
topic = NewTopic(topic='Test1', num_partitions=int(os.environ["TOPIC_PARTITION_NUM"]),
                 replication_factor=int(os.environ['REPLICATION_FACTOR']), config={'retention.ms': '360000'})
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
