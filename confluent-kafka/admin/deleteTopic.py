from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__).split('admin')[0], '.env')
load_dotenv(dotenv_path, verbose=True)
conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']}
topics = ['Test1']
try:
    admin = AdminClient(conf)
    tc = admin.delete_topics(topics)
    for i, j in tc.items():
        try:
            j.result()
            print(f'Topic {tc} deleted')
        except Exception as e:
            print("Failed to delete topic {}: {}".format(i, e))
except KafkaException as err:
    print(f'Error occurred while connecting broker - {err}')
