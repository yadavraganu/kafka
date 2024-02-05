from confluent_kafka.admin import AdminClient
from pprint import pprint

conf = {'bootstrap.servers': 'localhost:8098'}
try:
    admin = AdminClient(conf)
    pprint("=" * 50)
    topic_list = admin.list_topics().topics
    i = 0
    for t in topic_list:
        i += 1
        pprint(f'Topic {i} : {t}')
    pprint("=" * 50)
except Exception as err:
    pprint(f'Error occurred while connecting broker - {err}')

