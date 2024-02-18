import sys
sys.path.append("..")
from config.config_parser import get_config
from confluent_kafka import admin

conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS')}
try:
    admin = admin.AdminClient(conf)
    print("=" * 150)
    topic_list = admin.list_topics().topics
    topic_list = str(topic_list).split(", '")
    for i in topic_list:
        print(i.replace('{','').replace("'",''))
    print("=" * 150)
except Exception as err:
    print(f'Error while creating listing topics - {err}')
