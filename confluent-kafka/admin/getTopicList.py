import os
from confluent_kafka import admin
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__).split('admin')[0], '.env')
load_dotenv(dotenv_path, verbose=True)

conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']}
try:
    admin = admin.AdminClient(conf)
    print("=" * 50)
    topic_list = admin.list_topics().topics
    topic_list = str(topic_list).split(", '")
    for i in topic_list:
        print(i.replace('{','').replace("'",''))
    print("=" * 50)
except Exception as err:
    print(f'Error while creating listing topics - {err}')
