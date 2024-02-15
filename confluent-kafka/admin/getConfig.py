import os
from confluent_kafka import admin
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__).split('admin')[0], '.env')
load_dotenv(dotenv_path, verbose=True)

conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']}
# List of resources to get config for
lst = [admin.ConfigResource(2,'Test1')]
try:
    admin_client = admin.AdminClient(conf)
    print("=" * 150)
    desc = admin_client.describe_configs(lst)
    for i in desc:
        confs = str(desc[i].result()).replace('{','').replace('}','').split(', ')
        for j in confs:
            print(j)
    print("=" * 150)
except Exception as err:
    print(f'Error while creating listing topics - {err}')
