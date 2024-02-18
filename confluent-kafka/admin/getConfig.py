import sys
sys.path.append("..")
from config.config_parser import get_config
from confluent_kafka import admin

conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS')}
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
    print(f'Error while getting list of configs - {err}')
