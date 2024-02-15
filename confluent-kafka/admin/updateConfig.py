import os
from confluent_kafka import admin
from confluent_kafka.admin import ConfigEntry, ConfigResource, AlterConfigOpType
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__).split('admin')[0], '.env')
load_dotenv(dotenv_path, verbose=True)

conf = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']}

"""List of config entries which needs to updated. Use AlterConfigOpType code to delete, set,append entries 0 is for 
set operation"""

config = [ConfigEntry(name='min.cleanable.dirty.ratio', value="0.5", incremental_operation=AlterConfigOpType(0)),
          ConfigEntry(name='retention.ms', value="3600", incremental_operation=AlterConfigOpType(0))]

"""List of resources for which config entries needs to updated"""

lst = [ConfigResource(restype=2, name='Test1', incremental_configs=config)]
try:
    admin_client = admin.AdminClient(conf)
    print("=" * 150)
    desc = admin_client.incremental_alter_configs(lst)
    for i in desc:
        desc[i].result()
    print("=" * 150)
except Exception as err:
    print(f'Error while updating configs - {repr(err)}')
