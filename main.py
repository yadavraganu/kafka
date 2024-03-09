from config_parser import get_config
from kafka.admin.getTopicList import list_topics

list_topics(get_config('BOOTSTRAP_SERVERS'))
print(get_config('BOOTSTRAP_SERVERS'))