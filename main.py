from config_parser import get_config
import json
from kafka.admin.getTopicList import list_topics
from kafka.admin.createTopic import createTopic
from kafka.admin.getConfig import getConfig
from kafka.admin.deleteTopic import deleteTopic
from kafka.producer.producer_sync import produce_sync

# Get config
BOOTSTRAP_SERVERS = get_config('BOOTSTRAP_SERVERS')
TOPIC_NAME = get_config('TOPIC_NAME', section='createtopic')
TOPIC_PARTITION_NUM = get_config('TOPIC_PARTITION_NUM', section='createtopic')
REPLICATION_FACTOR = get_config('REPLICATION_FACTOR', section='createtopic')
RETENTION_MS = get_config('RETENTION_MS', section='createtopic')
TOPIC_NAMES = get_config('TOPIC_NAMES', section='deletetopic')
RESTYP = get_config('RESTYP', section='getconfig')
RESNAME = get_config('RESNAME', section='getconfig')

# Getting list if topics
list_topics(BOOTSTRAP_SERVERS)

# Creating new topic
EXTRA_CONFIG = {"retention.ms": f"{RETENTION_MS}"}
createTopic(BOOTSTRAP_SERVERS, TOPIC_NAME, TOPIC_PARTITION_NUM, REPLICATION_FACTOR, EXTRA_CONFIG)

# Get Topic Config
#getConfig(BOOTSTRAP_SERVERS, RESTYP, RESNAME)

# Delete topics
# deleteTopic(BOOTSTRAP_SERVERS, TOPIC_NAMES)j

# Sending messages to producer in sync mode
topic = get_config('topic', section='producer_sync')
for i in range(10):
    key = str(i).encode('utf-8')
    value = json.dumps({"val": i})
    produce_sync(BOOTSTRAP_SERVERS, key, value, topic)
