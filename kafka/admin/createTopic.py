from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

def createTopic(BOOTSTRAP_SERVERS,TOPIC_NAME,TOPIC_PARTITION_NUM,REPLICATION_FACTOR,EXTRA_CONFIG):
    try:
        conf = {'bootstrap.servers': f'{BOOTSTRAP_SERVERS}'}
        topic = NewTopic(topic=f'{TOPIC_NAME}', num_partitions=int(f'{TOPIC_PARTITION_NUM}'),
                    replication_factor=int(f'{REPLICATION_FACTOR}'), config=EXTRA_CONFIG)
        admin = AdminClient(conf)
        tc = admin.create_topics([topic])
        for i, j in tc.items():
            try:
                j.result()
                print(f'Topic {i} Created')
            except Exception as e:
                print("Failed to create topic {}: {}".format(i, e))
    except KafkaException as err:
        print(f'Error occurred while connecting broker - {err}')
