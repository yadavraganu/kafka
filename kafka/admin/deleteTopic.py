from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

def deleteTopic(BOOTSTRAP_SERVERS,TOPICS):
    topics = TOPICS.split(',')
    print(topics)
    conf = {'bootstrap.servers': f'{BOOTSTRAP_SERVERS}'}
    try:
        admin = AdminClient(conf)
        tc = admin.delete_topics(topics,operation_timeout=10)
        for i, j in tc.items():
            try:
                j.result()
                print(f'Topic {tc.i} deleted')
            except KafkaException as e:
                print("Failed to delete topic {}: {}".format(i, e))
    except KafkaException as err:
        print(f'Error occurred while connecting broker - {err}')
