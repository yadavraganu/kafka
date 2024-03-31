from confluent_kafka import admin


def list_topics(BOOTSTRAP_SERVERS):
    conf = {'bootstrap.servers': f'{BOOTSTRAP_SERVERS}'}
    print(conf)
    try:
        admin_client = admin.AdminClient(conf)
        print("=" * 150)
        topic_list = admin_client.list_topics().topics
        topic_list = str(topic_list).split(", '")
        for i in topic_list:
            print(i.replace('{', '').replace("'", ''))
        print("=" * 150)
    except Exception as err:
        print(f'Error while getting list of topics - {err}')
