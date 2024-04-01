from confluent_kafka import Producer, KafkaException


def callback(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return False
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def produce_sync(bootstrap_servers, key, message, topic):
    kafka_conf = {'bootstrap.servers': bootstrap_servers, 'acks': 'all', 'compression.type': 'gzip'}
    try:
        producer = Producer(kafka_conf)
        future = producer.produce(topic=topic, key=key, value=message, on_delivery=callback)
        try:
            producer.flush()
        except Exception as e:
            print(f'Error while sending message.Error is : {e}')
    except KafkaException as err:
        print(f'Error occurred while connecting broker - {err}')
