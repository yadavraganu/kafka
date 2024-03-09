import faker
import sys
import uuid

sys.path.append("..")
from config.config_parser import get_config
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import KafkaException, Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroSerializer
from schema.avro_schema import user_schema

producer_conf = {'bootstrap.servers': get_config('BOOTSTRAP_SERVERS', section='producer_avro'),
                 'acks': get_config('ACKS', section='producer_avro'),
                 'compression.type': get_config('COMPRESSION', section='producer_avro')}


class Person(object):
    def __init__(self, name, email):
        self.name = name
        self.email = email


def person_to_dict(person, ctx):
    return dict(name=person.name, email=person.email)


def gen_data():
    fc = faker.Faker()
    data = Person(name=fc.name(), email=fc.email())
    return data


def callback(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


schema_registry_conf = {'url': get_config('SCHEMA_REG_URL', section='producer_avro')}
schema_client = SchemaRegistryClient(schema_registry_conf)

key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_client, user_schema, person_to_dict)

producer_client = Producer(producer_conf)


def main(ip_topic, ip_key, ip_data):
    i = 0
    while i <= 10:
        try:
            producer_client.produce(topic=ip_topic, key=key_serializer(ip_key),
                                    value=value_serializer(ip_data, SerializationContext(ip_topic, MessageField.VALUE)),
                                    callback=callback)
            i += 1
        except KafkaException as err:
            print(f'Error while producing - {err}')
        except KeyboardInterrupt as err:
            print(f'Stopped manually {err}')
        finally:
            producer_client.flush()


main('Test', str(uuid.uuid4()), gen_data())
