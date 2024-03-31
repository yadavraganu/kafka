from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema


def get_schr_client(conf):
    sc = SchemaRegistryClient(conf)
    return sc


def register_schema(client, subject, schema_str, schema_type):
    try:
        schema = Schema(schema_str, schema_type)
        schema_id = client.register_schema(subject, schema)
        print(f"Schema registered with schema_id : {schema_id}")
    except Exception as e:
        print(f"Error while registering schema : {e}")
    return schema_id


def get_subjects(client):
    sbj_lst = client.get_subjects()
    return sbj_lst


#if __name__ == 'main':
conf = {'url': 'http://localhost:8081'}
sc = get_schr_client(conf)
user_schema = """{
                        "type": "record",
                        "name": "User",
                        "fields": [
                                    {
                                        "name": "name",
                                        "type": "string"
                                    },
                                    {
                                        "name": "email",
                                        "type": "string"
                                    }
                                ]
                        }"""
print(register_schema(sc, 'Test.subject', user_schema, 'AVRO'))
print(get_subjects(sc))
