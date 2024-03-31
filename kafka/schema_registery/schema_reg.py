from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

def get_schr_client(CONF):
    sc = SchemaRegistryClient(CONF)
    return sc

def register_schema(client,subject,schema_str):
    try:
        print('Hio')
        schema = Schema(schema_str)
        print(schema)
        schema_id = client.register_schema(subject,schema)
        print(f"Schema registered with schema_id : {e}")  
    except Exception as e:
        print(f"Error while registering schema : {e}")    
    return schema_id

if __name__ == 'main':
    conf = {'url':'127.0.0.1:8081'}
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
    print(register_schema(sc,'Test.subject',user_schema))
    print(sc.get_subjects())