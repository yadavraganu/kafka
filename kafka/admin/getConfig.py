from confluent_kafka import admin


def getConfig(BOOTSTRAP_SERVERS, RESTYPE, RESNAME):
    try:
        conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
        # List of resources to get config for
        lst = [admin.ConfigResource(int(RESTYPE), RESNAME)]
        admin_client = admin.AdminClient(conf)
        print("=" * 150)
        desc = admin_client.describe_configs(lst)
        for i in desc:
            confs = str(desc[i].result()).replace('{', '').replace('}', '').split(', ')
            print(f"Resource Name : {RESNAME}")
            for j in confs:
                print(j)
        print("=" * 150)
    except Exception as err:
        print(f'Error while getting list of configs - {err}')
