import argparse
import json
from confluent_kafka import Consumer, KafkaError
from cloudant.client import Cloudant


def init_cloudant_client(account, database_name, password):
    """

    :param account:
    :param database_name:
    :param password:
    :return: Cloudant client and database
    """
    client = Cloudant(account, password, account=account, connect=True)

    try:
        db = client[database_name]
    except KeyError as ke:
        print("Database not found [{}]. Creating database".format(ke))
        db = client.create_database(database_name)

    return client, db


def init_kafka_consumer(server, topic, group_id='group', offset='latest'):
    """
    :param server: a string. Server ip and port e.g. 192.168.1.1:9092
    :param topic: an array. What queue to listen to
    :param group_id: Consumer group
    :param offset: Partition offset
    :return:
    """
    print("Connection to Kafka consumer. Server={}, group.id={}, offset={}".format(server, group_id, offset))
    c = Consumer({'bootstrap.servers': server, 'group.id': group_id,
                  'default.topic.config': {'auto.offset.reset': offset}})

    print("Consumer subscribing to topic={}".format(topic))
    c.subscribe(topic)
    return c

def kafka_to_cloudant(kafka_client, cloudant_client, batch_size = 100):
    """

    :param kafka_client:
    :param cloudant_client:
    :param batch_size:
    :return:
    """
    print("Starting to poll. Batch size is set to {}".format(batch_size))

    running = True
    while running:

        msg_batch = []
        for i in range(batch_size):
            msg = kafka_client.poll()
            try:
                msg_batch.append(json.loads(msg.value().decode('utf-8')))
            except Exception as e:
                print("couldn't read message properly - skipped. {}".format(e))

        if not msg.error():
            result = cloudant_client[1].bulk_docs(msg_batch)
            #ignores result?
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', dest='kafka_server', help='Kafka server e.g. 192.168.1.1:9092')
    parser.add_argument('--topic', dest='kafka_topic', help='Kafka topic e.g. test_queue')
    parser.add_argument('--group_id', dest='kafka_group_id', help='Consumer group id', default='default_group')
    parser.add_argument('--offset', dest='kafka_offset',
                        help='Offset. smallest. or google other. maybe earliest or latest.', default='smallest')

    parser.add_argument('--cloudant_acc', dest='cloudant_account', help='Account name to Cloudant')
    parser.add_argument('--cloudant_db', dest='cloudant_database', help='Cloudant database name')
    parser.add_argument('--cloudant_pass', dest='cloudant_password', help='Password to Cloudant')

    args = parser.parse_args()

    print("Initialising kafka consumer client. Server[{}], Topic[{}], Group_id[{}], Offset[{}]".format(args.kafka_server, args.kafka_topic, args.kafka_group_id, args.kafka_offset))
    kc = init_kafka_consumer(args.kafka_server, [args.kafka_topic], args.kafka_group_id, args.kafka_offset)
    print("Kafka consumer client has been initialised")

    print("Initialising cloudant client. Account[{}], database[{}], password[********]".format(args.cloudant_account, args.cloudant_database, args.cloudant_password))
    cc = init_cloudant_client(args.cloudant_account, args.cloudant_database, args.cloudant_password)
    print("Cloudant client has been initialised")

    print("Streaming to cloudant")
    kafka_to_cloudant(kc, cc)

    # Close kafka client and Cloudant client
    kc.close()
    cc.close()