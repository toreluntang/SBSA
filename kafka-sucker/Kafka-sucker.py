from confluent_kafka import Consumer, KafkaError
import argparse
import json
import csv

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', dest='kafka_bootstrap_server', help='The kafka server')
    parser.add_argument('--topic', dest='kafka_topic', help='The topic the message should be consumed from ')
    parser.add_argument('--group', dest='kafka_group', help='The group the message should be consumed from')
    parser.add_argument('--id', dest='msg_id', help='The id of the messages wanted')

    args = parser.parse_args()

    kafka_bootstrap_server = args.kafka_bootstrap_server
    kafka_topic = args.kafka_topic
    kafka_group = args.kafka_group
    msg_id = args.msg_id

    print("Kafka server: {}".format(kafka_bootstrap_server))
    print("Kafka topic: {}".format(kafka_topic))
    print("Kafka group: {}".format(kafka_group))
    print("Message id: {}".format(msg_id))

    conf = {'bootstrap.servers': kafka_bootstrap_server,
            'group.id': kafka_group,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    c = Consumer(conf)
    c.subscribe([kafka_topic])


    csv_data = open('out.csv', 'w')
    csvwriter = csv.writer(csv_data)
    count = 0

    running = True
    while running:

        msg = c.poll()
        if not msg.error():
            json_msg = msg.value().decode('utf-8')
            json_msg = json.loads(json_msg)

            if json_msg['id'] == msg_id or msg_id == '':
                if count == 0:
                    header = json_msg.keys()
                    csvwriter.writerow(header)
                    count += 1

                csvwriter.writerow(json_msg.values())



        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False
    c.close()
