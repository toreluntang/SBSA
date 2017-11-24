from confluent_kafka import Producer
import argparse
import json

def check_int(val):
    try:
        ival = int(val)
    except ValueError:
        raise argparse.ArgumentParser("Value {} from input has to be an integer.".format(val))
    return ival

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', dest='kafka_bootstrap_server', help='The kafka server')
    parser.add_argument('--topic', dest='kafka_topic', help='The topic the message should be produced as')
    parser.add_argument('--num_msg', dest='num_of_msg', help='The number of messages to send. Default=1000', default=1000, type=check_int)
    parser.add_argument('--msg_type', dest='msg_type', help='What kind of messages should be produced. Choose \"small\", \"large\" or \"mix\". Default is small.', default='small')

    args = parser.parse_args()

    kafka_bootstrap_server = args.kafka_bootstrap_server
    kafka_topic = args.kafka_topic
    num_of_msg = args.num_of_msg
    msg_type = args.msg_type

    print("Kafka server: {}".format(kafka_bootstrap_server))
    print("Kafka topic: {}".format(kafka_topic))
    print("Generating {} message(s)".format(num_of_msg))

    with open('msg_small_1kb.json', 'rb') as file:
        msg_small = json.load(file)

    with open('msg_large_2mb.json', 'rb') as file:
        msg_large = json.load(file)

    conf = {
        'bootstrap.servers': kafka_bootstrap_server,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 1000,
        'batch.num.messages': 1000,
        'log.connection.close': True,
        'client.id': "10.26.51.51",
        'default.topic.config': {'acks': '0'}
    }

    # p = Producer({'bootstrap.servers': kafka_bootstrap_server})
    p = Producer(conf)

    if msg_type == 'mix':
        num_of_msg /= 2

    for i in range(int(num_of_msg)):
        if ((i+1) % 1000) == 0:
            print("Message batch {} produced out of {}".format((i+1), num_of_msg))

        if msg_type == 'small':
            p.produce(kafka_topic, json.dumps(msg_small))
        elif msg_type == 'large':
            p.produce(kafka_topic, json.dumps(msg_large))
        elif msg_type == 'mix':
            p.produce(kafka_topic, json.dumps(msg_small))
            p.produce(kafka_topic, json.dumps(msg_large))

        # @Edenhill https://github.com/confluentinc/confluent-kafka-python/issues/16
        p.poll(0)

    #importante to flush
    p.flush()

    print("A total of {} message(s) was produced.".format(num_of_msg))
