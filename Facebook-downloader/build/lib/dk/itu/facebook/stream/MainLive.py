import facebook
import time
import argparse
from confluent_kafka import Producer


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--token', dest='fb_app_token', default='958677020829333|hgcyyndMsmCOChj6npV6Pg87yYE', help='The facebook API key, which can be fetched from https://developers.facebook.com/tools/explorer/145634995501895/')
    parser.add_argument('--page_id', dest='fb_page_id', help='The page id from facebook. From https://www.facebook.com/FoxNews/ the page id is FoxNews.')
    parser.add_argument('--server', dest='kafka_bootstrap_server', help='The kafka server')
    parser.add_argument('--topic', dest='kafka_topic', help='The topic the message should be produced as')
    parser.add_argument('--sleep', dest='sleep_time', default=300, help='Sleep between each request to facebook for the newest post')

    args = parser.parse_args()

    fb_app_token = args.fb_app_token #'958677020829333|hgcyyndMsmCOChj6npV6Pg87yYE'
    fb_page_id = args.fb_page_id #FoxNews
    kafka_bootstrap_server = args.kafka_bootstrap_server #'10.26.50.252:9092'
    kafka_topic = args.kafka_topic  #'wiki-result'
    sleep_time = args.sleep_time #120


    p = Producer({'bootstrap.servers': kafka_bootstrap_server})

    graph = facebook.GraphAPI(access_token=fb_app_token, version='2.7')

    previous_id = 0

    while 1:
        print("Fetching newest post")
        post_and_reactions = graph.get_object(fb_page_id, fields='posts.limit(1){message,description,caption,name,reactions.limit(100)}')

        current_id = post_and_reactions['posts']['data'][0]['id']
        print("Newest post has id %s and previous post has id %s", current_id, previous_id)

        if current_id != previous_id:
            previous_id = current_id
            p.produce(kafka_topic, str(post_and_reactions['posts']['data']).encode('utf-8'))
            print("Message has been produced")

        print("Sleeping for %s seconds", sleep_time)
        time.sleep(sleep_time)