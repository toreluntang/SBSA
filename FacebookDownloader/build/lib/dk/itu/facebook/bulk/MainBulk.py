import facebook
import time
import requests
import argparse
import sys
from dateutil import parser as date_parser
from confluent_kafka import Producer


def check_int(val):
    try:
        ival = int(val)
    except ValueError:
        raise argparse.ArgumentParser("Value {} from input has to be an integer.".format(val))
    return ival


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--token', dest='fb_app_token', default='958677020829333|hgcyyndMsmCOChj6npV6Pg87yYE', help='The facebook API key, which can be fetched from https://developers.facebook.com/tools/explorer/145634995501895/')
    parser.add_argument('--page_id', dest='fb_page_id', help='The page id from facebook. From https://www.facebook.com/FoxNews/ the page id is FoxNews.')
    parser.add_argument('--server', dest='kafka_bootstrap_server', help='The kafka server')
    parser.add_argument('--topic', dest='kafka_topic', help='The topic the message should be produced as')
    parser.add_argument('--stop_date', dest='fb_stop_date', default='2016-01-01T00:00:00+0000', help='How far back should it download posts e.g. 2016-01-01T00:00:00+0000, which is default')
    parser.add_argument('--sleep', dest='sleep_time', default=10, help='Sleep between each request to facebook for the newest post', type=check_int)

    args = parser.parse_args()

    fb_app_token = args.fb_app_token # 'EAACEdEose0cBAEw9adSFuhHOh4IZAvr36qFZBtFzq1pP1yHdFfJxdEdF2t2ZCDdli0ZBx2yOJmQnyZCvjBRmB5VImKM3Un2ufl2ikLh8sWjIplRDGVfc7i65ZAvJr6JU5MikhL146n0ZBzp931ZAdk62ZBEph8d2EvHrDH2TeGhMdyGRWO6rQhfIdAQ9WuwoZBy28ZD'
    fb_page_id = args.fb_page_id # 'FoxNews'
    kafka_bootstrap_server = args.kafka_bootstrap_server # '10.26.50.252:9092'
    kafka_topic = args.kafka_topic # 'wiki-result'
    sleep_time = args.sleep_time # 120
    fb_stop_date = args.fb_stop_date # 2016-01-01T00:00:00+0000

    p = Producer({'bootstrap.servers': kafka_bootstrap_server})

    graph = facebook.GraphAPI(access_token=fb_app_token, version='2.7')

    post_and_reactions = graph.get_object(fb_page_id, fields='posts.limit(100){message,description,caption,name,reactions.limit(100),created_time}')
    posts = post_and_reactions['posts']

    print("Processes first batch of 100")
    for d in posts['data']:
        p.produce('wiki-result', str(d).encode('utf-8'))
    p.flush()

    while 1:
        print("Sleep for 60")
        time.sleep(sleep_time)

        print("Fetches next batch of posts")
        posts = requests.get(posts['paging']['next']).json()

        print("Processes next batch")
        for d in posts['data']:
            post_from = date_parser.parse(d['created_time'])

            #TODO verify when emojis/reactions was implemented
            emojis_from = date_parser.parse(fb_stop_date)

            if post_from < emojis_from:
                print("System exits because posts now are too old. Posts are from {} and stop has been set to {}".format(post_from, emojis_from))
                # exit with zero means successful termination
                sys.exit(0)

            p.produce('wiki-result', str(d).encode('utf-8'))
            # print(str(d))
        #Flush after each iteration?
        p.flush()