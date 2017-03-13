import logging
import sys
import boto3

from DuplicateClusterAssignment import DuplicateClusterAssignment


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def sqs_polling(queue_name, memcache_endpoint):
    # SQS client config
    sqs = boto3.resource('sqs', region_name='us-east-1')
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    dca = DuplicateClusterAssignment(es_index='images', distance_threshold=0.3, memcached_endpoint='localhost')

    # poll sqs forever
    while 1:
        # receives up to 10 messages at a time
        for message in queue.receive_messages():
            logger.warning("Read message: %s" % message.body)

            image_url = message.body

            # cluster image and insert to memcached
            dca.insert_and_cluster(image_url)


def main():

    queue_name=sys.argv[1]
    memcached_endpoint=sys.argv[2]

    sqs_polling(queue_name, memcached_endpoint)


if __name__ == "__main__":
    main()
