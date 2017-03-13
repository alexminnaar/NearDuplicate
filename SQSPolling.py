import logging
import sys
import boto3
import multiprocessing
from DuplicateClusterAssignment import DuplicateClusterAssignment

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def sqs_polling(queue_name, memcached_endpoint, process_id):
    # SQS client config
    sqs = boto3.resource('sqs', region_name='us-east-1')
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    dca = DuplicateClusterAssignment(es_index='images', distance_threshold=0.3, memcached_endpoint=memcached_endpoint)

    # poll sqs forever
    while 1:
        # receives up to 10 messages at a time
        for message in queue.receive_messages():
            logger.warning("Process %d: Read message: %s" % (process_id, message.body))

            image_url = message.body

            # cluster image and insert to memcached
            dca.insert_and_cluster(image_url)


def main():
    queue_name = sys.argv[1]
    memcached_endpoint = sys.argv[2]

    sqs_polling(queue_name, memcached_endpoint)

    p1 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 1,))
    p1.start()

    p2 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 2,))
    p2.start()

    p3 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 3,))
    p3.start()

    p4 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 4,))
    p4.start()

    p5 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 5,))
    p5.start()

    p6 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 6,))
    p6.start()

    p7 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 7,))
    p7.start()

    p8 = multiprocessing.Process(
        target=sqs_polling, args=(queue_name, memcached_endpoint, 8,))
    p8.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()


if __name__ == "__main__":
    main()
