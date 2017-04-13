import logging
import sys
import boto3
import multiprocessing
from DuplicateClusterAssignment import DuplicateClusterAssignment
from time import sleep
from kafka import KafkaConsumer

logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)


def kafka_polling(kafka_topic, kafka_group_id, kafka_host, es_endpoint, memcache_endpoint, process_id):
    logger.warning(
        "Process %d: Beginning to poll Kafka. Topic: %s, groupid: %s, host: %s" % (
            process_id, kafka_topic, kafka_group_id, kafka_host))

    dca = DuplicateClusterAssignment(elasticsearch_endpoint=es_endpoint, es_index='images_0.1', distance_threshold=0.1,
                                     memcached_endpoint=memcache_endpoint)

    # Kafka client config
    consumer = KafkaConsumer(kafka_topic, group_id=kafka_group_id, bootstrap_servers=[kafka_host])

    for message in consumer:

        image_url = message.value.decode('utf-8')

        logger.warning('Process %d: Received image url %s' % (process_id, image_url))

        # cluster image
        try:
            dca.insert_and_cluster(image_url)
        except Exception:
            logger.error("Process %d: Failed to write to memcached" % process_id, exc_info=True)

        finally:
            # TODO: not deleting message for testing purposes. Change this when finished testing
            # message.delete()
            logger.warning("Process %d: message processed" % process_id)


def main():
    kafka_topic = sys.argv[1]
    kafka_group_id = sys.argv[2]
    kafka_host = sys.argv[3]
    es_endpoint = sys.argv[4]
    memcache_endpoint = sys.argv[4]

    # keep track of processes to restart if needed. PID => Process
    processes = {}

    # TODO: only launching one process for testing purposes.  Change this when finished testing
    num_processes = range(1, 2)

    for p_num in num_processes:
        p = multiprocessing.Process(
            target=kafka_polling,
            args=(kafka_topic, kafka_group_id, kafka_host, es_endpoint, memcache_endpoint, p_num,))
        p.start()
        processes[p_num] = p

    # periodically poll child processes to check if they are still alive
    while len(processes) > 0:

        # check every 5 minutes
        sleep(300.0)

        for n in processes.keys():
            p = processes[n]

            # if process is dead, create a new one to take its place
            if not p.is_alive():
                logger.error('Process %d is dead! Starting new process to take its place.' % n)
                replacement_p = multiprocessing.Process(target=kafka_polling,
                                                        args=(kafka_topic, kafka_group_id, kafka_host, es_endpoint,
                                                              memcache_endpoint, n,))
                replacement_p.start()
                processes[n] = replacement_p

            elif p.is_alive():
                logger.warning('Process %d is still alive' % n)

            # since polling never ends, sqs_polling should never successfully exit but we add this for completeness
            elif p.exitcode == 0:
                p.join()


if __name__ == "__main__":
    main()
