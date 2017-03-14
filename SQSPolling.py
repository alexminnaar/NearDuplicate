import logging
import sys
import boto3
import multiprocessing
from DuplicateClusterAssignment import DuplicateClusterAssignment
from time import sleep

LOG_FILENAME = "sqs_polling.log"
logging.basicConfig(filename=LOG_FILENAME, level=logging.WARNING)
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

            try:
                # cluster image and insert to memcached
                dca.insert_and_cluster(image_url)
            except Exception:
                logger.error("Process %d: Failed to write to memcached" % process_id, exc_info=True)

            message.delete()


def main():
    queue_name = sys.argv[1]
    memcached_endpoint = sys.argv[2]

    # keep track of processes to restart if needed. PID => Process
    processes = {}

    num_processes = range(1, 9)

    for p_num in num_processes:
        p = multiprocessing.Process(
            target=sqs_polling, args=(queue_name, memcached_endpoint, p_num,))
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
                replacement_p = multiprocessing.Process(target=sqs_polling,
                                                        args=(queue_name, memcached_endpoint, n,))
                replacement_p.start()
                processes[n] = replacement_p

            elif p.is_alive():
                logger.warning('Process %d is still alive' % n)

            # since polling never ends, sqs_polling should never successfully exit but we add this for completeness
            elif p.exitcode == 0:
                p.join()


if __name__ == "__main__":
    main()
