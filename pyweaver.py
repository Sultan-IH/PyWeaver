import logging, os, time, sys
import RedditClient as rc
import log_config  # when imported all the logging variables needed for other modules become available

from multiprocessing.dummy import Queue, Event
from DBClient import SubmissionStreamProcess, ConnectionPool, CommentStreamProcess
from servus.node import Node, wrap_in_process, IS_PRODUCTION

PROGRAM_CONFIG = log_config.PROGRAM_CONFIG

if IS_PRODUCTION:
    STDOUT_FILE = log_config.LOG_BASE + '.stdout'
    sys.stdout = open(STDOUT_FILE, 'wt')

logger = logging.getLogger(__name__)

# Registering with the load distribution server: Servus
num_subreddits = int(os.getenv("NUM_SUBREDDITS"))
node = Node(PROGRAM_CONFIG)
node.get_resources('subreddits', num_subreddits)

# functions send their progress metrics into the queue
metrics_queue = Queue()

# error queue, sends
error_queue = Queue()

# node runs metric collection from queue, same queue is shared among threads
wrap_in_process(func=node.run_report_cycle, args=(metrics_queue,))

logger.info("PyWeaver started in " + ("production" if IS_PRODUCTION else "development") + " environment.")

stop_work_event = Event()


def main():
    # collection of threads reaping posts and comments
    _processes = []

    conn_manager = ConnectionPool()

    logger.info("Main routine started")

    # Injecting reddit instance into the module
    rc.__reddit__ = rc.create_agent()

    # juicy stuff, spawning a CommentProcessing instance for every sub
    logger.info('creating thread pool for scraping')

    for task in node.jobs[-1]['tasks']:
        # for each subreddit we want to start a comment scraping process and a submission scraping process
        for Process in [CommentStreamProcess, SubmissionStreamProcess]:
            cursor = next(conn_manager)
            _process = Process(task, cursor, error_queue, metrics_queue, stop_work_event)
            _process.start()
            _processes.append(_process)

    # listening for an exception, clean up, repeat
    exception = error_queue.get()  # blocks thread until exception received
    logger.info("received an exception " + str(exception))
    node.report_error(exception)

    # clean up,
    stop_work_event.set()  # causes all threads to exit

    for process in _processes:
        logger.info(f"terminated [{process.task}] thread")
        process.join()

    # after we exit from all the threads we reset the event to no
    stop_work_event.clear()
    
    # clear all pool connections
    conn_manager.on_exit()

    # most commonly reddit servers can't handle the load, so we just give them some time
    # (I am sure they are doing their best)
    time.sleep(20)
    main()


if __name__ == '__main__':
    main()
