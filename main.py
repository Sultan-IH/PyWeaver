from multiprocessing.dummy import Pool, Queue, Value, Process
import RedditClient as rc
from DBClient import CommentProcess, ConnectionPool, insert_comment_dummy
from servus.node import Node, wrap_in_process, IS_PRODUCTION
import logging, os

logger = logging.getLogger(__name__)

# Registering with the load distribution server: Servus
num_subreddits = int(os.getenv("NUM_SUBREDDITS"))
node = Node()
node.get_resources('subreddits', num_subreddits)

# functions send their progress metrics into the queue
metrics_queue = Queue()

# error queue
error_queue = Queue()

# node runs metric collection from queue
wrap_in_process(func=node.run_report_cycle, args=(metrics_queue,))

logger.info("PyWeaver started in " + ("production" if IS_PRODUCTION else "development") + " environment.")


def main():
    # collection of threads reaping posts and comments
    processes = []

    conn_manager = ConnectionPool()

    logger.info("Main routine started")

    # Injecting reddit instance into the module
    rc.__reddit__ = rc.create_agent()

    # juicy stuff, spawning a CommentProcessing instance for every sub
    logger.info('creating thread pool for scraping')
    for task in node.jobs[-1]['tasks']:
        cursor = next(conn_manager)
        p = CommentProcess(task, cursor, error_queue, metrics_queue)
        p.start()
        processes.append(p)

    # listening for an exception, clean up, repeat
    exception = error_queue.get()
    logger.info("received an exception" + str(exception))
    # clean up, TODO: figure out how to kill threads
    conn_manager.on_exit()
    main()


if __name__ == '__main__':
    main()
