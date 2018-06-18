from multiprocessing.dummy import Pool, Queue, Value, Process
import RedditClient as rc
from DBClient import CommentProcess, ConnectionPool, SubmissionProcess
from servus.node import Node, wrap_in_process, IS_PRODUCTION
import logging, os, time

logger = logging.getLogger(__name__)

# Registering with the load distribution server: Servus
num_subreddits = int(os.getenv("NUM_SUBREDDITS"))
node = Node()
node.get_resources('subreddits', num_subreddits)

# functions send their progress metrics into the queue
metrics_queue = Queue()

# error queue
error_queue = Queue()

# node runs metric collection from queue, same queue is shared among threads
wrap_in_process(func=node.run_report_cycle, args=(metrics_queue,))

logger.info("PyWeaver started in " + ("production" if IS_PRODUCTION else "development") + " environment.")


def main():
    # collection of threads reaping posts and comments
    comment_processes = []
    submission_processes = []

    conn_manager = ConnectionPool()

    logger.info("Main routine started")

    # Injecting reddit instance into the module
    rc.__reddit__ = rc.create_agent()

    # juicy stuff, spawning a CommentProcessing instance for every sub
    logger.info('creating thread pool for scraping')

    for task in node.jobs[-1]['tasks']:
        cursor = next(conn_manager)
        print(task)

        # for each subreddit we want to start a comment process
        comment_process = CommentProcess(task, cursor, error_queue, metrics_queue)
        comment_process.start()
        comment_processes.append(comment_process)
        print("started comment harvesting")

        # and a submission scraping process
        cursor = next(conn_manager)
        submission_process = SubmissionProcess(task, cursor, error_queue, metrics_queue)
        submission_process.start()
        submission_processes.append(submission_process)
        print("started submission harvesting")

    # listening for an exception, clean up, repeat
    exception = error_queue.get()
    logger.info("received an exception" + str(exception))

    # most commonly reddit servers can't handle the load, so we just give them some time
    # (I am sure they are doing their best)
    time.sleep(10)

    # clean up, TODO: figure out how to kill threads or manage them
    conn_manager.on_exit()
    main()


if __name__ == '__main__':
    main()
