import RedditClient as rc, logging, time
from servus.node import Node, wrap_in_process
from DBClient import ConnectionPool, query_submissions, SubmissionUpdateProcess
from multiprocessing.dummy import Queue

logger = logging.getLogger(__name__)

conn_manager = ConnectionPool()
node = Node('Tarantula')
PAUSE_DURATION = 30 * 60  # in seconds

rc.__reddit__ = rc.create_agent()

# shared channels of communication between threads
error_queue = Queue()

metrics_queue = Queue()

# enable reporting for the node
wrap_in_process(func=node.run_report_cycle, args=(metrics_queue, ))


# Every 30 minutes query the db for past day

def main():
    threads = []
    date = 1111
    cursor = next(conn_manager)
    submission_ids = query_submissions(cursor, date)

    for submission_id in submission_ids:
        cursor = next(conn_manager)
        t = SubmissionUpdateProcess(submission_id, cursor, error_queue, metrics_queue)
        t.start()
        threads.append(t)

    # listening for an exception, clean up, repeat
    exception = error_queue.get()  # blocks thread until exception received
    logger.info("received an exception" + str(exception))
    node.report_error(exception)
    # most commonly reddit servers can't handle the load, so we just give them some time
    # (I am sure they are doing their best)
    time.sleep(10)

    # clean up, TODO: figure out how to kill threads or manage them
    conn_manager.on_exit()
    main()
