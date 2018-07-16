import logging
import os
from multiprocessing.dummy import Queue
import DBClient as db
import RedditClient as rc
from Tarantula.ControlProcess import UpdateControlProcess
from Tarantula.Report import Report
from env_config import PROGRAM_CONFIG, IS_PRODUCTION
from servus.Node import Node, wrap_in_process

logger = logging.getLogger(__name__)

node = Node(PROGRAM_CONFIG)
PAUSE_DURATION = 30 * 60 if IS_PRODUCTION else 3 * 60
NUM_THREADS = int(os.getenv("NUM_THREADS")) if os.getenv("NUM_THREADS") else 10
LOOKBACK = 1  # how many days to look back
MAX_CONNS = os.getenv("MAX_DB_CONNS") if os.getenv("MAX_DB_CONNS") else 15
metrics_queue = Queue()
report = Report(metrics_queue)
# enable reporting for the node
wrap_in_process(func=node.run_report_cycle, args=(report,))

logger.info("Tarantula started in " + ("production" if IS_PRODUCTION else "development") + " environment.")


def main():
    rc.__reddit__ = rc.create_agent()
    error_queue = Queue()
    control_process = UpdateControlProcess(
                                           error_queue,
                                           metrics_queue,
                                           PAUSE_DURATION,
                                           NUM_THREADS,
                                           LOOKBACK)
    control_process.start()
    exception = error_queue.get()
    node.report_error(exception)

    control_process.terminate()
    main()


if __name__ == '__main__':
    main()
