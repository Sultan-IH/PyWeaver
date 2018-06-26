import logging
from multiprocessing.dummy import Queue

import DBClient as db
import RedditClient as rc
from Tarantula.ControlProcess import UpdateControlProcess
from env_config import PROGRAM_CONFIG, IS_PRODUCTION
from servus.node import Node, wrap_in_process

logger = logging.getLogger(__name__)

node = Node(PROGRAM_CONFIG)

PAUSE_DURATION = 30 * 60 if IS_PRODUCTION else 3 * 60
NUM_THREADS = 20 if IS_PRODUCTION else 10
LOOKBACK = 1  # how many days to look back

metrics_queue = Queue()

# enable reporting for the node
wrap_in_process(func=node.run_report_cycle, args=(metrics_queue,))

logger.info("Tarantula started in " + ("production" if IS_PRODUCTION else "development") + " environment.")


def main():
    rc.__reddit__ = rc.create_agent()
    conn_manager = db.ConnectionPool()
    error_queue = Queue()
    control_process = UpdateControlProcess(conn_manager,
                                           error_queue,
                                           metrics_queue,
                                           PAUSE_DURATION,
                                           NUM_THREADS,
                                           LOOKBACK)
    control_process.start()

    exception = error_queue.get()
    node.report_error(exception)

    control_process.terminate()
    db.__conn_manager__.on_exit()
    main()


if __name__ == '__main__':
    main()
