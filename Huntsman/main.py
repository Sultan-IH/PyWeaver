import logging
import os
from multiprocessing.dummy import Queue

import RedditClient as rc
from Huntsman.ControlProcess import InsertControlProcess
from Huntsman.Report import Report
from env_config import PROGRAM_CONFIG, IS_PRODUCTION
from servus.Node import Node

logger = logging.getLogger(__name__)

start, end = 1498867200, 1530403200
subreddits = ['btc', 'BlockChain', 'NEO', 'altcoin', 'CryptoMarkets', 'ethtrader']

rc.__reddit__ = rc.create_agent()

node = Node(PROGRAM_CONFIG)
NUM_THREADS = int(os.getenv("NUM_THREADS")) if os.getenv("NUM_THREADS") else 12
MAX_CONNS = os.getenv("MAX_DB_CONNS") if os.getenv("MAX_DB_CONNS") else 20
metrics_queue = Queue()
report = Report(metrics_queue)

# enable reporting for the node


logger.info("Huntsman started in " + ("production" if IS_PRODUCTION else "development") + " environment.")


def main():
    rc.__reddit__ = rc.create_agent()
    status_queue = Queue()
    for sub_name in subreddits:
        control_process = InsertControlProcess(sub_name,
                                               status_queue,
                                               metrics_queue,
                                               NUM_THREADS
                                               )
        control_process.start()
        status = status_queue.get()

        if status != 'all clear':
            node.report_error(status)
            main()  # restart
        else:
            html = report.make_html()
            node._report_metrics(html)


if __name__ == '__main__':
    main()
