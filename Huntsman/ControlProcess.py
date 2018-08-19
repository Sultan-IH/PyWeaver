import time

import logging
import os
import requests
from multiprocessing.dummy import Queue, Event

import DBClient as db
from Huntsman.InsertProcess import SubmissionInsertProcess
from InterruptableThread import InterruptableThread, StopWorkerException, kill_thread_pool

logger = logging.getLogger(__name__)

MAX_CONNS = os.getenv("MAX_DB_CONNS") if os.getenv("MAX_DB_CONNS") else 15
BASE_URL = 'https://api.pushshift.io/reddit/search/submission/?'


class InsertControlProcess(InterruptableThread):
    """
    Every PAUSE_PERIOD spawns NUM_WORKERS to update scores on comments from the previous day
    """
    start_ts, end_ts = 1498867200, 1530403200

    def __init__(self,
                 task: str,
                 status_queue: Queue,
                 metrics_queue: Queue,
                 num_workers: int = 12
                 ):

        self._status_queue = status_queue
        self._metrics_queue = metrics_queue
        self._stop_work_event = Event()
        self.num_workers = num_workers
        self.workers = []
        self.task = task
        self.batch_num = 0
        super().__init__()

    def run(self):
        task_queue = Queue()
        self._conn_manager = db.ConnectionPool(max_conns=MAX_CONNS)
        self.workers = []

        # somehow get the post ids
        ts = reap_submissions(self.start_ts, self.end_ts, self.task, task_queue)
        if ts == -1:
            logger.info("all done. starting clean up")
            self._conn_manager.on_exit()
            kill_thread_pool(self._stop_work_event, self.workers)
            self._metrics_queue.put(self.task)
            self._status_queue.put('all clear')
            return
        else:
            self.batch_num += 1
            self.start_ts = ts

        logger.info(f"got a workload of {task_queue.qsize()} tasks; working on batch num [{self.batch_num}]")

        for i in range(self.num_workers):
            _cursor = next(self._conn_manager)
            worker = SubmissionInsertProcess(i, self.task, _cursor, self._status_queue, self._metrics_queue, task_queue,
                                             self._stop_work_event)
            worker.start()
            self.workers.append(worker)
        # wait until all done
        while True:
            alive_threads = [thread.is_alive() for thread in self.workers]
            if not any alive_threads: break
            logger.info(f"{[sum(alive_threads)] threads are alive}")
            time.sleep(20)
        self._conn_manager.on_exit()
        self.run()

    def terminate(self, extype=StopWorkerException):
        kill_thread_pool(self._stop_work_event, self.workers)
        self._conn_manager.on_exit()
        logger.info("InsertControlProcess terminated")
        self.interrupt(extype)


def reap_submissions(start, end, subreddit, task_queue: Queue, size=250):
    search_string = f"after={start}&before={end}&subreddit={subreddit}&size={size}"
    res = requests.get(BASE_URL + search_string)
    data = res.json()['data']
    if len(data) == 0:
        return -1
    latest = int(data[-1]['created_utc'])

    for item in data:
        task_queue.put(item['id'])
    logger.info(f"reaped a [{len(data)}] submission batch from [{subreddit}].")

    return latest  # + 1 so we don't reap the same comment
