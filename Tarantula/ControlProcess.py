import time

import logging
import os
from datetime import datetime, timezone, timedelta
from multiprocessing.dummy import Queue, Event
from typing import List

import DBClient as db
from InterruptableThread import InterruptableThread, StopWorkerException, kill_thread_pool
from Tarantula.UpdateProcesses import SubmissionUpdateProcess

logger = logging.getLogger(__name__)

MAX_CONNS = os.getenv("MAX_DB_CONNS") if os.getenv("MAX_DB_CONNS") else 15


class UpdateControlProcess(InterruptableThread):
    """
    Every PAUSE_PERIOD spawns NUM_WORKERS to update scores on comments from the previous day
    """

    def __init__(self,
                 error_queue: Queue,
                 metrics_queue: Queue,
                 pause_period: int = 30 * 60,
                 num_workers: int = 20,
                 daydelta: int = 1):

        self._pause_period = pause_period
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._daydelta = daydelta
        self._stop_work_event = Event()
        self.num_workers = num_workers
        self.workers = []

        lookback = datetime.now(tz=timezone.utc) - timedelta(days=self._daydelta)
        self._wall_clock = int(lookback.timestamp())  # how recent are we updating those posts

        super().__init__()

    def run(self):
        task_queue = Queue()
        self._conn_manager = db.ConnectionPool(max_conns=MAX_CONNS)

        _cursor = next(self._conn_manager)
        db.query_submissions(_cursor, self._wall_clock, task_queue)
        _cursor.close()
        logger.info(f"got a workload of {task_queue.qsize()} tasks")

        for i in range(self.num_workers):
            _cursor = next(self._conn_manager)
            worker = SubmissionUpdateProcess(i, _cursor, self._error_queue, self._metrics_queue, task_queue,
                                             self._stop_work_event)
            worker.start()

        time.sleep(self._pause_period)
        logger.info("starting clean up")
        self._conn_manager.on_exit()
        # check that all threads completed and that the task queue is empty
        kill_thread_pool(self._stop_work_event, self.workers)
        if not task_queue.empty():
            logger.warning("not all tasks finished consider using more threads")

        # check if wall clock needs moving
        lookback = datetime.now(tz=timezone.utc) - timedelta(days=self._daydelta + 1)
        if lookback.timestamp() >= self._wall_clock:
            logger.info(f'changed wallclock to {lookback.timestamp()}')
            lookback = datetime.now(tz=timezone.utc) - timedelta(days=self._daydelta)
            self._wall_clock = int(lookback.timestamp())

        self._metrics_queue.put("cycle")
        self.run()

    def terminate(self, extype=StopWorkerException):
        kill_thread_pool(self.workers)
        self._conn_manager.on_exit()
        logger.info("UpdateControlProcess terminated")
        self.interrupt(extype)


def kill_gthreads(pool: List[InterruptableThread], exctype=StopWorkerException):
    for thread in pool:
        thread.interrupt(exctype)
