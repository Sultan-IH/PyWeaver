import time

import logging
from datetime import datetime, timezone, timedelta
from multiprocessing.dummy import Queue, Event

from DBClient import query_submissions, ConnectionPool
from InterruptableThread import InterruptableThread, kill_thread_pool, StopWorkerException
from Tarantula.UpdateProcesses import SubmissionUpdateProcess

logger = logging.getLogger(__name__)


class UpdateControlProcess(InterruptableThread):
    """
    Every PAUSE_PERIOD spawns NUM_WORKERS to update scores on comments from the previous day
    """

    def __init__(self,
                 conn_manager: ConnectionPool,
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
        self._conn_manager = conn_manager
        self.num_workers = num_workers
        self.workers = []

        lookback = datetime.now(tz=timezone.utc) - timedelta(days=self._daydelta)
        self._wall_clock = int(lookback.timestamp())  # how recent are we updating those posts

        super().__init__(target=self.run, args=())

    def run(self):
        task_queue = Queue()
        _cursor = next(self._conn_manager)
        query_submissions(_cursor, self._wall_clock, task_queue)
        _cursor.close()
        logger.info(f"got a workload of {task_queue.qsize()} tasks")

        for i in range(self.num_workers):
            _cursor = next(self._conn_manager)
            worker = SubmissionUpdateProcess(i, _cursor, self._error_queue, self._metrics_queue, task_queue,
                                             self._stop_work_event)
            worker.start()
            self.workers.append(worker)

        time.sleep(self._pause_period)
        logger.info("starting clean up")
        self._conn_manager.on_exit()
        # check that all threads completed and that the task queue is empty
        kill_thread_pool(self._stop_work_event, self.workers)

        if task_queue.unfinished_tasks != 0:
            logger.warning("not all tasks finished consider using more threads")

        # check if wall clock needs moving
        lookback = datetime.now(tz=timezone.utc) - timedelta(days=self._daydelta + 1)
        if lookback.timestamp() >= self._wall_clock:
            logger.info(f'changed wallclock to {lookback.timestamp()}')
            lookback = datetime.now(tz=timezone.utc) - timedelta(days=self._daydelta)
            self._wall_clock = int(lookback.timestamp())

        self.run()

    def terminate(self, extype=StopWorkerException):
        kill_thread_pool(self._stop_work_event, self.workers)
        logger.info("UpdateControlProcess terminated")
        self.interrupt(extype)
