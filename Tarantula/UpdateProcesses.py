import logging
import psycopg2 as pg
from multiprocessing.dummy import Queue, Event

import RedditClient as rc
from InterruptableThread import InterruptableThread

logger = logging.getLogger(__name__)


class SubmissionUpdateProcess(InterruptableThread):
    sql_statement = f"""
                UPDATE reddit_replies
                SET ups = %s
                WHERE id = %s
            """

    def __init__(self, _id, cursor, error_queue: Queue, metrics_queue: Queue, task_queue: Queue,
                 stop_work_event: Event):
        self._cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._task_queue = task_queue
        self._stop_work_event = stop_work_event
        self._id = _id
        super().__init__()

    def run(self):
        task = self._task_queue.get()
        logger.info(f"thread [{self._id}]: updating scores from submission with id [{task}]")
        for comment in rc.feed_submission_comments(task, self._error_queue, self._stop_work_event.is_set()):
            if self._stop_work_event.is_set() or comment == 'stop':
                logger.info(f"comment harvesting thread [{self._id}] exiting")
                # check if an exception has occurred that caused all threads to stop
                # or if there was an error in stream_subreddit_comments
                return
            values = (comment.score, comment.id)
            try:
                self._cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(e)
            else:
                self._metrics_queue.put('score')
                logger.info(f"thread [{self._id}]: updated a [{comment.id}] comment to [{comment.score}] upvotes")

        logger.info(f"thread [{self._id}]: finished with task [{task}]")
        logger.info(f"thread [{self._id}]: tasks left: {self._task_queue.qsize()}")

        if self._task_queue.empty():
            logger.info(f"thread [{self._id}]: finished all work!!! returning")
            return
        else:
            self.run()
