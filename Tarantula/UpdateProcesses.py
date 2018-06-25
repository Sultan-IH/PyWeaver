import logging
import multiprocessing.dummy as mp
import psycopg2 as pg

import RedditClient as rc
from InterruptableThread import InterruptableThread, StopWorkerException

logger = logging.getLogger(__name__)


class SubmissionUpdateProcess(InterruptableThread):
    sql_statement = f"""
                UPDATE reddit_replies
                SET ups = %s
                WHERE id = %s
            """

    def __init__(self, _id, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue, task_queue: mp.Queue):
        self._cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._task_queue = task_queue
        self._id = _id
        super().__init__(target=self.run, args=())

    def run(self):
        task = self._task_queue.get()
        logger.info(f"thread [{self._id}]: updating scores from submission with id [{task}]")
        logger.info(f"thread [{self._id}]: queue size after get() [{self._task_queue.qsize()}]")
        for comment in rc.feed_submission_comments(task, self._error_queue):
            values = (comment.score, comment.id)
            try:
                self._cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(e)
            else:
                self._metrics_queue.put('upvote')
                logger.info(f"Updated a [{comment.id}] comment to [{comment.score}] upvotes")

        logger.info(f"thread [{self._id}]: finished with task [{task}]")
        self._task_queue.task_done()
        if self._task_queue.unfinished_tasks == 0:
            raise StopWorkerException('Finished all work')
        else:
            self.run()
