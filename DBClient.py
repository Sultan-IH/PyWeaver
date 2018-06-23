import ctypes
import inspect
import logging
import multiprocessing.dummy as mp
import os
import psycopg2 as pg
import threading
from psycopg2.pool import ThreadedConnectionPool

import RedditClient as rc

logger = logging.getLogger(__name__)


def _async_raise(tid, exctype):
    """
    Raises an exception in the threads with id tid
    """
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid),
                                                     ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # "if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class InterruptableThread(mp.Process):

    def _get_my_tid(self):
        """
        determines this (self's) thread id

        CAREFUL : this function is executed in the context of the caller
        thread, to get the identity of the thread represented by this
        instance.
        """
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                print(tid)
                return tid

        raise AssertionError("could not determine the thread's id")

    def interrupt(self, exctype):
        """Raises the given exception type in the context of this thread.

        If the thread is busy in a system call (time.sleep(),
        socket.accept(), ...), the exception is simply ignored.

        If you are sure that your exception should terminate the thread,
        one way to ensure that it works is:

            t = ThreadWithExc( ... )
            ...
            t.raiseExc( SomeException )
            while t.isAlive():
                time.sleep( 0.1 )
                t.raiseExc( SomeException )

        If the exception is to be caught by the thread, you need a way to
        check that your thread has caught it.

        CAREFUL : this function is executed in the context of the
        caller thread, to raise an excpetion in the context of the
        thread represented by this instance.
        """
        _async_raise(self._get_my_tid(), exctype)


class ConnectionPool:
    def __init__(self):
        conn_params = (
            f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')}"
            f" password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} ")
        self.pool = ThreadedConnectionPool(1, 40, conn_params)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            conn = self.pool.getconn()
            conn.autocommit = True
            print("delt a connection")

            return conn.cursor()

        except Exception as e:
            logger.error(e)
            raise StopIteration

    def dummy(self):
        print("dummy")

    def on_exit(self):
        self.pool.closeall()
        logger.info("closed all pool connections")


def insert_submission_dummy(subname: str):
    """
    Testing for now
    :return:
    """

    for submission in rc.stream_subreddit_submissions(subname):
        print("[%s] post" % submission.title)


def insert_comment_dummy(args):
    """
    Testing for now
    :return:
    """
    subname = args[0]
    for comment in rc.stream_subreddit_comments(subname):
        print("[%s] comment in [%s]" % (comment.id, subname))


class CommentStreamProcess(InterruptableThread):
    sql_statement = """
             INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, parent_id)
             SELECT %s, %s, %s, %s, %s, %s, %s
             WHERE NOT EXISTS(
                 SELECT * FROM reddit_replies WHERE id = %s
             )"""

    def __init__(self, task: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue, stop_event: mp.Event):
        self.task = task
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._stop_event = stop_event
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info("streaming and inserting comments from " + self.task)
        for comment in rc.stream_subreddit_comments(self.task, self._error_queue):
            if self._stop_event.is_set():
                logger.info(f"{self.task} comment harvesting thread exiting")
                break  # check if an exception has occurred that caused all threads to stop

            values = (comment.id, str(comment.author), comment.body, self.task,
                      comment.score, comment.created_utc, comment.parent_id, comment.id)
            try:
                self.cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(e)
            else:
                self._metrics_queue.put('comment')
                logger.info(f"Inserted [{comment.id}] comment from [{self.task}]")


class SubmissionStreamProcess(InterruptableThread):
    sql_statement = """
            INSERT INTO reddit_posts 
            (post_id, author, subreddit, title, ups, num_comments, CreatedUTC, text, permalink)  
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT * FROM reddit_posts WHERE post_id = %s
            )"""

    def __init__(self, task: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue, stop_event: mp.Event):
        self.task = task
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._stop_event = stop_event
        # self.daemon = True needs more clarification
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info("streaming and inserting submissions from " + self.task)
        for submission in rc.stream_subreddit_submissions(self.task, self._error_queue):
            if self._stop_event.is_set():  # TODO delete this mechanism
                logger.info(f"{self.task} submission harvesting thread exiting")
                break  # check if an exception has occurred that caused all threads to stop

            values = (submission.id, str(submission.author), self.task, submission.title,
                      submission.score, submission.num_comments, submission.created_utc, submission.selftext,
                      submission.permalink, submission.id)
            try:
                self.cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(e)
            else:
                self._metrics_queue.put('submission')
                logger.info(f"Inserted [{submission.id}] submission from [{self.task}]")

        return  # triggered if stop_event is set


def query_submissions(cursor, date: int):
    sql_query = f"""
            SELECT post_id FROM reddit_posts WHERE createdutc > {date}
        """
    cursor.execute(sql_query)
    return cursor.fetchall()


class SubmissionUpdateProcess(mp.Process):
    sql_statement = f"""
                UPDATE reddit_replies
                SET ups = %s
                WHERE id = %s
            """

    def __init__(self, _id: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue):
        self.post_id = _id
        self._cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info(f"updating scores from submission with id [{self.post_id}]")
        for comment in rc.feed_submission_comments(self.post_id, self._error_queue):
            values = (comment.score, comment.id)
            try:
                self._cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(e)
            else:
                self._metrics_queue.put('upvote')
                logger.info(f"Updated a [{comment.id}] comment to [{comment.score}] upvotes")


class StopWorkerException(Exception):
    pass