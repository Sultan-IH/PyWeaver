from psycopg2.pool import ThreadedConnectionPool
import psycopg2 as pg
import logging, os, atexit
import RedditClient as rc
from typing import Tuple
import multiprocessing.dummy as mp

logger = logging.getLogger(__name__)


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


def insert_submission(subname: str):
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


class CommentProcess(mp.Process):
    sql_statement = """
             INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, parent_id)
             SELECT %s, %s, %s, %s, %s, %s, %s
             WHERE NOT EXISTS(
                 SELECT * FROM reddit_replies WHERE id = %s
             )"""

    def __init__(self, task: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue):
        self.task = task
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info("streaming and inserting comments from " + self.task)
        for comment in rc.stream_subreddit_comments(self.task, self._error_queue):
            values = (comment.id, str(comment.author), comment.body, self.task,
                      comment.score, comment.created_utc, comment.parent_id, comment.id)
            try:
                self.cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(str(e))
            else:
                self._metrics_queue.put('comment')
                logger.info("Inserted comment from the " + self.task + " subreddit")


class SubmissionProcess(mp.Process):
    sql_statement = """
            INSERT INTO reddit_posts 
            (post_id, author, subreddit, title, ups, num_comments, CreatedUTC, text, permalink)  
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT * FROM reddit_posts WHERE post_id = %s
            )"""

    def __init__(self, task: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue):
        self.task = task
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info("streaming and inserting submissions from " + self.task)
        for submission in rc.stream_subreddit_submissions(self.task, self._error_queue):
            values = (submission.id, str(submission.author), self.task, submission.title,
                      submission.score, submission.num_comments, submission.created_utc, submission.selftext,
                      submission.permalink, submission.id)
            try:
                self.cursor.execute(self.sql_statement, values)
            except (pg.IntegrityError, pg.OperationalError) as e:
                logger.error("Error on executing sql: {0}".format(e))
                self._error_queue.put(str(e))
            else:
                self._metrics_queue.put('submission')
                logger.info("Inserted submission from the " + self.task + " subreddit")
