import logging
import multiprocessing.dummy as mp
import os
from psycopg2.pool import ThreadedConnectionPool

import RedditClient as rc


logger = logging.getLogger(__name__)

__conn_manager__ = None


def create_conn_manager():
    global __conn_manager__
    __conn_manager__ = ConnectionPool()


class ConnectionPool:
    def __init__(self, max_conns: int = 40):
        conn_params = (
            f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')}"
            f" password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} ")
        self.pool = ThreadedConnectionPool(1, max_conns, conn_params)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            conn = self.pool.getconn()
            conn.autocommit = True

            return conn.cursor()

        except Exception as e:
            logger.error(e)
            raise StopIteration

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



