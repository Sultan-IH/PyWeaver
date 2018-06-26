import logging
import multiprocessing.dummy as mp
import psycopg2 as pg

import RedditClient as rc
from InterruptableThread import InterruptableThread

logger = logging.getLogger(__name__)


class CommentStreamProcess(InterruptableThread):
    sql_statement = """
             INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, parent_id)
             SELECT %s, %s, %s, %s, %s, %s, %s
             WHERE NOT EXISTS(
                 SELECT * FROM reddit_replies WHERE id = %s
             )"""

    def __init__(self, task: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue, stop_event: mp.Event):
        self.task = task
        self._id = task
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._stop_event = stop_event
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info("streaming and inserting comments from " + self.task)
        for comment in rc.stream_subreddit_comments(self.task, self._error_queue):
            if self._stop_event.is_set() or comment == 'exception':
                logger.info(f"{self.task} comment harvesting thread exiting")
                # if there was an error in stream_subreddit_comments
                return  # check if an exception has occurred that caused all threads to stop

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
        self._id = task
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._stop_event = stop_event
        super().__init__(target=self.run, args=())

    def run(self):
        logger.info("streaming and inserting submissions from " + self.task)
        for submission in rc.stream_subreddit_submissions(self.task, self._error_queue):
            if self._stop_event.is_set() or submission == 'exception':
                logger.info(f"{self.task} submission harvesting thread exiting")
                # if there was an error in stream_subreddit_comments
                return  # check if an exception has occurred that caused all threads to stop
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
