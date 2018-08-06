import logging
import multiprocessing.dummy as mp

import RedditClient as rc
from InterruptableThread import InterruptableThread

logger = logging.getLogger(__name__)


class SubmissionInsertProcess(InterruptableThread):
    submission_statement = """
            INSERT INTO reddit_posts 
            (post_id, author, subreddit, title, ups, num_comments, CreatedUTC, text, permalink)  
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT * FROM reddit_posts WHERE post_id = %s
            )"""
    comment_statement = """
             INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, parent_id)
             SELECT %s, %s, %s, %s, %s, %s, %s
             WHERE NOT EXISTS(
                 SELECT * FROM reddit_replies WHERE id = %s
             )"""

    def __init__(self, i: int, sub_name: str, cursor, error_queue: mp.Queue, metrics_queue: mp.Queue,
                 task_queue: mp.Queue,
                 stop_event: mp.Event):
        self.sub_name = sub_name
        self._id = i
        self.cursor = cursor
        self._error_queue = error_queue
        self._metrics_queue = metrics_queue
        self._stop_event = stop_event
        self._task_queue = task_queue
        super().__init__(target=self.run, args=())

    def run(self):
        task = self._task_queue.get()
        logger.info(f"thread [{self._id}]: updating scores from submission with id [{task}]")
        try:
            submission = rc.__reddit__.submission(id=task)

            values = (submission.id, str(submission.author), self.sub_name, submission.title,
                      submission.score, submission.num_comments, submission.created_utc, submission.selftext,
                      submission.permalink, submission.id)
            self.cursor.execute(self.submission_statement, values)

            submission.comments.replace_more(limit=None)
            comment_count = 0
            for comment in submission.comments.list():
                values = (comment.id, str(comment.author), comment.body, self.sub_name,
                          comment.score, comment.created_utc, comment.parent_id, comment.id)

                self.cursor.execute(self.comment_statement, values)
                comment_count += 1

        except Exception as e:
            logger.error("Error on inserting a submission: {0}".format(e))
        else:
            logger.info(f"inserted [{task}] post with [{comment_count}] comments from [{self.sub_name}]")

        logger.info(f"thread [{self._id}]: finished with task [{task}]")
        logger.info(f"thread [{self._id}]: tasks left: {self._task_queue.qsize()}")

        if self._task_queue.empty():
            logger.info(f"thread [{self._id}]: finished all work!!! returning")
            return
        else:
            self.run()
