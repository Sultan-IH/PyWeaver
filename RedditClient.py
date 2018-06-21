import praw, os
from typing import Generator
from multiprocessing.dummy import Queue

__reddit__ = None


def create_agent() -> praw.Reddit:
    return praw.Reddit(user_agent=f'Comment Extraction (by /u/{os.getenv("username")})',
                       client_id=os.getenv("client_id"), client_secret=os.getenv("client_secret"),
                       username=os.getenv("username"), password=os.getenv("password"))


def stream_subreddit_comments(subname: str, error_queue: Queue) -> Generator:
    """
    :return: A generator which yields comments from a subreddit
    """

    subreddit = __reddit__.subreddit(subname)
    try:

        for comment in subreddit.stream.comments():
            yield comment

    except Exception as e:
        error_queue.put(str(e))


def stream_subreddit_submissions(subname: str, error_queue: Queue) -> Generator:
    """
    :return: A generator which yields posts from a subreddit
    """

    subreddit = __reddit__.subreddit(subname)
    try:

        for submission in subreddit.stream.submissions():
            yield submission

    except Exception as e:
        error_queue.put(str(e))


def feed_submission_comments(_id: str, error_queue: Queue) -> Generator:
    """
    :param _id: id of the submission in a subreddit
    :return: traverse breadth, first the comment tree (returns comments)
    """
    submission = __reddit__.submission(id=_id)
    try:

        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            yield comment

    except Exception as e:
        error_queue.put(str(e))