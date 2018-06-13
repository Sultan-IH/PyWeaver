import praw, yaml
from typing import Generator

__reddit__ = None


def create_agent(path_to_agent: str) -> praw.Reddit:
    with open(path_to_agent) as file:
        agent = yaml.load(file)
    return praw.Reddit(user_agent=f'Comment Extraction (by /u/{agent["username"]})',
                       client_id=agent["client_id"], client_secret=agent["client_secret"],
                       username=agent["username"], password=agent["password"])


def stream_subreddit_comments(subname: str) -> Generator:
    """
    :return: A generator which yields comments  from a subreddit
    """

    subreddit = __reddit__.subreddit(subname)
    for comment in subreddit.stream.comments():
        yield comment


def stream_subreddit_submissions(subname: str) -> Generator:
    """
    :return: A generator which yields  posts from a subreddit
    """

    subreddit = __reddit__.subreddit(subname)
    for submission in subreddit.stream.submissions():
        yield submission
