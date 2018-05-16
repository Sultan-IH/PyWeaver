import praw
import yaml

from DBClient import DBCLient
import logging


class RedditClient:
    wallclock_interval = 24 * 60 * 60
    update_cycle_interval = 10 * 60

    def __init__(self, path_to_agent: str, db_cli: DBCLient):
        with open(path_to_agent) as file:
            self.agent = yaml.load(file)
        self.db_cli = db_cli
        self.reddit = praw.Reddit(user_agent=f'Comment Extraction (by /u/{self.agent["username"]})',
                                  client_id=self.agent["client_id"], client_secret=self.agent["client_secret"],
                                  username=self.agent["username"], password=self.agent["password"])