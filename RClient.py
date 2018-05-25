import praw, psycopg2 as pq
import yaml
from hashlib import sha256
from struct import pack
from DBClient import DBCLient
import logging


class RedditClient:

    def __init__(self, path_to_agent: str, db_cli: DBCLient):
        with open(path_to_agent) as file:
            self.agent = yaml.load(file)
        self.db_cli = db_cli
        self.reddit = praw.Reddit(user_agent=f'Comment Extraction (by /u/{self.agent["username"]})',
                                  client_id=self.agent["client_id"], client_secret=self.agent["client_secret"],
                                  username=self.agent["username"], password=self.agent["password"])

    def stream_comments(self, subreddit_name):
        subreddit = self.reddit.subreddit(subreddit_name)
        sql_statement = """
                 INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, hash, parent_id)
                 SELECT %s, %s, %s, %s, %s, %s, %s, %s
                 WHERE NOT EXISTS(
                     SELECT * FROM reddit_replies WHERE hash = '{hash}'
                 )"""
        for comment in subreddit.stream.comments():
            _hash = self.hash_comment(comment, subreddit_name)
            values = (comment.id, str(comment.author), comment.body, subreddit_name,
                      comment.score, comment.created_utc, pq.Binary(_hash), comment.parent_id)
            try:
                self.db_cli.cursor.execute(sql_statement, values)
            except pq.IntegrityError or pq.OperationalError as e:
                logging.warning("Error on executing sql: {0}".format(e))
            finally:
                logging.info("Inserted comment from the " + subreddit_name + " subreddit")
                yield 1

    def hash_comment(self, comment, subreddit):
        _bytes = bytearray(comment.body, 'utf-8')
        _bytes += bytearray(str(comment.author), 'utf-8')
        _bytes += bytearray(subreddit, 'utf-8')
        _bytes += bytes(pack('f', comment.created_utc))
        return sha256(_bytes).digest()
