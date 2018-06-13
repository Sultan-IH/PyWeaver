from psycopg2.pool import ThreadedConnectionPool
import psycopg2 as pg
import yaml, logging
from RedditClient import stream_subreddit_submissions, stream_subreddit_comments


class DBCLient:
    def __init__(self, file_path, conn=None):
        if file_path:
            with open(file_path) as file:
                self.config = yaml.load(file)
            conn_params = (
                f"dbname={self.config['DB_NAME']} user={self.config['DB_USER']}"
                f" password={self.config['DB_PASSWORD']} host={self.config['DB_HOST']} ")
            pool = ThreadedConnectionPool(1, 40, conn_params)
            self.pool = pool

        elif conn:
            self.cursor = conn.cursor()

    def new_cli(self):
        new_conn = self.pool.getconn()
        return DBCLient(file_path=None, conn=new_conn)

    def update_score_comment(self, comment_id, score):
        sql_statement = f"""
                UPDATE reddit_replies
                SET ups = {score}
                WHERE id = '{comment_id}'
            """
        self.cursor.execute(sql_statement)


def insert_submission(subname: str):
    """
    Testing for now
    :return:
    """

    for submission in stream_subreddit_submissions(subname):
        print("[%s] post" % submission.title)


def insert_comment_dummy(subname: str):
    """
    Testing for now
    :return:
    """

    for comment in stream_subreddit_comments(subname):
        print("[%s] comment in [%s]" % (comment.id, subname))


def insert_comment(args):
    """
    Testing for now
    :return: void
    """
    subname, cursor = args[0], args[1]
    sql_statement = """
             INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, parent_id)
             SELECT %s, %s, %s, %s, %s, %s, %s
             WHERE NOT EXISTS(
                 SELECT * FROM reddit_replies WHERE id = '{id}'
             )"""
    for comment in stream_subreddit_comments(subname):
        values = (comment.id, str(comment.author), comment.body, subname,
                  comment.score, comment.created_utc, comment.parent_id)
        try:
            cursor.execute(sql_statement, values)
        except (pg.IntegrityError, pg.OperationalError) as e:
            logging.warning("Error on executing sql: {0}".format(e))
        else:
            logging.info("Inserted comment from the " + subname + " subreddit")
