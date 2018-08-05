import time

import requests
from threading import Thread

import DBClient as db
import RedditClient as rc

start, end = 1498867200, 1530403200
subreddits = ['btc', 'BlockChain', 'NEO', 'altcoin', 'CryptoMarkets', 'ethtrader', 'Iota', 'CryptoMoonShots',
              'ethereum', 'Bitcoin', 'crypto', 'CryptoCurrency']

base_url = 'https://api.pushshift.io/reddit/search/submission/?'
total_comment_count, total_submission_count = 0, 0
rc.__reddit__ = rc.create_agent()
conn_manager = db.ConnectionPool(max_conns=20)


def reap_submissions(start, end, subreddit):
    search_string = f"after={start}&before={end}&subreddit={subreddit}&size=500"
    res = requests.get(base_url + search_string)
    data = res.json()['data']
    if len(data) == 0:
        return
    latest = int(data[-1]['created_utc'])
    while latest <= end:
        yield data
        yield from reap_submissions(latest, end, subreddit)


comment_statement = """
         INSERT INTO reddit_replies(id, author, text, subreddit, Ups, CreatedUTC, parent_id)
         SELECT %s, %s, %s, %s, %s, %s, %s
         WHERE NOT EXISTS(
             SELECT * FROM reddit_replies WHERE id = %s
         )"""
submission_statement = """
        INSERT INTO reddit_posts 
        (post_id, author, subreddit, title, ups, num_comments, CreatedUTC, text, permalink)  
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT * FROM reddit_posts WHERE post_id = %s
        )"""


def reap(name):
    print(f'working on: [{name}]')
    cursor = next(conn_manager)
    for idx, data in enumerate(reap_submissions(start, end, name)):
        print(f"latest stamp: [{data[-1]['created_utc']}]; batch number: [{idx}]")
        for submission_dict in data:
            _id = submission_dict['id']
            submission = rc.__reddit__.submission(id=_id)

            values = (submission.id, str(submission.author), name, submission.title,
                      submission.score, submission.num_comments, submission.created_utc, submission.selftext,
                      submission.permalink, submission.id)
            try:
                cursor.execute(submission_statement, values)
            except Exception as e:
                print("Error on executing sql: {0}".format(e))
            else:
                print(f"successfully inserted [{_id}] post from [{name}]")
                global total_submission_count
                total_submission_count += 1

            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():
                values = (comment.id, str(comment.author), comment.body, name,
                          comment.score, comment.created_utc, comment.parent_id, comment.id)
                try:
                    cursor.execute(comment_statement, values)
                except Exception as e:
                    print("Inner loop: Error on executing sql: {0}".format(e))
                else:
                    print(f"successfully inserted [{comment.id}] comment from [{name}]")
                    global total_comment_count
                    total_comment_count += 1


pool = []
for subreddit_name in subreddits:
    t = Thread(target=reap, args=(subreddit_name,))
    t.start()
    pool.append(t)

while any([t.isAlive() for t in pool]):
    time.sleep(30)

print(f"total comments reaped: [{total_comment_count}]")
print(f"total submissions reaped: [{total_submission_count}]")
