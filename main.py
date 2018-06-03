from RClient import RedditClient
from DBClient import DBCLient
from threading import Thread
import os, atexit, logging, schedule, time

from servus.node import Node

n = Node()

n.get_resources('subreddits', 6)

main_pg = DBCLient('./remote.yaml')

atexit.register(main_pg.pool.closeall)
atexit.register(n.die)


def start(job_name, db_cli):
    rcli = RedditClient("./pyweaver.yaml", db_cli)
    for i in rcli.stream_comments(job_name):
        n.hourly_comment_count += i


logging.info(n.jobs[-1]['tasks'])

for job in n.jobs[-1]['tasks']:
    logging.info("Job: " + job)
    new_cli = main_pg.new_cli()
    Thread(target=start, args=(job, new_cli)).start()

schedule.every(12).hour.do(n.send_report)

while True: schedule.run_pending(), time.sleep(1)
