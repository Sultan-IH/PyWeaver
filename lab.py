from multiprocessing.dummy import Pool, Process
import RedditClient as rc
from DBClient import insert_comment, DBCLient
from servus.node import Node
import os, logging

# only for the testing machine
os.environ['NODE_NAME'] = 'SULTAN_MAC_TEST'

# Registering with the load distribution server: Servus
num_subreddits = 4
n = Node()
n.get_resources('subreddits', num_subreddits)
# p = Process(target=n.report_wrapper)
# p.start()

# Injecting reddit instance into the module
rc.__reddit__ = rc.create_agent('./pyweaver.yaml')

# DB Setup
pg = DBCLient('./remote.yaml')
args = [(task, pg.new_cli().cursor) for task in n.jobs[-1]['tasks']]


def main():
    logging.info("Main routine started")
    try:
        with Pool(num_subreddits) as comments_pool:
            comments_pool.map(insert_comment, args)

    except Exception as e:
        logging.error(e)
        logging.info("Restarting reddit instance")

        # Reinjecting reddit instance and calling the function again.
        rc.__reddit__ = rc.create_agent('./pyweaver.yaml')
        main()


main()
