import atexit
import time

import logging
import os
import requests
from datetime import datetime
from multiprocessing.dummy import Process, Queue, Value

from env_config import IS_PRODUCTION

logger = logging.getLogger(__name__)


class Node:
    report_interval = 3 * 3600 if IS_PRODUCTION else 30
    comment_count = 0
    post_count = 0

    def __init__(self, config: dict):

        self.jobs = []
        self.node_name = os.getenv("NODE_NAME")  # reference to the machine this is running on
        self.servus_address = os.getenv("SERVUS_ADDRESS") + '/graphql'
        self.program_name = config['name']
        self.version = config['version']

        if self.node_name is None or self.servus_address is None:
            logger.error("No nodename or servus address provided. exiting")
            print("No nodename or servus address provided. exiting"), exit(1)

        # registering with servus
        query = load_gql("./servus/newNode.gql") % (self.program_name, self.version, self.node_name)
        response = make_request(self.servus_address, query)
        if not response['newNode']['ok']:
            logger.error("Bad response from servus upon registering. exiting...")
            print("Bad response from servus upon registering. exiting..."), exit(1)

        self._id = response['newNode']['id']
        logger.info("registered with Servus with id: [%s]", self._id)
        atexit.register(self.die)

    def get_resources(self, collection, n):
        job = {
            'collection': collection,
            'n': n
        }
        query = load_gql("./servus/getResources.gql") % (n, collection, self._id)
        response = make_request(self.servus_address, query)
        if not response['getResources']:
            logger.error("Bad response from servus upon registering. exiting...")
            exit(1)

        job['tasks'] = [coin['name'] for coin in response['getResources']]
        self.jobs.append(job)
        logger.info("got tasks: " + ' '.join(job['tasks']))

    def die(self):
        query = load_gql('./servus/removeNode.gql') % self._id
        response = make_request(self.servus_address, query)
        if response['removeNode']['ok']:
            logger.info('Node died gracefully')

    def _report_metrics(self, html_metrics):

        report = load_gql('./servus/newReport.gql') % (self._id, html_metrics)
        response = make_request(self.servus_address, report)
        if response['newReport']['ok']:
            logger.info('sent an hourly report' + report)
            self.post_count = 0
            self.comment_count = 0
        else:
            logger.info("bad response when trying to send a metric report")

    def run_report_cycle(self, report):
        time.sleep(self.report_interval)
        if IS_PRODUCTION:
            metrics_html = report.make_html()
            self._report_metrics(metrics_html)
        else:
            metrics_str = report.make_str()
            print(metrics_str)
        self.run_report_cycle(report)

    def report_error(self, exception: Exception):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        report = load_gql('./servus/reportError.gql') % (str(exception), str(now), str(self._id))
        logger.info("error report: " + report)
        response = make_request(self.servus_address, report)
        if not response['reportError']['ok']:
            logger.info("bad response when trying to send an error report")


### HELPER FUNCTIONS ###


def make_request(address, query: str):
    r = requests.post(address, json={"query": query})
    if r.status_code != 200:
        logger.error("Bad response from servus when submitting request"), exit(1)
    response = r.json()
    return response['data']


def load_gql(file_path: str):
    with open(file_path) as file:
        return file.read()


def wrap_in_process(func, args=tuple()):
    print("Running " + func.__name__ + " in a process")
    p = Process(target=func, args=args)
    p.start()
    return p


def listen_val(v: Value):
    while True:
        print("listen: " + str(v.value))
        time.sleep(5)


def listen_queue(q: Queue):
    while True:
        print("listen_queue: " + str(q.get()))
