import requests, os, logging, json
from log_setup import get_config, init_logs


class Node:
    hourly_comment_count = 0
    hourly_post_count = 0

    def __init__(self):
        init_logs()
        self.jobs = []
        self.node_name = os.getenv("NODE_NAME")
        config = get_config()
        self.servus_address = config['servus_address'] + '/graphql'
        self.program_name = config['name']
        self.version = config['version']

        if self.node_name is None or self.servus_address is None:
            logging.error("No nodename or servus address provided. exiting")
            exit(1)

        # registering with servus
        query = load_gql("./servus/newNode.gql") % (self.program_name, self.version, self.node_name)
        response = make_request(self.servus_address, query)
        if not response['newNode']['ok']:
            logging.error("Bad response from servus upon registering. exiting...")
            exit(1)
        self._id = response['newNode']['id']
        logging.info("registered with Servus with id: [%s]", self._id)

    def get_resources(self, collection, n):
        job = {
            'collection': collection,
            'n': n
        }
        query = load_gql("./servus/getResources.gql") % (n, collection, self._id)
        response = make_request(self.servus_address, query)
        if not response['getResources']:
            logging.error("Bad response from servus upon registering. exiting...")
            exit(1)

        job['tasks'] = [coin['name'] for coin in response['getResources']]
        self.jobs.append(job)
        logging.info("got tasks: " + ' '.join(job['tasks']))

    def die(self):
        query = load_gql('./servus/removeNode.gql') % self._id
        response = make_request(self.servus_address, query)
        if response['removeNode']['ok']:
            logging.info('Node died gracefully')

    def send_report(self):
        metrics = " <p>Posts Scraped: %d</p> <p>Comments Scraped: %d</p>" % (
            self.hourly_post_count, self.hourly_comment_count)
        report = load_gql('./servus/newReport.gql') % (self._id, metrics)
        response = make_request(self.servus_address, report)
        if response['removeNode']['ok']:
            logging.info('sent an hourly report' + metrics)
            self.hourly_comment_count = 0
            self.hourly_post_count = 0


def make_request(address, query: str):
    r = requests.post(address, json={"query": query})
    if r.status_code != 200:
        logging.error("Bad response from servus when submitting request")
        exit(1)
    response = r.json()
    return response['data']


def load_gql(file_path: str):
    with open(file_path) as file:
        return file.read()
