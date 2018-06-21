import requests, os, logging, atexit, schedule, time, yaml
from datetime import datetime
from multiprocessing.dummy import Process, Queue, Value
from log_config import handler

logger = logging.getLogger(__name__)
logger.addHandler(handler)
IS_PRODUCTION = True if os.getenv("PRODUCTION") == 'TRUE' else False


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

    def _send_report(self, comment_count, post_count):
        metrics = " <p>Posts Scraped: %d</p> <p>Comments Scraped: %d</p>" % (
            post_count, comment_count)

        report = load_gql('./servus/newReport.gql') % (self._id, metrics)
        response = make_request(self.servus_address, report)
        logger.info("metrics report: " + report)
        if response['newReport']['ok']:
            logger.info('sent an hourly report' + metrics)
            self.post_count = 0
            self.comment_count = 0
        else:
            logger.info("bad response when trying to send a metric report")

    def run_report_cycle(self):
        schedule.every(self.report_interval).seconds.do(self._send_report)

        def run():
            while True:
                schedule.run_pending(), time.sleep(1)

        self.report_process = Process(target=run)
        self.report_process.start()

    def run_report_cycle(self, queue):
        time.sleep(self.report_interval)
        comment_count, post_count = 0, 0
        for _ in range(queue.qsize()):
            item = queue.get()
            if item == 'comment':
                comment_count += 1
            elif item == 'submission':
                post_count += 1
        if IS_PRODUCTION:
            self._send_report(comment_count, post_count)
        else:
            print(f"""
                num comments: {comment_count}
                submission count: {post_count}
            """)
        self.run_report_cycle(queue)

    def run_error_reports(self, error_queue: Queue):
        """
        not sure if needed as calling get() will remove the error from error_queue
        if run in a separate thread, will create race conditions with main
        :param error_queue: Queue
        :return:
        """
        exception = error_queue.get()  # blocks until error is recieved
        self._report_error(exception)
        self.run_error_reports(error_queue)

    def report_error(self, exception: Exception):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        report = load_gql('./servus/newReport.gql') % (str(exception), str(now), str(self._id))
        logger.info("error report: " + report)
        response = make_request(self.servus_address, report)
        if not response['removeNode']['ok']:
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
