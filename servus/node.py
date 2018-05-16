import requests, os, logging, json
from log_setup import get_config, init_logs


class Node:

    def __init__(self):
        init_logs()
        self.servus_address = os.getenv("SERVUS_ADDRESS") + '/graphql'
        self.node_name = os.getenv("NODE_NAME")
        config = get_config()
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
