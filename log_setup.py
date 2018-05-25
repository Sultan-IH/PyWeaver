import logging
import yaml
from datetime import datetime


def init_logs():
    date = datetime.now().strftime("%Y-%m-%d.%H:%M:%S")

    config = get_config()
    logfile = "./logs/" + date + "." + config['version'] + "." + config['name'] + ".log"

    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def get_config():
    with open('config.yaml') as file:
        try:
            config = yaml.load(file)
        except yaml.YAMLError as exc:
            exit(1, exc)

    return config
