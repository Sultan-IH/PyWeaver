import sys

import logging.handlers
import os
import yaml
from datetime import datetime

# when imported all the logging variables needed for other modules become available

IS_PRODUCTION = True if os.getenv("PRODUCTION") == 'TRUE' else False


def get_config() -> dict:
    with open('config.yaml') as file:
        try:
            config = yaml.load(file)
        except yaml.YAMLError as exc:
            print("get_config got an error" + str(exc)), exit(1)
        else:
            return config


PROGRAM_CONFIG = get_config()

date = datetime.now().strftime("%Y-%m-%d.%H:%M:%S")

LOG_BASE = PROGRAM_CONFIG['logdir'] + date + "." + PROGRAM_CONFIG['version'] + "." + PROGRAM_CONFIG['name']
LOG_FILENAME = LOG_BASE + ".log"

handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=2000000, backupCount=3)  # max size is 2mb
handler.setLevel(logging.INFO)  # Set logging level.

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

root_logger = logging.getLogger('')
root_logger.setLevel(logging.INFO)
root_logger.addHandler(handler)

if IS_PRODUCTION:
    STDOUT_FILE = LOG_BASE + '.stdout'
    sys.stdout = open(STDOUT_FILE, 'wt')
