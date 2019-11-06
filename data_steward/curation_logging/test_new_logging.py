# Imports the Google Cloud client library
from flask import request
from curation_logging.curation_gae_handler import *

LOG_NAME = 'curation_gae_logger'

if __name__ == '__main__':
    # Instantiates a client
    # logging_client = client.Client()
    # logger = logging_client.logger(LOG_NAME)
    # logger.delete()

    begin_request_logging()
    logging.info('Hello world %s %s', 'number', 'two')
    logging.error('Bad world')
    logging.info('Hello world')
    logging.warning('Bad world')
    end_request_logging(None)

    begin_request_logging()
    logging.info('Hello world 2')
    logging.error('Bad world 2')
    end_request_logging(None)

    # curation_logging_handler = CurationLoggingHandler()
    #
    # cloud_logger = logging.getLogger(LOG_NAME)
    # cloud_logger.setLevel(logging.INFO)
    # cloud_logger.addHandler(curation_logging_handler)
    #
    # cloud_logger.info("Hello world!")
    #
    # cloud_logger.error("Terrible world!")
