import logging

import googleapiclient
import oauth2client
from google.appengine.api import app_identity

import bq_utils

LOGGER = logging.getLogger(__name__)


def add_console_logging(add_handler):
    # this config should be done in a separate module, but that can wait
    # until later.  Useful for debugging.
    logging.basicConfig(level=logging.DEBUG,
                        filename='/tmp/cleaner.log',
                        filemode='a')

    if add_handler:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)


def clean_dataset(project=None, dataset=None, rule_query=None):
    """
    :param project: Name of the project
    :param dataset: Dataset name where cleaning rule should be applied
    :param rule_query: fully parsed query
    :return:
    """
    if project is None or project == '' or project.isspace():
        project = app_identity.get_application_id()
        LOGGER.debug('Project name not provided.  Using default.')

    failures = 0
    successes = 0

    try:
        results = bq_utils.query(rule_query)
    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
        LOGGER.exception("FAILED:  Clean rule not executed:\n%s", full_query)
        failures += 1

    LOGGER.info("Executing query %s", rule_query)

    # wait for job to finish
    query_job_id = results['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if incomplete_jobs != []:
        failures += 1
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    successes += 1

    if successes > 0:
        LOGGER.info("Successfully applied %d clean rules for %s.%s",
                    successes, project, dataset)
    else:
        LOGGER.warning("No clean rules successfully applied to %s.%s",
                       project, dataset)

    if failures > 0:
        print("Failed to apply %d clean rules for %s.%s",
              failures, project, dataset)
        LOGGER.warning("Failed to apply %d clean rules for %s.%s",
                       failures, project, dataset)
