"""
A module to serve as the entry point to the cleaners package.
"""
# Python imports
import logging
import sys

# Third party imports
import googleapiclient
from google.appengine.api import app_identity
import oauth2client

# Project imports
import bq_utils
#import constants.bq_utils as bq_consts
import constants.cleaners.combined as combined_consts
import constants.cleaners.combined_deid as deid_consts

LOGGER = logging.getLogger(__name__)

def _add_console_logging(add_handler):
    # this config should be done in a separate module, but that can wait
    # until later.  Useful for debugging.
    if add_handler:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)
    elif LOGGER.handlers == []:
        logging.basicConfig(filename='/tmp/cleaner.log')


def _clean_dataset(project=None, dataset=None, statements=None):
    if project is None or project == '' or project.isspace():
        project = app_identity.get_application_id()
        LOGGER.debug('Project name not provided.  Using default.')


    if statements is None:
        statements = []

    failures = 0
    successes = 0
    for statement in statements:
        full_query = statement.format(project=project, dataset=dataset)

        try:
            results = bq_utils.query(full_query)
        except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError):
            LOGGER.exception("FAILED:  Clean rule not executed:\n%s", full_query)
            failures += 1
            continue

        LOGGER.info("Executing query %s", full_query)

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
        LOGGER.warning("Failed to apply %d clean rules for %s.%s",
                       failures, project, dataset)


def clean_rdr_dataset():
    # stub:  to be implemented as needed
    pass


def clean_ehr_dataset():
    # stub:  to be implemented as needed
    pass

def clean_unioned_ehr_dataset():
    # stub:  to be implemented as needed
    pass


def clean_ehr_rdr_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_ehr_rdr_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning ehr_rdr_dataset")
    _clean_dataset(project, dataset, combined_consts.SQL_QUERIES)


def clean_ehr_rdr_unidentified_dataset(project=None, dataset=None):
    if dataset is None or dataset == '' or dataset.isspace():
        dataset = bq_utils.get_dataset_id()
        LOGGER.info('Dataset is unspecified.  Using default value of:\t%s', dataset)

    LOGGER.info("Cleaning de-identified dataset")
    _clean_dataset(project, dataset, deid_consts.SQL_QUERIES)


def clean_all_cdr():
    clean_rdr_dataset()
    clean_ehr_dataset()
    clean_unioned_ehr_dataset()
    clean_ehr_rdr_dataset()
    clean_ehr_rdr_unidentified_dataset()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true', help=('Send logs to console'))
    args = parser.parse_args()
    _add_console_logging(args.s)

    clean_all_cdr()
