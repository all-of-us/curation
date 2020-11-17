"""
Resource creation entry module.

This module coordinates creating resources used in tests.
"""
import os

from google.oauth2 import service_account
from google.cloud import bigquery

from ci.test_buckets import create_test_buckets
from ci.test_datasets import create_test_datasets

DATASET_NAMES = [
    'RDR_DATASET_ID', 'COMBINED_DATASET_ID', 'BIGQUERY_DATASET_ID',
    'UNIONED_DATASET_ID'
]
"""Datasets that are expected to be created for testing purposes only."""

BUCKET_NAMES = [
    'DRC_BUCKET_NAME', 'BUCKET_NAME_FAKE', 'BUCKET_NAME_NYC',
    'BUCKET_NAME_PITT', 'BUCKET_NAME_CHS', 'BUCKET_NAME_UNIONED_EHR'
]
"""Buckets that are expected to be created for testing purposes only."""

REQUIREMENTS = [
    'APPLICATION_ID', 'USERNAME', 'GOOGLE_APPLICATION_CREDENTIALS',
    'VOCABULARY_DATASET'
]
"""
Variables that are required to run the tests, but are not expected to be created.
"""

CLIENT = None


def get_client(project_id, app_creds):
    """
    Ensure only one client is created and reused
    :param project_id:  project to get a client for
    :returns: a big query client object
    """
    global CLIENT
    if not CLIENT:
        credentials = service_account.Credentials.from_service_account_file(
            app_creds)
        CLIENT = bigquery.Client(project=project_id, credentials=credentials)

    return CLIENT


def get_environment_config():
    """
    Uses the referenced variable names to read values from the environment.

    So, even if a variable is defined in the environment but is not referenced
    in one of the lists, it is ignored during setup.  Whether this is a feature
    or a bug is debatable.

    return: a dictionary of variables read from the environment.
    """
    config = {}

    env_vars = DATASET_NAMES + BUCKET_NAMES + REQUIREMENTS
    for var in env_vars:
        config[var] = os.environ.get(var)

    return config


def main():
    """
    Controller function for creating test resources.

    Oversees creating test buckets and datasets.
    """
    config = get_environment_config()

    create_test_buckets(config, BUCKET_NAMES)
    create_test_datasets(config, DATASET_NAMES)


if __name__ == "__main__":
    main()
