"""
Test resource creation entry module.

This module coordinates creating resources used in tests.
This includes coordinating the creation of buckets and datasets.
"""
import argparse
import os

from ci.test_buckets import create_test_buckets, delete_test_buckets
from ci.test_datasets import create_test_datasets, delete_test_datasets

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


def get_args(raw_args=None):
    parser = argparse.ArgumentParser(
        "Test dataset and bucket setup and teardown script")
    parser.add_argument(
        'action',
        choices=('setup', 'teardown', 'cleanup'),
        help=('Action to take.  Either \'setup\', (create), '
              '\'teardown\', (delete), or \'cleanup\' (remove old) '
              'test resources.  \'cleanup\' targets buckets older '
              '90 days.'))
    test_args = parser.parse_args()
    return test_args


def main(raw_args=None):
    """
    Controller function for test resources.

    Oversees creation and cleanup of test buckets and datasets.
    """
    args = get_args(raw_args)

    config = get_environment_config()

    if args.action == 'setup':
        create_test_buckets(config, BUCKET_NAMES)
        create_test_datasets(config, DATASET_NAMES)
    elif args.action == 'teardown':
        delete_test_datasets(config, DATASET_NAMES)
        delete_test_buckets(config, BUCKET_NAMES)
    elif args.action == 'cleanup':
        delete_old_test_buckets(config)
    else:
        print(f'action \'{action}\' not understood.  nothing done.')

    print('Test resource controller done.')


if __name__ == "__main__":
    main()
