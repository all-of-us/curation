import os

from ci.create_buckets import create_test_buckets
from ci.create_datasets import create_test_datasets

DATASET_NAMES = [
    'RDR_DATASET_ID', 'COMBINED_DATASET_ID', 'BIGQUERY_DATASET_ID',
    'UNIONED_DATASET_ID'
]
BUCKET_NAMES = [
    'DRC_BUCKET_NAME', 'BUCKET_NAME_FAKE', 'BUCKET_NAME_NYC',
    'BUCKET_NAME_PITT', 'BUCKET_NAME_CHS', 'BUCKET_NAME_UNIONED_EHR'
]
REQUIREMENTS = ['APPLICATION_ID', 'USERNAME', 'GOOGLE_APPLICATION_CREDENTIALS']


def get_environment_config():
    config = {}

    env_vars = DATASET_NAMES + BUCKET_NAMES + REQUIREMENTS
    for var in env_vars:
        config[var] = os.environ.get(var)

    return config


def main():
    config = get_environment_config()
    create_test_buckets(config, BUCKET_NAMES)
    create_test_datasets(config, DATASET_NAMES)


if __name__ == "__main__":
    main()
