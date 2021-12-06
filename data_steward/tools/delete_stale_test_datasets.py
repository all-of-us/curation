"""
Delete stale datasets in test environment. 
"""
from datetime import datetime, timezone
import logging
import os

# Project imports
from utils import bq, pipeline_logging

LOGGER = logging.getLogger(__name__)


def _check_project(bq_client):
    """
    Check if the project is set to test.

    :param bq_client: Client
    :return: None if the project is set properly
    :raise: ValueError if project is not 'aou-res-curation-test'
    """

    if 'test' not in bq_client.project:
        raise ValueError(
            f'Wrong project: {bq_client.project}. This script runs only for test environment'
        )

    return None


def _filter_stale_datasets(bq_client, first_n: int = None):
    """
    Get the first n datasets that meet all of the following criteria:
    1. Datasets older than 90 days
    2. Empty datasets

    :param bq_client: Client
    :param first_n: number of datasets to return. If not specified, return all.
    :return: list of dataset names that are stale
    """

    stale_datasets, n = [], 0
    now = datetime.now(timezone.utc)

    dataset_names = [
        dataset.dataset_id for dataset in bq_client.list_datasets()
    ]

    for dataset_name in dataset_names:

        if first_n and n >= first_n:
            break

        dataset_created = bq_client.get_dataset(dataset_name).created

        if (now - dataset_created).days <= 90:
            continue

        if next(bq_client.list_tables(dataset_name), None):
            continue

        stale_datasets.append(dataset_name)
        LOGGER.info(
            f"{n}: stale_dataset={dataset_name}, time_created={dataset_created}."
        )
        n += 1

    return stale_datasets


def main():

    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    bq_client = bq.get_client(os.environ.get('GOOGLE_CLOUD_PROJECT'))

    _check_project(bq_client)

    stale_datasets = _filter_stale_datasets(bq_client, first_n=100)

    for stale_dataset in stale_datasets:
        LOGGER.info(
            f"Running - bq_client.delete_dataset({stale_dataset})"
        )  # or LOGGER.critical/warn/error/ (debug) whichever circleci creates output
        #Uncomment the following before release.
        #bq_client.delete_dataset({stale_dataset})


if __name__ == "__main__":
    main()