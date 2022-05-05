"""
Delete first_n stale datasets in test environment. 
Stale datasets meet all of the following conditions:
(1) equal to or older than 90 days old, 
(2) empty, and 
(3) in test environment.
"""
from datetime import datetime, timezone
import argparse
import logging
import os

# Third party imports
from google.api_core import exceptions

# Project imports
from utils import pipeline_logging
from gcloud.bq import BigQueryClient

LOGGER = logging.getLogger(__name__)


def _check_project(bq_client: BigQueryClient):
    """
    Check if the project is set to test.

    :param bq_client: a BigQueryClient
    :return: None if the project is set properly
    :raise: ValueError if project is not 'aou-res-curation-test'
    """

    if 'test' not in bq_client.project:
        raise ValueError(
            f'Wrong project: {bq_client.project}. This script runs only for test environment'
        )

    return None


def _filter_stale_datasets(bq_client: BigQueryClient, first_n: int = None):
    """
    Get the first n datasets that meet all of the following criteria:
    1. Datasets older than 90 days
    2. Empty datasets

    :param bq_client: a BigQueryClient
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

        try:
            dataset_created = bq_client.get_dataset(dataset_name).created

            if (now - dataset_created).days <= 90:
                LOGGER.info(f"Skipping {dataset_name} - it is not old enough.")
                continue

            if len(list(bq_client.list_tables(dataset_name))) >= 1:
                LOGGER.info(f"Skipping {dataset_name} - it has datasets in it.")
                continue

            if len(list(bq_client.list_routines(dataset_name))) >= 1:
                LOGGER.info(f"Skipping {dataset_name} - it has routines in it.")
                continue

            if len(list(bq_client.list_models(dataset_name))) >= 1:
                LOGGER.info(f"Skipping {dataset_name} - it has models in it.")
                continue

            stale_datasets.append(dataset_name)
            LOGGER.info(
                f"{n}: stale_dataset={dataset_name}, time_created={dataset_created}."
            )
            n += 1

        except exceptions.NotFound as e:
            LOGGER.info(
                f"{dataset_name} not found. This dataset is likely to be "
                f"a temporary dataset that was created and deleted by other job or test. "
                f"Message: {e.message}")

    return stale_datasets


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description="""Delete first_n stale datasets in test environment.""")
    parser.add_argument('--first_n',
                        action='store',
                        type=int,
                        dest='first_n',
                        help='First n stale datasets to delete.',
                        required=True)

    return parser


def main(first_n):

    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    bq_client = BigQueryClient(os.environ.get('GOOGLE_CLOUD_PROJECT'))

    _check_project(bq_client)

    datasets_to_delete = _filter_stale_datasets(bq_client, first_n)

    for stale_dataset in datasets_to_delete:

        LOGGER.info(f"Running - bq_client.delete_dataset({stale_dataset})")

        try:
            bq_client.delete_dataset(stale_dataset)
        except exceptions.BadRequest as e:
            LOGGER.warning(
                f"Failed to delete {stale_dataset}. Message: {e.message}")

    return datasets_to_delete


if __name__ == "__main__":

    parser = get_arg_parser()
    args = parser.parse_args()

    main(args.first_n)