"""
Delete first_n stale buckets in test environment. 
Stale buckets meet all of the following conditions:
(1) equal to or older than 90 days old, 
(2) empty, and 
(3) in test environment.
"""
import argparse
import logging
from datetime import datetime, timezone

# Project imports
import app_identity
from gcloud.gcs import get_storage_client
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)


def _check_project(storage_client):
    """
    Check if the project is set to test.

    :param storageclient: StorageClient
    :return: None if the project is set properly
    :raise: ValueError if project is not 'aou-res-curation-test'
    """

    if 'test' not in storage_client.project:
        raise ValueError(
            f'Wrong project: {storage_client.project}. This script runs only for test environment.'
        )

    return None


def _filter_stale_buckets(storage_client, first_n: int = None):
    """
    Get the first n buckets that meet all of the following criteria:
    1. Buckets older than 90 days
    2. Empty buckets

    :param storageclient: StorageClient
    :param first_n: number of buckets to return. If not specified, return all.
    :return: list of bucket names that are stale
    """

    buckets = storage_client.list_buckets()

    stale_buckets, n = [], 0

    now = datetime.now(timezone.utc)

    for bucket in buckets:

        if first_n and n >= first_n:
            break

        if (now - bucket.time_created).days <= 90:
            continue

        if len(list(storage_client.list_blobs(bucket.name))) >= 1:
            continue

        stale_buckets.append(bucket.name)
        LOGGER.info(
            f"{n}: stale_bucket={bucket.name}, time_created={bucket.time_created}."
        )
        n += 1

    return stale_buckets


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description="""Delete first_n stale buckets in test environment.""")
    parser.add_argument('--first_n',
                        action='store',
                        type=int,
                        dest='first_n',
                        help='First n stale buckets to delete.',
                        required=True)

    return parser


def main(first_n):

    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    project_id = app_identity.get_application_id()
    sc = get_storage_client(project_id)

    _check_project(sc)

    buckets_to_delete = _filter_stale_buckets(sc, first_n)

    for stale_bucket in buckets_to_delete:
        LOGGER.info(f"Running - sc.get_bucket({stale_bucket}).delete()")
        sc.get_bucket(stale_bucket).delete()

    return buckets_to_delete


if __name__ == "__main__":

    parser = get_arg_parser()
    args = parser.parse_args()

    main(args.first_n)
