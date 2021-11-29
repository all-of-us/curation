"""
Delete stale buckets in test environment. 
"""
from datetime import datetime, timezone
import logging

# Project imports
from utils import pipeline_logging
from gcloud.gcs import StorageClient

LOGGER = logging.getLogger(__name__)


def _check_project(storage_client):
    """
    Check if the project is set to test.

    :param storageclient: StorageClient
    :return: None if the project is set properly
    :raise: ValueError if project is not 'aou-res-curation-test'
    """
    test_project = 'aou-res-curation-test'

    if storage_client.project != test_project:
        raise ValueError(
            f'Wrong project: {storage_client.project}. This script runs only for {test_project}'
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

        if next(storage_client.list_blobs(bucket.name), None):
            continue

        stale_buckets.append(bucket.name)
        LOGGER.info(
            f"{n}: stale_bucket={bucket.name}, time_created={bucket.time_created}."
        )
        n += 1

    return stale_buckets


def main():

    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    sc = StorageClient()

    _check_project(sc)

    stale_buckets = _filter_stale_buckets(storage_client=sc, first_n=200)

    for stale_bucket in stale_buckets:
        LOGGER.info(f"Running - sc.get_bucket({stale_bucket}).delete()")
        sc.get_bucket(stale_bucket).delete()


if __name__ == "__main__":
    main()
