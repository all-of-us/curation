"""
Delete stale buckets in test environment. 
"""
from google.cloud import storage
from datetime import datetime, timezone
import logging

# Project imports
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)
CLIENT = storage.Client(project='aou-res-curation-test')


def _filter_stale_buckets(buckets: list, first_n: int = None):
    """
    Given a list of buckets, get the first n buckets that meet all of the following criteria:
    1. Buckets older than 90 days
    2. Empty buckets

    :param buckets: list of buckets to filter
    :param first_n: number of buckets to return. If not specified, return all.
    :return: list of bucket names that are stale
    """
    stale_buckets, n = [], 0

    now = datetime.now(timezone.utc)

    for bucket in buckets:

        if first_n and n >= first_n:
            break

        if (now - bucket.time_created).days > 90 and len(
                list(CLIENT.list_blobs(bucket.name))) == 0:
            stale_buckets.append(bucket.name)
            LOGGER.info(
                f"{n}: stale_bucket={bucket.name}, time_created={bucket.time_created}."
            )
            n += 1

    return stale_buckets


def main():

    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    stale_buckets = _filter_stale_buckets(buckets=CLIENT.list_buckets(),
                                          first_n=200)

    for stale_bucket in stale_buckets:
        LOGGER.info(f"Running - CLIENT.get_bucket({stale_bucket}).delete()")
        # Uncomment the following before release:
        # CLIENT.get_bucket(stale_bucket).delete()


if __name__ == "__main__":
    main()
