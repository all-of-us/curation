"""
Test bucket resources for integration tests.

Creates buckets with a 30 day lifecycle.
Bucket deletion and modification functions (for testing purposes) should be
added to this module.
"""
from datetime import datetime, timedelta

from google.cloud import storage
from google.cloud.exceptions import Conflict, NotFound
from google.oauth2 import service_account

CLIENT = None
"""Storage Client object"""


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
        CLIENT = storage.Client(project=project_id, credentials=credentials)

    return CLIENT


def _create_bucket(config, bucket_name):
    """
    Create a bucket in the test project.

    :param config: a dictionary of os.environ variables needed to create
        the test bucket.
    :param bucket_name:  the string used to name the bucket.
    """
    app_id = config.get('APPLICATION_ID', 'NOT SET')
    storage_client = get_client(app_id,
                                config.get('GOOGLE_APPLICATION_CREDENTIALS'))

    # Creates the new bucket
    bucket = storage.bucket.Bucket(storage_client, name=bucket_name)
    # set lifecycle
    bucket.add_lifecycle_delete_rule(age=30)
    bucket.location = "US"

    try:
        bucket = storage_client.create_bucket(bucket)
    except Conflict:
        print(f"{bucket_name} already exists.  moving on.")
    else:
        # set iam policy
        policy = bucket.get_iam_policy()
        policy.version = 1
        policy.bindings.append({
            "role":
                "roles/storage.objectAdmin",
            "members": [
                "group:all-of-us-data-curation-eng.staging@staging.pmi-ops.org",
                f"serviceAccount:{app_id}@appspot.gserviceaccount.com",
                f"serviceAccount:circleci-test@{app_id}.iam.gserviceaccount.com"
            ]
        })
        policy.bindings.append({
            "role":
                "roles/storage.objectViewer",
            "members": [
                "group:all-of-us-data-curation-eng.staging@staging.pmi-ops.org",
                f"serviceAccount:{app_id}@appspot.gserviceaccount.com",
                f"serviceAccount:circleci-test@{app_id}.iam.gserviceaccount.com"
            ]
        })
        bucket.set_iam_policy(policy)

        print(f"Bucket {bucket.name} created.")


def create_test_buckets(config, buckets):
    """
    Create the buckets defined in  the buckets iterable.

    :param config: a dictionary of environment variables and values needed
        to create test  buckets
    :param buckets: an iterable containing strings to use  as bucket names
    """
    for name_id in buckets:
        name = config.get(name_id)
        _create_bucket(config, name)


def _delete_bucket(config, bucket_name):
    """
    Delete a bucket and log.

    Delete a bucket and its contents.  If the bucket is not found, report
    that too.

    :param config: dictionary of environment variables.
    :param bucket_name: name of the bucket to identify and delete
    """
    storage_client = get_client(config.get('APPLICATION_ID', 'NOT SET'),
                                config.get('GOOGLE_APPLICATION_CREDENTIALS'))

    # Get the bucket
    try:
        bucket = storage_client.get_bucket(f'gs://{bucket_name}')
    except NotFound:
        print(f"Bucket 'gs://{bucket_name}' does not exist and cannot be deleted.")
    else:
        # remove bucket contents and delete bucket
        bucket.delete(force=True, client=storage_client)
        print(f"Bucket 'gs://{bucket.name}' deleted.")


def delete_test_buckets(config, buckets):
    """
    Delete test buckets.

    Delete buckets older than a maximum age that are also empty.

    :param config: a dictionary of environment variables and vaues needed
        to delete buckets
    :param buckets: an iterable containing strings to use as bucket names
        to delete
    """
    for name_id in buckets:
        name = config.get(name_id)
        _delete_bucket(config, name)

def delete_old_test_buckets(config):
    """
    Delete test buckets older than 90 days.

    Limit to removing 500 per call to prevent overloading the system.

    :param config: environment variables dictionary
    """
    storage_client = get_client(config.get('APPLICATION_ID', 'NOT SET'),
                                config.get('GOOGLE_APPLICATION_CREDENTIALS'))

    old_buckets = []
    for bucket in storage_client.list_buckets(500):
        if bucket.time_created < datetime.now() - timedelta(days=90):
            old_buckets.append(bucket)

    for bucket in old_buckets:
        if list(storage_client.list_blobs(bucket, max_results=2):
            print(f"Bucket 'gs://{bucket.name}' is not empty.  Skipping removal.")
        else:
            _delete_bucket(config, bucket.name)
