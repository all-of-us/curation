"""
Test bucket resources for integration tests.

Creates buckets with a 30 day lifecycle.
Bucket deletion and modification functions (for testing purposes) should be
added to this module.
"""
from google.cloud import storage
from google.cloud.exceptions import Conflict
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


def create_bucket(config, bucket_name):
    """
    Create a bucket in the test project.

    :param config: a dictionary of os.environ variables needed to create
        the test bucket.
    :param bucket_name:  the string used to name the bucket.
    """
    storage_client = get_client('aou-res-curation-test',
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
                "serviceAccount:aou-res-curation-test@appspot.gserviceaccount.com",
                "serviceAccount:circleci-test@aou-res-curation-test.iam.gserviceaccount.com"
            ]
        })
        policy.bindings.append({
            "role":
                "roles/storage.objectViewer",
            "members": [
                "group:all-of-us-data-curation-eng.staging@staging.pmi-ops.org",
                "serviceAccount:aou-res-curation-test@appspot.gserviceaccount.com",
                "serviceAccount:circleci-test@aou-res-curation-test.iam.gserviceaccount.com"
            ]
        })
        bucket.set_iam_policy(policy)

        print("Bucket {} created.".format(bucket.name))


def create_test_buckets(config, buckets):
    """
    Create the buckets defined in  the buckets iterable.

    :param config: a dictionary of environment variables and values needed
        to create test  buckets
    :param buckets: an iterable containing strings to use  as bucket names
    """
    for name_id in buckets:
        name = config.get(name_id)
        create_bucket(config, name)


if __name__ == "__main__":
    from ci.test_setup import get_environment_config, BUCKET_NAMES

    config = get_environment_config()
    create_test_buckets(config, BUCKET_NAMES)
