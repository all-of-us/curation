"""
Interact with Google Cloud Storage (GCS)
"""
# Python stl imports
import os
import json

# Project imports
from validation.app_errors import BucketDoesNotExistError

# Third-party imports
import google.auth
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket
from google.auth.transport.requests import AuthorizedSession
from google.api_core import page_iterator


class StorageClient(Client):
    """
    A client that extends GCS functionality
    See https://googleapis.dev/python/storage/latest/client.html
    """

    def get_items_metadata(self, bucket: Bucket) -> list:
        """
        Get metadata for each object within a bucket
        :param bucket: Bucket to inspect
        :return: list of dicts with storage objects' metadata
        """

        bucket_name: str = bucket.name
        scopes: list = ['https://www.googleapis.com/auth/devstorage.read_only']
        url: str = f'https://storage.googleapis.com/storage/v1/b/{bucket_name}/o'

        credentials, _ = google.auth.default(scopes=scopes)
        authed_session = AuthorizedSession(credentials)
        response = authed_session.request('GET', url)
        result: dict = json.loads(response.content)
        return result['items']

    def get_drc_bucket(self) -> str:
        result = os.environ.get('DRC_BUCKET_NAME')
        return result

    def get_hpo_bucket(self, hpo_id: str) -> str:
        """
        Get the name of an HPO site's private bucket

        Empty/unset bucket indicates that the bucket is intentionally left blank and can be ignored
        :param hpo_id: id of the HPO site
        :return: name of the bucket
        """
        # TODO reconsider how to map bucket name
        bucket_env = 'BUCKET_NAME_' + hpo_id.upper()
        hpo_bucket_name = os.getenv(bucket_env)

        # App engine converts an env var set but left empty to be the string 'None'
        if not hpo_bucket_name or hpo_bucket_name.lower() == 'none':
            # should not use hpo_id in message if sent to end user.  For now,
            # only sent to alert messages slack channel.
            raise BucketDoesNotExistError(
                f"Failed to fetch bucket '{hpo_bucket_name}' for hpo_id '{hpo_id}'",
                hpo_bucket_name)
        return hpo_bucket_name

    def empty_bucket(self, bucket: str, **kwargs) -> None:
        """
        Delete all blobs in a bucket.
        :param name: A GCS bucket name.

        Some common keyword arguments:
        :param prefix: (Optional) Prefix used to filter blobs.
        (i.e gsutil rm -r gs://bucket/prefix/)
        """

        pages = self.list_blobs(bucket, **kwargs).pages
        for page in pages:
            for blob in page:
                blob.delete()

    def list_sub_prefixes(self, bucket: str, prefix: str) -> None:
        """
        List sub folders in folder specified by prefix

        SO link: https://stackoverflow.com/a/59008580
        :param bucket: GCS bucket name as string
        :param prefix: path to directory to look into e.g. a/b/c/
        :return: list of strings of sub-directories e.g. [a/b/c/v1/, a/b/c/v2/]
        """
        if not prefix.endswith('/'):
            prefix += '/'

        extra_params: dict = {
            'projection': 'noAcl',
            'prefix': prefix,
            'delimiter': '/'
        }

        path: str = f'/b/{bucket}/o'

        pg_iterator = page_iterator.HTTPIterator(
            client=self,
            api_request=self._connection.api_request,
            path=path,
            items_key='prefixes',
            item_to_value=lambda _, x: x,
            extra_params=extra_params,
        )
        return list(pg_iterator)
