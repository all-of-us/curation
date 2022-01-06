"""
Interact with Google Cloud Storage (GCS)
"""
# Python stl imports
import os

from google.cloud.storage.bucket import Bucket, Blob

# Project imports
from validation.app_errors import BucketDoesNotExistError

# Third-party imports
from google.api_core import page_iterator
from google.cloud.storage.client import Client


class StorageClient(Client):
    """
    A client that extends GCS functionality
    See https://googleapis.dev/python/storage/latest/client.html
    """

    def get_bucket_items_metadata(self, bucket: Bucket) -> list:
        """
        Given a bucket, iterate through it's contents and pull out each objects
        metadata
        :param bucket: Bucket to iterate through
        :return: a list of dicts containing metadata
        """

        blobs: list = list(self.list_blobs(bucket))
        metadata: list = [self.get_blob_metadata(blob) for blob in blobs]
        return metadata

    def get_blob_metadata(self, blob: Blob) -> dict:
        """
        Gather and ship a blob's metadata in dictionary form.
        Note: Date and times are in the python stl format.  They are datetime objects.
        """

        if blob.id is None:
            # Bucket.get_blob() makes an HTTP request, thus we check if we need to
            blob = self.get_bucket(blob.bucket.name).get_blob(blob.name)

        metadata: dict = {
            'id': blob.id,
            'name': blob.name,
            'bucket': blob.bucket.name,
            'generation': blob.generation,
            'metageneration': blob.metageneration,
            'contentType': blob.content_type,
            'storageClass': blob.storage_class,
            'size': blob.size,
            'md5Hash': blob.md5_hash,
            'crc32c': blob.crc32c,
            'etag': blob.etag,
            'updated': blob.updated,
            'timeCreated': blob.time_created
        }
        return metadata

    def get_drc_bucket(self) -> Bucket:
        return self.get_bucket(os.environ.get('DRC_BUCKET_NAME'))

    def get_hpo_bucket(self, hpo_site: str) -> Bucket:
        """
        Get the name of an HPO site's private bucket

        Empty/unset bucket indicates that the bucket is intentionally left blank and can be ignored
        :param hpo_id: id of the HPO site
        :return: name of the bucket
        """
        # TODO reconsider how to map bucket name
        bucket_name: str = self._get_hpo_bucket_id(hpo_site)

        # App engine converts an env var set but left empty to be the string 'None'
        if not bucket_name or bucket_name.lower() == 'none':
            # should not use hpo_id in message if sent to end user.  If the
            # error is logged as a WARNING or higher, this will trigger a
            # GCP alert.
            raise BucketDoesNotExistError(
                f"Failed to fetch bucket '{bucket_name}' for hpo '{hpo_site}'",
                bucket_name)
        return self.get_bucket(bucket_name)

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

    def list_sub_prefixes(self, bucket_name: str, prefix: str) -> list:
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

        path: str = f'/b/{bucket_name}/o'

        pg_iterator = page_iterator.HTTPIterator(
            client=self,
            api_request=self._connection.api_request,
            path=path,
            items_key='prefixes',
            item_to_value=lambda _, x: x,
            extra_params=extra_params,
        )
        return list(pg_iterator)

    def _get_hpo_bucket_id(self, hpo_id: str) -> str:
        return os.environ.get(f'BUCKET_NAME_{hpo_id.upper()}')
