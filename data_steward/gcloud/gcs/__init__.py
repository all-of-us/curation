"""
Interact with Google Cloud Storage (GCS)
"""
# Python stl imports

# Project imports

# Third-party imports
from google.api_core import page_iterator
from google.cloud.storage.client import Client


class StorageClient(Client):
    """
    A client that extends GCS functionality
    See https://googleapis.dev/python/storage/latest/client.html
    """

    def empty_bucket(self, bucket: str, **kwargs) -> None:
        """
        Delete all blobs in a bucket.
        :param name: A GCS bucket name.

        Some common keyword arguments:
        :param prefix: (Optional) Prefix used to filter blobs.
        (i.e gsutil rm -r gs://bucket/prefix/)
        """

        blobs = self.list_blobs(bucket, **kwargs)
        for blob in blobs:
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
