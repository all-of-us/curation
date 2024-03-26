"""
Interact with Google Cloud Storage (GCS)
"""
# Python stl imports
import logging
import os
from typing import Union

# Third-party imports
from google.api_core import page_iterator
from google.cloud.exceptions import NotFound
from google.auth import default
from google.cloud.storage.bucket import Bucket, Blob
from google.cloud.storage.client import Client
from pandas.core.frame import DataFrame

# Project imports
from common import JINJA_ENV
from constants.utils.bq import SELECT_BUCKET_NAME_QUERY, LOOKUP_TABLES_DATASET_ID, HPO_ID_BUCKET_NAME_TABLE_ID
from utils import auth
from utils.bq import query
from validation.app_errors import BucketDoesNotExistError, BucketNotSet


class StorageClient(Client):
    """
    A client that extends GCS functionality
    See https://googleapis.dev/python/storage/latest/client.html
    """

    def __init__(self, project_id: str, scopes=None, credentials=None):
        """
        Get a storage client for a specified project.

        :param project_id: Identifies the project to create a cloud storage client for
        :param scopes: List of Google scopes as strings
        :param credentials: Google credentials object (ignored if scopes is defined,
            uses delegated credentials instead)

        :return:  A StorageClient instance
        """
        if scopes:
            credentials, project_id = default()
            credentials = auth.delegated_credentials(credentials, scopes=scopes)

        super().__init__(project=project_id, credentials=credentials)

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
            blob = self.bucket(blob.bucket.name).get_blob(blob.name)

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
        return self.bucket(os.environ.get('DRC_BUCKET_NAME'))

    def get_hpo_bucket(self, hpo_id: str) -> Bucket:
        """
        Get the name of an HPO site's private bucket
        Empty/unset bucket indicates that the bucket is intentionally left blank and can be ignored
        :param hpo_id: id of the HPO site
        :return: bucket
        """
        bucket_name: str = self._get_hpo_bucket_id(hpo_id)

        # App engine converts an env var set but left empty to be the string 'None'
        if not bucket_name or bucket_name.lower() == 'none':
            # should not use hpo_id in message if sent to end user.  If the
            # error is logged as a WARNING or higher, this will trigger a
            # GCP alert.
            raise BucketNotSet(
                f"Bucket '{bucket_name}' for hpo '{hpo_id}' is unset/empty, "
                f"or it has multiple records in the lookup table")

        try:
            bucket = self.bucket(bucket_name)
            # this call is only to verify that the bucket can be accessed, the results are not important
            self.get_bucket_items_metadata(bucket)
        except NotFound:
            raise BucketDoesNotExistError(
                f"Failed to acquire bucket '{bucket_name}' for hpo '{hpo_id}'",
                bucket_name)

        return bucket

    def empty_bucket(self, bucket: Union[Bucket, str], **kwargs) -> None:
        """
        Delete all blobs in a bucket.
        :param bucket: A GCS bucket, as a Bucket object or a string.
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
        """
        Get the name of an HPO site's private bucket.
        :param hpo_id: id of the HPO site
        :return: name of the bucket, or str 'None' if (1) no matching record is found
        or (2) multiple records are found in the lookup table.
        """
        service = os.environ.get('GAE_SERVICE', 'default')

        hpo_bucket_query = JINJA_ENV.from_string(
            SELECT_BUCKET_NAME_QUERY).render(
                project_Id=self.project,
                dataset_id=LOOKUP_TABLES_DATASET_ID,
                table_id=HPO_ID_BUCKET_NAME_TABLE_ID,
                hpo_id=hpo_id,
                service=service)

        result_df: DataFrame = query(hpo_bucket_query)

        if result_df['bucket_name'].count() != 1:
            return 'None'

        return result_df['bucket_name'].iloc[0]

    def copy_file(self, src_bucket: Bucket, dest_bucket: Bucket, src_path: str,
                  dest_path: str):
        """
        Copy a file from one bucket to another. Blob.rewrite is used here
        instead of Bucket.copy_blob to avoid timeout error.
        :param src_bucket: Bucket where the file to copy is in
        :param dest_bucket: Bucket where to copy the file to
        :param src_path: Full path of the source file
        :param dest_path: Full path of the target file
        """
        src_blob = src_bucket.get_blob(src_path)
        dest_blob = dest_bucket.blob(dest_path)

        rewrite_token = False
        while True:
            rewrite_token, bytes_rewritten, bytes_to_rewrite = dest_blob.rewrite(
                src_blob, token=rewrite_token)
            logging.info(
                f"{dest_path}: Copied: {bytes_rewritten}/{bytes_to_rewrite} bytes."
            )

            if not rewrite_token:
                break
