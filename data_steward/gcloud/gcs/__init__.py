"""
Interact with Google Cloud Storage (GCS)
"""
# Python stl imports
import os

# Project imports
from validation.app_errors import BucketDoesNotExistError
from constants.utils.bq import GET_BUCKET_QUERY, LOOKUP_TABLES_DATASET_ID, HPO_ID_BUCKET_NAME_TABLE_ID
from utils.bq import get_client

# Third-party imports
from google.api_core import page_iterator
from google.cloud.storage.client import Client


class StorageClient(Client):
    """
    A client that extends GCS functionality
    See https://googleapis.dev/python/storage/latest/client.html
    """

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
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

        bq_client = get_client(project_id)

        hpo_bucket_query = GET_BUCKET_QUERY.format(
            project_id=project_id,
            dataset_id=LOOKUP_TABLES_DATASET_ID,
            table_id=HPO_ID_BUCKET_NAME_TABLE_ID,
            hpo_id=hpo_id)

        query_result = bq_client.query(hpo_bucket_query)

        if len(query_result) >= 2:
            raise ValueError(
                f'{len(query_result)} buckets are returned for {hpo_id} '
                f'in {project_id}.{LOOKUP_TABLES_DATASET_ID}.{HPO_ID_BUCKET_NAME_TABLE_ID}.'
            )
        elif len(query_result) == 0:
            raise BucketDoesNotExistError(
                f'No buckets found for {hpo_id} '
                f'in {project_id}.{LOOKUP_TABLES_DATASET_ID}.{HPO_ID_BUCKET_NAME_TABLE_ID}'
            )

        return query_result[0].bucket_name

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
