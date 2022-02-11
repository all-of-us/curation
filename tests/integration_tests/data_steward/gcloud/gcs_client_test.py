"""
Test the Google Cloud Storage Client and associated helper functions
"""
# Python stl imports
import os
import unittest
from unittest.mock import patch

# Project imports
import app_identity
from gcloud.gcs import StorageClient
from validation.app_errors import BucketDoesNotExistError


class GcsClientTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.storage_client = StorageClient(self.project_id)
        self.bucket_name: str = os.environ.get('BUCKET_NAME_FAKE')
        self.prefix: str = 'prefix'
        self.data: bytes = b'bytes'

        # NOTE: this needs to be in sorted order
        self.sub_prefixes: tuple = (f'{self.prefix}/a', f'{self.prefix}/b',
                                    f'{self.prefix}/c', f'{self.prefix}/d')
        self.storage_client.empty_bucket(self.bucket_name)
        self._stage_bucket()

    @patch.object(StorageClient, '_get_hpo_bucket_id')
    def test_get_hpo_bucket_not_found(self, mock_get_bucket_id):
        fake_hpo_id = 'fake_hpo_id'
        fake_bucket_name = 'fake_bucket_name'
        mock_get_bucket_id.return_value = fake_bucket_name
        expected_message = f"Failed to acquire bucket '{fake_bucket_name}' for hpo '{fake_hpo_id}'"

        with self.assertRaises(BucketDoesNotExistError) as e:
            self.storage_client.get_hpo_bucket(fake_hpo_id)
        self.assertEqual(e.exception.message, expected_message)

    def test_get_bucket_items_metadata(self):

        items_metadata: list = self.storage_client.get_bucket_items_metadata(
            self.bucket_name)

        actual_metadata: list = [item['name'] for item in items_metadata]
        expected_metadata: list = [
            f'{prefix}/obj.txt' for prefix in self.sub_prefixes
        ]

        self.assertCountEqual(actual_metadata, expected_metadata)
        self.assertIsNotNone(items_metadata[0]['id'])

    def test_get_blob_metadata(self):

        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob_name: str = f'{self.sub_prefixes[0]}/obj.txt'

        blob = bucket.blob(blob_name)
        metadata: dict = self.storage_client.get_blob_metadata(blob)

        self.assertIsNotNone(metadata['id'])
        self.assertIsNotNone(metadata['name'])
        self.assertIsNotNone(metadata['bucket'])
        self.assertIsNotNone(metadata['generation'])
        self.assertIsNotNone(metadata['metageneration'])
        self.assertIsNotNone(metadata['contentType'])
        self.assertIsNotNone(metadata['storageClass'])
        self.assertIsNotNone(metadata['size'])
        self.assertIsNotNone(metadata['md5Hash'])
        self.assertIsNotNone(metadata['crc32c'])
        self.assertIsNotNone(metadata['etag'])
        self.assertIsNotNone(metadata['updated'])
        self.assertIsNotNone(metadata['timeCreated'])

        self.assertEqual(metadata['name'], blob_name)
        self.assertEqual(metadata['size'], len(self.data))

    def test_empty_bucket(self):

        self.storage_client.empty_bucket(self.bucket_name)
        items: list = self.storage_client.list_blobs(self.bucket_name)

        # check that bucket is empty
        self.assertCountEqual(items, [])

    def test_list_sub_prefixes(self):

        items: list = self.storage_client.list_sub_prefixes(
            self.bucket_name, self.prefix)

        # Check same number of elements
        self.assertEqual(len(self.sub_prefixes), len(items))

        # Check same prefix
        for index, item in enumerate(items):
            self.assertEqual(item[:-1], self.sub_prefixes[index])

    def _stage_bucket(self):

        bucket = self.storage_client.bucket(self.bucket_name)
        for sub_prefix in self.sub_prefixes:
            blob = bucket.blob(f'{sub_prefix}/obj.txt')
            blob.upload_from_string(self.data)

    def tearDown(self):
        self.storage_client.empty_bucket(self.bucket_name)
