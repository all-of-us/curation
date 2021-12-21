"""
Test the Google Cloud Storage Client and associated helper functions
"""
# Python stl imports
import os
import unittest

# Project imports
from gcloud.gcs import StorageClient

# Third-party imports


class GcsClientTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.client = StorageClient()
        self.bucket_name: str = os.environ.get('BUCKET_NAME_FAKE')
        self.blob_name: str = 'blob_name'
        self.prefix: str = 'prefix'
        self.data: bytes = b'bytes'

        # NOTE: this needs to be in sorted order
        self.sub_prefixes: tuple = (f'{self.prefix}/a', f'{self.prefix}/b',
                                    f'{self.prefix}/c', f'{self.prefix}/d')
        self.client.empty_bucket(self.bucket_name)

    def test_get_bucket_items_metadata(self):

        self._stage_bucket()
        items_metadata: list = self.client.get_bucket_items_metadata(
            self.bucket_name)

        # same number of elements
        self.assertEqual(len(items_metadata), len(self.sub_prefixes))

        # metadata name matches as expected
        sorted_metadata = sorted(items_metadata, key=lambda item: item['name'])
        for index, prefix in enumerate(self.sub_prefixes):
            expect: str = f'{self.bucket_name}/{prefix}/obj.txt'
            actual: str = f'{self.bucket_name}/{sorted_metadata[index]["name"]}'
            self.assertEqual(actual, expect)

    def test_get_blob_metadata(self):

        bucket = self.client.get_bucket(self.bucket_name)

        blob = bucket.blob(self.blob_name)
        blob.upload_from_string(self.data)
        metadata = self.client.get_blob_metadata(blob)

        self.assertEqual(metadata['name'], self.blob_name)
        self.assertEqual(metadata['size'], len(self.data))

    def test_empty_bucket(self):

        self._stage_bucket()
        self.client.empty_bucket(self.bucket_name)

        actual = self.client.list_blobs(self.bucket_name)
        expected: list = []

        self.assertCountEqual(actual, expected)

    def test_list_sub_prefixes(self):

        self._stage_bucket()

        items = self.client.list_sub_prefixes(self.bucket_name, self.prefix)

        self.assertEqual(len(self.sub_prefixes), len(items))
        for item in items:
            self.assertIn(item[:-1], self.sub_prefixes)

    def _stage_bucket(self):
        bucket = self.client.bucket(self.bucket_name)
        for sub_prefix in self.sub_prefixes:
            blob = bucket.blob(f'{sub_prefix}/obj.txt')
            blob.upload_from_string(self.data)

    def tearDown(self):
        self.client.empty_bucket(self.bucket_name)
