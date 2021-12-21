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
        self.prefix: str = 'prefix'
        self.data: bytes = b'bytes'

        # NOTE: this needs to be in sorted order
        self.sub_prefixes: tuple = (f'{self.prefix}/a', f'{self.prefix}/b',
                                    f'{self.prefix}/c', f'{self.prefix}/d')
        self.client.empty_bucket(self.bucket_name)
        self._stage_bucket()

    def test_get_bucket_items_metadata(self):

        # get metadata for each item
        items_metadata: list = self.client.get_bucket_items_metadata(
            self.bucket_name)

        # same number of elements
        self.assertEqual(len(items_metadata), len(self.sub_prefixes))

        # metadata name matches an expected name
        sorted_metadata = sorted(items_metadata, key=lambda item: item['name'])
        for index, prefix in enumerate(self.sub_prefixes):
            expect: str = f'{self.bucket_name}/{prefix}/obj.txt'
            actual: str = f'{self.bucket_name}/{sorted_metadata[index]["name"]}'
            self.assertEqual(actual, expect)

    def test_get_blob_metadata(self):

        bucket = self.client.get_bucket(self.bucket_name)
        blob_name: str = f'{self.sub_prefixes[0]}/obj.txt'

        # Bucket.get_blob makes an HTTP request
        # Bucket.blob does not
        blob = bucket.get_blob(blob_name)
        metadata: dict = self.client.get_blob_metadata(blob)

        self.assertEqual(metadata['name'], blob_name)
        self.assertEqual(metadata['size'], len(self.data))

    def test_empty_bucket(self):

        self.client.empty_bucket(self.bucket_name)
        items: list = self.client.list_blobs(self.bucket_name)

        # check that bucket is empty
        self.assertCountEqual(items, [])

    def test_list_sub_prefixes(self):

        items: list = self.client.list_sub_prefixes(self.bucket_name,
                                                    self.prefix)
        sorted_items: list = sorted(items)

        # Check same number of elements
        self.assertEqual(len(self.sub_prefixes), len(sorted_items))

        # Check same prefix
        for index, item in enumerate(sorted_items):
            self.assertEqual(item[:-1], self.sub_prefixes[index])

    def _stage_bucket(self):

        bucket = self.client.bucket(self.bucket_name)
        for sub_prefix in self.sub_prefixes:
            blob = bucket.blob(f'{sub_prefix}/obj.txt')
            blob.upload_from_string(self.data)

    def tearDown(self):
        self.client.empty_bucket(self.bucket_name)
