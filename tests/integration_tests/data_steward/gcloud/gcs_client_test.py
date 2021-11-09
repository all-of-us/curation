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

        self.sub_prefixes: tuple = (f'{self.prefix}/a', f'{self.prefix}/b',
                                    f'{self.prefix}/c', f'{self.prefix}/d')

    def test_list_sub_prefixes(self):
        self._empty_bucket()
        self._stage_bucket()

        items = self.client.list_sub_prefixes(self.bucket_name, self.prefix)
        print(items)

        self.assertEqual(len(self.sub_prefixes), len(items))
        for item in items:
            self.assertIn(item[:-1], self.sub_prefixes)

        self._empty_bucket()

    def _empty_bucket(self):
        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.prefix)
        for blob in blobs:
            blob.delete()

    def _stage_bucket(self):
        bucket = self.client.bucket(self.bucket_name)
        for sub_prefix in self.sub_prefixes:
            bucket.blob(f'{sub_prefix}/obj.txt').upload_from_string(self.data)

    def tearDown(self):
        self._empty_bucket()
