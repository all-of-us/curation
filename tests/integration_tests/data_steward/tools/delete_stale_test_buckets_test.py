"""
Integration test for delete_stale_test_buckets module
"""

# Python imports
import os
from unittest import TestCase
from unittest.mock import patch, Mock
from datetime import datetime, timedelta, timezone

from google.api_core.exceptions import NotFound

# Third party imports

# Project imports
from tools import delete_stale_test_buckets
from gcloud.gcs import StorageClient


class DeleteStaleTestBucketsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.sc = StorageClient()
        self.first_n = 3
        self.now = datetime.now(timezone.utc)

    @patch('google.cloud.storage.bucket.Bucket.delete')
    def test_main(self, mock_delete_bucket):

        buckets_to_delete = delete_stale_test_buckets.main(self.first_n)

        for bucket_name in buckets_to_delete:
            bucket_created = self.sc.get_bucket(bucket_name).time_created

            # Assert: Bucket is stale (1: 90 days or older)
            self.assertGreaterEqual((self.now - bucket_created).days, 90)

            # Assert: Bucket is stale (2: Empty(=no blobs))
            self.assertEqual(len(list(self.sc.list_blobs(bucket_name))), 0)
