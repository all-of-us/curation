"""
Unit test for delete_stale_test_buckets module
"""

# Python imports
from unittest import TestCase
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime, timedelta, timezone

# Third party imports

# Project imports
from tools import delete_stale_test_buckets


class DeleteStaleTestBucketsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.mock_old_bucket_1 = Mock()
        self.mock_old_bucket_1.name = 'all_of_us_dummy_old_bucket_1'
        self.mock_old_bucket_1.time_created = datetime.now(
            timezone.utc) - timedelta(days=365)

        self.mock_old_bucket_2 = Mock()
        self.mock_old_bucket_2.name = 'all_of_us_dummy_old_bucket_2'
        self.mock_old_bucket_2.time_created = datetime.now(
            timezone.utc) - timedelta(days=180)

        self.mock_new_bucket = Mock()
        self.mock_new_bucket.name = 'all_of_us_dummy_new_bucket'
        self.mock_new_bucket.time_created = datetime.now(
            timezone.utc) - timedelta(days=1)

        self.buckets = [
            self.mock_old_bucket_1, self.mock_old_bucket_2, self.mock_new_bucket
        ]

        self.mock_blob = Mock()
        self.mock_blob.name = 'dummy.csv'

    @patch('tools.delete_stale_test_buckets.StorageClient')
    def test_check_project_error(self, mock_storage_client):
        mock_storage_client.project = 'aou-wrong-project-name'

        with self.assertRaises(ValueError):
            delete_stale_test_buckets._check_project(mock_storage_client)

    @patch('tools.delete_stale_test_buckets.StorageClient')
    def test_filter_stale_buckets_all_not_empty(self, mock_storage_client):
        """Test case: All buckets are NOT empty.
        """
        mock_storage_client.list_buckets.return_value = self.buckets
        mock_storage_client.list_blobs.return_value = [
            self.mock_blob for _ in range(0, 3)
        ]

        result = delete_stale_test_buckets._filter_stale_buckets(
            mock_storage_client, 100)

        self.assertEqual(result, [])

    @patch('tools.delete_stale_test_buckets.StorageClient')
    def test_filter_stale_buckets_all_empty(self, mock_storage_client):
        """Test case: All buckets are empty.
        """
        mock_storage_client.list_buckets.return_value = self.buckets
        mock_storage_client.list_blobs.return_value = []

        result = delete_stale_test_buckets._filter_stale_buckets(
            mock_storage_client, 100)

        self.assertEqual(
            result, [self.mock_old_bucket_1.name, self.mock_old_bucket_2.name])

    @patch('tools.delete_stale_test_buckets.StorageClient')
    def test_filter_stale_buckets_first_n_not_given(self, mock_storage_client):
        """Test case: All buckets are empty. first_n not given.
        """
        mock_storage_client.list_buckets.return_value = self.buckets
        mock_storage_client.list_blobs.return_value = []

        result = delete_stale_test_buckets._filter_stale_buckets(
            mock_storage_client)

        self.assertEqual(
            result, [self.mock_old_bucket_1.name, self.mock_old_bucket_2.name])

    @patch('tools.delete_stale_test_buckets.StorageClient')
    def test_filter_stale_buckets_first_n_given(self, mock_storage_client):
        """Test case: All buckets are empty. first_n given. first_n < # of stale buckets.
        """
        mock_storage_client.list_buckets.return_value = self.buckets
        mock_storage_client.list_blobs.return_value = []

        result = delete_stale_test_buckets._filter_stale_buckets(
            mock_storage_client, 1)

        self.assertEqual(result, [self.mock_old_bucket_1.name])
