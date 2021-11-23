"""
Unit test for delete_stale_test_buckets module
"""

# Python imports
import unittest
from datetime import datetime, timedelta, timezone

# Third party imports
from mock import patch, Mock

# Project imports
from tools import delete_stale_test_buckets


class DeleteStaleTestBucketsTest(unittest.TestCase):

    @patch('gcloud.gcs.StorageClient')
    @patch('gcloud.gcs.StorageClient.list_blobs')
    def test_filter_stale_buckets(self, list_blobs_mock, client_mock):

        old_bucket_mock_1 = Mock()
        old_bucket_mock_1.name = 'all_of_us_dummy_old_bucket_1'
        old_bucket_mock_1.time_created = datetime.now(
            timezone.utc) - timedelta(days=365)

        old_bucket_mock_2 = Mock()
        old_bucket_mock_2.name = 'all_of_us_dummy_old_bucket_2'
        old_bucket_mock_2.time_created = datetime.now(
            timezone.utc) - timedelta(days=180)

        new_bucket_mock = Mock()
        new_bucket_mock.name = 'all_of_us_dummy_new_bucket'
        new_bucket_mock.time_created = datetime.now(
            timezone.utc) - timedelta(days=1)

        buckets = [old_bucket_mock_1, old_bucket_mock_2, new_bucket_mock]

        # Test case 1 ... All buckets are not empty.
        blob_mock = Mock()
        blob_mock.name = 'dummy.csv'
        list_blobs_mock.return_value = iter([blob_mock for _ in range(0, 3)])

        result = delete_stale_test_buckets._filter_stale_buckets(buckets, 100)

        self.assertEqual(result, [])

        # Test case 2 ... All buckets are empty.
        list_blobs_mock.return_value = iter(())

        result = delete_stale_test_buckets._filter_stale_buckets(buckets, 100)

        self.assertEqual(result,
                         [old_bucket_mock_1.name, old_bucket_mock_2.name])

        # Test case 3 ... All buckets are empty. first_n not given.
        result = delete_stale_test_buckets._filter_stale_buckets(buckets)

        self.assertEqual(result,
                         [old_bucket_mock_1.name, old_bucket_mock_2.name])

        # Test case 4 ... All buckets are empty. first_n < # of stale buckets
        result = delete_stale_test_buckets._filter_stale_buckets(buckets, 1)

        self.assertEqual(result, [old_bucket_mock_1.name])
