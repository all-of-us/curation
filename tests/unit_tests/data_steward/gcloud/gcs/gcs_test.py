# Python imports
import os
from unittest import TestCase
from unittest.mock import patch, MagicMock
from typing import Callable

# Third party imports

# Project imports
from gcloud.gcs import StorageClient
from validation.app_errors import BucketNotSet


class DummyClient(StorageClient):
    """
    A class which inherits all of StorageClient but doesn't authenticate
    """

    # pylint: disable=super-init-not-called
    def __init__(self):
        pass


class GCSTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.client = DummyClient()
        self.bucket: str = 'foo_bucket'
        self.prefix: str = 'foo_prefix/'
        self.file_name: str = 'foo_file.csv'
        self.hpo_id = 'fake_hpo_id'

    @patch('gcloud.gcs.StorageClient._get_hpo_bucket_id')
    def test_get_hpo_bucket_not_set(self, mock_get_hpo_bucket_id):
        mock_get_hpo_bucket_id.side_effect = [None, '', 'None']
        expected_message = lambda bucket: f"Bucket '{bucket}' for hpo '{self.hpo_id}' is unset/empty"

        # Test case 1 ... _get_hpo_bucket() returns None
        with self.assertRaises(BucketNotSet) as e:
            self.client.get_hpo_bucket(self.hpo_id)
        self.assertEqual(e.exception.message, expected_message(None))

        # Test case 2 ... _get_hpo_bucket() returns ''
        with self.assertRaises(BucketNotSet) as e:
            self.client.get_hpo_bucket(self.hpo_id)
        self.assertEqual(e.exception.message, expected_message(''))

        # Test case 3 ... _get_hpo_bucket() returns 'None'
        with self.assertRaises(BucketNotSet) as e:
            self.client.get_hpo_bucket(self.hpo_id)
        self.assertEqual(e.exception.message, expected_message('None'))

    @patch('google.cloud.storage.bucket.Bucket')
    @patch.object(DummyClient, 'list_blobs')
    def test_get_bucket_items_metadata(self, mock_list_blobs, mock_bucket):

        mock_blob = MagicMock()
        mock_blob.name = 'foo_name'
        mock_blob.bucket.name = self.bucket

        expected_items: list = [mock_blob]
        mock_list_blobs.return_value = expected_items
        actual_items: list = self.client.get_bucket_items_metadata(mock_bucket)
        actual_metadata: dict = actual_items[0]

        self.assertEqual(len(expected_items), len(actual_items))
        self.assertEqual(actual_metadata['name'], mock_blob.name)
        self.assertEqual(actual_metadata['bucket'], mock_blob.bucket.name)

    @patch('google.cloud.storage.bucket.Blob')
    def test_get_blob_metadata(self, mock_blob):
        mock_blob.name = 'foo_name'
        mock_blob.bucket.name = self.bucket

        metadata = self.client.get_blob_metadata(mock_blob)

        self.assertIn('name', metadata)
        self.assertEqual(mock_blob.name, metadata['name'])
        self.assertIn('bucket', metadata)
        self.assertEqual(mock_blob.bucket.name, metadata['bucket'])

    @patch.object(DummyClient, 'list_blobs')
    def test_empty_bucket(self, mock_list_blobs):
        # Mock up blobs
        mock_blob = MagicMock()
        mock_blob.delete.return_value = None
        # Mock up pages
        mock_pages = MagicMock()
        mock_pages.pages = [[mock_blob]]
        # Mock page returning list funciton
        self.client.list_blobs.return_value = mock_pages
        # Test
        self.client.empty_bucket(self.bucket)
        mock_blob.delete.assert_called_once()

    @patch('gcloud.gcs.page_iterator')
    @patch.object(DummyClient, '_connection')
    def test_list_sub_prefixes(self, mock_connection, mock_iterator):

        foo_request: str = 'fake_api_request'
        path: str = f"/b/{self.bucket}/o"
        extra_params: dict = {
            "projection": "noAcl",
            "prefix": self.prefix,
            "delimiter": '/'
        }

        mock_iterator.HTTPIterator = MagicMock()
        self.client._connection.return_value = MagicMock()
        self.client._connection.api_request = foo_request

        self.client.list_sub_prefixes(self.bucket, self.prefix)
        self.assertEqual(mock_iterator.HTTPIterator.call_count, 1)
        args = mock_iterator.HTTPIterator.call_args[1]
        self.assertEqual(args['client'], self.client)
        self.assertEqual(args['api_request'], foo_request)
        self.assertEqual(args['path'], path)
        self.assertEqual(args['items_key'], 'prefixes')
        self.assertIsInstance(args['item_to_value'], Callable)
        self.assertEqual(args['extra_params'], extra_params)
