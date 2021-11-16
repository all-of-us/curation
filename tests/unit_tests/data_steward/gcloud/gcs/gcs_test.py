# Python imports
from unittest import TestCase
from unittest.mock import patch, MagicMock
from typing import Callable

# Third party imports

# Project imports
from gcloud.gcs import StorageClient


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

        # mock_default_auth = patch('google.auth.default')
        # self.mock_auth = mock_default_auth.start()
        # self.mock_auth.return_value = (sentinel.credentials, 'test-project')
        # self.client = StorageClient().create_anonymous_client()

        self.client = DummyClient()
        self.bucket = 'foo_bucket'
        self.prefix = 'foo_prefix/'
        self.file_name = 'foo_file.csv'

        # self.addCleanup(mock_default_auth.stop)

    @patch.object(DummyClient, 'list_blobs')
    def test_empty_bucket(self, mock_list_blobs):
        mock_blob = MagicMock()
        mock_blob.delete.return_value = None

        self.client.list_blobs.return_value = [mock_blob]
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
