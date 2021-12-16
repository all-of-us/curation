# Python imports
import json
from unittest import TestCase
from unittest.mock import patch, MagicMock, PropertyMock
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
        self.client = DummyClient()
        self.bucket = 'foo_bucket'
        self.prefix = 'foo_prefix/'
        self.file_name = 'foo_file.csv'

    @patch('google.auth.default')
    @patch('gcloud.gcs.AuthorizedSession')
    def test_get_items_metadata(self, mock_authed_session, mock_auth_default):

        mock_bucket = MagicMock()
        mock_bucket.name = 'mock_bucket_name'

        mock_credentials, _ = 'mock_credentials', None
        mock_auth_default.return_value = (mock_credentials, None)

        expected: dict = {'items': [{'metakey': 'metavalue'}]}
        return_value: str = json.dumps(expected)

        mock_response = MagicMock()
        type(mock_response).content = PropertyMock(return_value=return_value)

        mocked_session = mock_authed_session(mock_credentials)
        mocked_session.request.return_value = mock_response

        actual = self.client.get_items_metadata(mock_bucket)

        mock_authed_session.assert_called_with(mock_credentials)
        self.assertEqual(actual, expected['items'])

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
