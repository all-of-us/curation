# Python imports
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch, MagicMock
from typing import Callable

# Third party imports

# Project imports
from gcloud.gcs import StorageClient


class GCSTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.client = StorageClient.create_anonymous_client()
        self.bucket = 'foo_bucket'
        self.folder_prefix = 'folder/'
        self.file_name = 'fake_file.csv'
        self.fake_file_obj = BytesIO()

    @patch('gcloud.gcs.page_iterator')
    def test_list_sub_prefixes(self, mock_iterator):

        mock_iterator.HTTPIterator = MagicMock()

        fake_request = 'fake_api_request'
        self.client._connection.api_request = fake_request
        path = f"/b/{self.bucket}/o"
        extra_params = {
            "projection": "noAcl",
            "prefix": self.folder_prefix,
            "delimiter": '/'
        }

        self.client.list_sub_prefixes(self.bucket, self.folder_prefix)
        self.assertEqual(mock_iterator.HTTPIterator.call_count, 1)
        args = mock_iterator.HTTPIterator.call_args[1]
        self.assertEqual(args['client'], self.client)
        self.assertEqual(args['api_request'], fake_request)
        self.assertEqual(args['path'], path)
        self.assertEqual(args['items_key'], 'prefixes')
        self.assertIsInstance(args['item_to_value'], Callable)
        self.assertEqual(args['extra_params'], extra_params)
