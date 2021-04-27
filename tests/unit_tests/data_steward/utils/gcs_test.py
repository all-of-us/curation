# Python imports
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch, MagicMock
from typing import Callable

# Third party imports

# Project imports
from utils import gcs


class GCSTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.client = MagicMock()
        self.bucket = 'foo_bucket'
        self.bucket_obj = MagicMock(return_value=self.bucket)
        self.client.bucket.return_value = self.bucket_obj
        self.folder_prefix = 'folder/'
        self.file_name = 'fake_file.csv'
        self.fake_file_obj = BytesIO()

    @patch('utils.gcs.page_iterator')
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

        gcs.list_sub_prefixes(self.client, self.bucket, self.folder_prefix)
        assert mock_iterator.HTTPIterator.call_count == 1
        args = mock_iterator.HTTPIterator.call_args[1]
        self.assertEqual(args['client'], self.client)
        self.assertEqual(args['api_request'], fake_request)
        self.assertEqual(args['path'], path)
        self.assertEqual(args['items_key'], 'prefixes')
        self.assertIsInstance(args['item_to_value'], Callable)
        self.assertEqual(args['extra_params'], extra_params)

    @patch('utils.gcs.BytesIO')
    def test_retrieve_file_contents_as_list(self, mock_call_file_obj):
        fake_lines = [None]
        fake_file_obj = MagicMock()
        mock_call_file_obj.return_value = fake_file_obj
        fake_file_obj.seek = MagicMock()
        fake_file_obj.readlines.return_value = fake_lines

        blob_obj = MagicMock()
        self.bucket_obj.blob.return_value = blob_obj
        blob_obj.download_to_file = MagicMock()
        contents = gcs.retrieve_file_contents_as_list(self.client, self.bucket,
                                                      self.folder_prefix,
                                                      self.file_name)
        blob_obj.download_to_file.assert_called_once_with(fake_file_obj)
        self.assertEqual(contents, fake_lines)

    def test_upload_file_to_gcs(self):
        rewind = True
        content_type = 'text/csv'
        blob_obj = MagicMock()
        self.bucket_obj.blob.return_value = blob_obj
        blob_obj.upload_from_file = MagicMock()
        gcs.upload_file_to_gcs(self.client, self.bucket, self.folder_prefix,
                               self.file_name, self.fake_file_obj, rewind,
                               content_type)
        blob_obj.upload_from_file.assert_called_once_with(
            self.fake_file_obj, rewind=rewind, content_type=content_type)
