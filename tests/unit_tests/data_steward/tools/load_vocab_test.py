import datetime
import unittest

import mock
from google.cloud.bigquery import Dataset, DatasetReference, Client
from google.cloud.storage import Blob

import common
from tools import load_vocab


class LoadVocabTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        project_id = 'fake_project_id'
        dataset_id = 'fake_dataset_id'
        bucket_name = 'fake_bucket'
        dataset_ref = DatasetReference(project_id, dataset_id)
        self.dst_dataset = Dataset(dataset_ref)
        self.bq_client = Client(project_id)
        self.bucket_name = bucket_name

    def test_get_release_date(self):
        release_date = datetime.date(2021, 2, 10)
        expected = '20210210'
        actual = load_vocab.get_release_date(release_date)
        self.assertEqual(expected, actual)

    def test_load_stage(self):
        # TODO check that extra files are skipped
        mock_list_blobs = [
            Blob(f'{table}.csv', self.bucket_name)
            for table in common.VOCABULARY_TABLES
        ]
        with mock.patch.object(load_vocab.storage.Client,
                               'list_blobs',
                               return_value=mock_list_blobs):
            with mock.patch.object(load_vocab.Client,
                                   'load_table_from_uri',
                                   autospec=True) as mock_ltfu:
                load_vocab.load_stage(self.dst_dataset, self.bq_client,
                                      self.bucket_name)
            # the expected calls to load_table_from_uri
            expected_calls = [(f'gs://{self.bucket_name}/{table}.csv',
                               self.dst_dataset.table(table))
                              for table in common.VOCABULARY_TABLES]
            actual_calls = [
                (source_uri, destination)
                for (_self, source_uri,
                     destination), kwargs in mock_ltfu.call_args_list
            ]
            self.assertListEqual(expected_calls, actual_calls)

        # throws error when vocabulary files are missing
        expected_missing = [common.DOMAIN, common.CONCEPT_SYNONYM]
        mock_list_blobs = [
            Blob(f'{table}.csv', self.bucket_name)
            for table in common.VOCABULARY_TABLES
            if table not in expected_missing
        ]
        with mock.patch.object(load_vocab.storage.Client,
                               'list_blobs',
                               return_value=mock_list_blobs):
            expected_msg = f'Bucket {self.bucket_name} is missing files for tables {expected_missing}'
            with self.assertRaises(RuntimeError) as c:
                load_vocab.load_stage(self.dst_dataset, self.bq_client,
                                      self.bucket_name)
                self.assertIsInstance(c.exception, RuntimeError)
                self.assertEqual(str(c.exception), expected_msg)
