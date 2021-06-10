import datetime
import unittest

import mock
from google.cloud.bigquery import Dataset, DatasetReference, AccessEntry
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
        self.bq_client = mock.MagicMock()
        self.gcs_client = mock.MagicMock()
        self.bucket_name = bucket_name
        self.all_blobs = [
            Blob(f'{table}.csv', self.bucket_name)
            for table in common.VOCABULARY_TABLES
        ]

    def test_get_release_date(self):
        release_date = datetime.date(2021, 2, 10)
        expected = '20210210'
        actual = load_vocab.get_release_date(release_date)
        self.assertEqual(expected, actual)

    def test_load_stage(self):
        # TODO check that extra files are skipped

        # the expected calls to load_table_from_uri
        # are made when all vocabulary files are present
        all_blobs = [
            Blob(f'{table}.csv', self.bucket_name)
            for table in common.VOCABULARY_TABLES
        ]
        self.gcs_client.list_blobs.return_value = all_blobs
        load_vocab.load_stage(self.dst_dataset, self.bq_client,
                              self.bucket_name, self.gcs_client)
        mock_ltfu = self.bq_client.load_table_from_uri
        expected_calls = [(f'gs://{self.bucket_name}/{table}.csv',
                           self.dst_dataset.table(table))
                          for table in common.VOCABULARY_TABLES]
        actual_calls = [(source_uri, destination)
                        for (source_uri,
                             destination), _ in mock_ltfu.call_args_list]
        self.assertListEqual(expected_calls, actual_calls)

        # error is thrown when vocabulary files are missing
        expected_missing = [common.DOMAIN, common.CONCEPT_SYNONYM]
        incomplete_blobs = [
            Blob(f'{table}.csv', self.bucket_name)
            for table in common.VOCABULARY_TABLES
            if table not in expected_missing
        ]
        self.gcs_client.list_blobs.return_value = incomplete_blobs
        expected_msg = f'Bucket {self.bucket_name} is missing files for tables {expected_missing}'
        with self.assertRaises(RuntimeError) as c:
            load_vocab.load_stage(self.dst_dataset, self.bq_client,
                                  self.bucket_name, self.gcs_client)
            self.assertIsInstance(c.exception, RuntimeError)
            self.assertEqual(str(c.exception), expected_msg)

    def test_table_name_to_filename(self):
        expected = 'CONCEPT.csv'
        actual = load_vocab._table_name_to_filename('concept')
        self.assertEqual(expected, actual)

    def test_filename_to_table_name(self):
        expected = 'concept'
        actual = load_vocab._filename_to_table_name('CONCEPT.csv')
        self.assertEqual(expected, actual)

    def test_dataset_properties_from_file(self):
        mock_json = '''{
         "access": [
           { "role": "OWNER", "specialGroup": "projectOwners" },
           { "role": "WRITER", "userByEmail": "fake.person@pmi-ops.org" }
         ]
        }'''

        expected = {
            'access_entries': [
                AccessEntry(
                    role='OWNER',
                    entity_type='specialGroup',
                    entity_id='projectOwners',
                ),
                AccessEntry(
                    role='WRITER',
                    entity_type='userByEmail',
                    entity_id='fake.person@pmi-ops.org',
                )
            ]
        }
        with mock.patch('tools.load_vocab.open',
                        mock.mock_open(read_data=mock_json)):
            actual = load_vocab.dataset_properties_from_file(
                'fake_json_path.json')
        self.assertDictEqual(expected, actual)
