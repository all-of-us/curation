import os
import shutil
import tempfile
import unittest
from pathlib import Path

import mock
from google.cloud import storage, bigquery

import app_identity
from common import CONCEPT, VOCABULARY
from tests.test_util import TEST_VOCABULARY_PATH
from tools import load_vocab as lv


class LoadVocabTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.staging_dataset_id = f'{self.dataset_id}_staging'
        self.bucket = os.environ.get('BUCKET_NAME_FAKE')
        self.bq_client = bigquery.Client(project=self.project_id)
        self.gcs_client = storage.Client(project=self.project_id)
        self.test_vocabs = [CONCEPT, VOCABULARY]

        # copy files to temp dir where safe to modify
        self.test_vocab_folder_path = Path(tempfile.mkdtemp())
        for vocab in self.test_vocabs:
            filename = lv._table_name_to_filename(vocab)
            file_path = os.path.join(TEST_VOCABULARY_PATH, filename)
            shutil.copy(file_path, self.test_vocab_folder_path)

        # mock dataset_properties_from_file
        # using the default properties
        dataset = self.bq_client.create_dataset(
            f'{self.project_id}.{self.dataset_id}', exists_ok=True)
        mock_dataset_properties_from_file = mock.patch(
            'tools.load_vocab.dataset_properties_from_file')
        self.mock_bq_query = mock_dataset_properties_from_file.start()
        self.mock_bq_query.return_value = {
            'access_entries': dataset.access_entries
        }
        self.addCleanup(mock_dataset_properties_from_file.stop)

    @mock.patch('tools.load_vocab.VOCABULARY_TABLES', [CONCEPT, VOCABULARY])
    def test_upload_stage(self):
        lv.main(self.project_id, self.bucket, self.test_vocab_folder_path,
                self.dataset_id, 'fake_dataset_props.json')
        expected_row_count = {CONCEPT: 101, VOCABULARY: 52}
        for dataset in [self.staging_dataset_id, self.dataset_id]:
            for vocab in self.test_vocabs:
                content_query = f'SELECT * FROM `{self.project_id}.{dataset}.{vocab}`'
                content_job = self.bq_client.query(content_query)
                rows = content_job.result()
                self.assertEqual(len(list(rows)), expected_row_count[vocab])

    def tearDown(self) -> None:
        # Delete files using a single API request
        bucket = self.gcs_client.bucket(self.bucket)
        blob_names = [lv._table_name_to_filename(t) for t in self.test_vocabs]
        bucket.delete_blobs(blob_names)

        # Drop tables using a single API request
        drop_tables_query = '\n'.join([
            f'''DROP TABLE IF EXISTS `{self.project_id}.{dataset}.{table}`;'''
            for dataset in [self.dataset_id, self.staging_dataset_id]
            for table in self.test_vocabs
        ])
        self.bq_client.query(drop_tables_query).result()

        # remove the temp dir
        shutil.rmtree(self.test_vocab_folder_path)
