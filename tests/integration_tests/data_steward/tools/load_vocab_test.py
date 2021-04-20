import unittest
import os
from pathlib import Path
import mock

from google.cloud import storage, bigquery

import app_identity
from tests.test_util import TEST_VOCABULARY_PATH
from common import CONCEPT, VOCABULARY
from utils import auth
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
        self.sandbox_dataset_id = f'{self.dataset_id}_sandbox'
        self.bucket = os.environ.get('BUCKET_NAME_FAKE')
        self.account = os.environ.get('SERVICE_ACCOUNT')
        impersonation_credentials = auth.get_impersonation_credentials(
            self.account, lv.SCOPES)
        self.bq_client = bigquery.Client(project=self.project_id,
                                         credentials=impersonation_credentials)
        self.gcs_client = storage.Client(project=self.project_id,
                                         credentials=impersonation_credentials)
        self.test_vocab_folder_path = Path(TEST_VOCABULARY_PATH)
        self.test_vocabs = [CONCEPT, VOCABULARY]
        self.contents = {}
        for vocab in self.test_vocabs:
            vocab_path = self.test_vocab_folder_path / f'{vocab}.csv'
            with vocab_path.open('r') as f:
                self.contents[vocab] = f.read()

    @mock.patch('tools.load_vocab.VOCABULARY_TABLES', [CONCEPT, VOCABULARY])
    def test_upload_stage(self):
        lv.main(self.project_id, self.bucket, self.test_vocab_folder_path,
                self.account, self.dataset_id)
        expected_row_count = {'concept': 101, 'vocabulary': 52}
        for dataset in [self.sandbox_dataset_id, self.dataset_id]:
            for vocab in self.test_vocabs:
                content_query = f'SELECT * FROM `{self.project_id}.{dataset}.{vocab}`'
                content_job = self.bq_client.query(content_query)
                rows = content_job.result()
                self.assertEqual(len(list(rows)), expected_row_count[vocab])

    def tearDown(self) -> None:
        for vocab in self.test_vocabs:
            bucket = self.gcs_client.bucket(self.bucket)
            blob = bucket.blob(f'{vocab}.csv')
            blob.delete()
            self.bq_client.delete_table(f'{self.dataset_id}.{vocab}')
            self.bq_client.delete_table(f'{self.sandbox_dataset_id}.{vocab}')
            vocab_path = self.test_vocab_folder_path / f'{vocab}.csv'
            with vocab_path.open('w') as f:
                f.write(self.contents[vocab])
