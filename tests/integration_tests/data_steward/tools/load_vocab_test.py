# Python imports
import unittest
import os
from pathlib import Path
import csv
import mock

# Project imports
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient
import app_identity
from tests.test_util import TEST_VOCABULARY_PATH
from resources import AOU_VOCAB_CONCEPT_CSV_PATH
from common import CONCEPT, VOCABULARY
from tools import load_vocab as lv


def get_custom_concept_and_vocabulary_counts():
    with open(
            AOU_VOCAB_CONCEPT_CSV_PATH,
            newline='\n',
    ) as f:
        reader = csv.reader(f, delimiter='\t')
        data = list(reader)
    custom_concept_counter = len(data) - 1
    custom_vocabulary_counter = 0
    for row in data:
        # Checks if a concept is custom vocabulary concept by verifying if
        # vocabulary_id and concept_class_id both are "Vocabulary"
        if row[3] == 'Vocabulary' and row[4] == 'Vocabulary':
            custom_vocabulary_counter += 1
    return {
        CONCEPT: custom_concept_counter,
        VOCABULARY: custom_vocabulary_counter
    }


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
        self.bq_client = BigQueryClient(self.project_id)
        self.storage_client = StorageClient(project_id=self.project_id)
        self.test_vocab_folder_path = Path(TEST_VOCABULARY_PATH)
        self.test_vocabs = [CONCEPT, VOCABULARY]
        self.contents = {}
        for vocab in self.test_vocabs:
            vocab_path = self.test_vocab_folder_path / lv._table_name_to_filename(
                vocab)
            with vocab_path.open('r') as f:
                self.contents[vocab] = f.read()

    @mock.patch('tools.load_vocab.VOCABULARY_TABLES', [CONCEPT, VOCABULARY])
    def test_upload_stage(self):
        lv.main(self.project_id, self.bucket, self.test_vocab_folder_path,
                self.dataset_id)
        expected_row_count = get_custom_concept_and_vocabulary_counts()
        for dataset in [self.staging_dataset_id, self.dataset_id]:
            # Custom concept counts check
            content_query = f'SELECT * FROM `{self.project_id}.{dataset}.{CONCEPT}` WHERE concept_id >= 2000000000'
            content_job = self.bq_client.query(content_query)
            rows = content_job.result()
            self.assertEqual(len(list(rows)), expected_row_count[CONCEPT])
            # Custom Vocabulary counts check
            content_query = f'SELECT * FROM `{self.project_id}.{dataset}.{VOCABULARY}` WHERE vocabulary_concept_id >= 2000000000'

            content_job = self.bq_client.query(content_query)
            rows = content_job.result()
            self.assertEqual(len(list(rows)), expected_row_count[VOCABULARY])

    def tearDown(self) -> None:
        for vocab in self.test_vocabs:
            bucket = self.storage_client.bucket(self.bucket)
            blob = bucket.blob(lv._table_name_to_filename(vocab))
            blob.delete()
            self.bq_client.delete_table(f'{self.dataset_id}.{vocab}',
                                        not_found_ok=True)
            self.bq_client.delete_table(f'{self.staging_dataset_id}.{vocab}',
                                        not_found_ok=True)
            self.bq_client.delete_dataset(
                dataset=f'{self.project_id}.{self.staging_dataset_id}',
                delete_contents=True,
                not_found_ok=True)
            vocab_path = self.test_vocab_folder_path / lv._table_name_to_filename(
                vocab)
            with vocab_path.open('w') as f:
                f.write(self.contents[vocab])
