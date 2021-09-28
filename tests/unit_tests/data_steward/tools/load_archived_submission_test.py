from unittest import TestCase, mock
from unittest.mock import ANY

from tools import load_archived_submission as ls
import common


class LoadVocabTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project_id'
        self.dataset_id = 'fake_dataset_id'
        self.bucket_name = 'fake_bucket'
        self.hpo_id = 'fake'
        self.bq_client = mock.MagicMock()
        self.gcs_client = mock.MagicMock()
        self.prefix = f'{self.hpo_id}/site_bucket/folder'
        self.tables = [
            common.CONDITION_OCCURRENCE, common.PROCEDURE_OCCURRENCE,
            common.MEASUREMENT, common.OBSERVATION, common.DRUG_EXPOSURE,
            common.SPECIMEN
        ]
        self.files = [f'{self.prefix}/{table}.csv' for table in self.tables]

    def test_filename_to_table_name(self):
        """
        Verify the tablename is extracted correctly
        """
        expected = common.CONDITION_OCCURRENCE
        actual = ls._filename_to_table_name(self.files[0])
        self.assertEqual(expected, actual)
        expected = common.OBSERVATION
        actual = ls._filename_to_table_name(self.files[3])
        self.assertEqual(expected, actual)

    def test_load_folder(self):
        """"
        Verify the load function is called with the right URIs
        """
        blobs = []
        for file in self.files:
            blob = mock.Mock()
            blob.name = file
            blobs.append(blob)
        self.gcs_client.list_blobs.return_value = blobs
        self.bq_client.project = self.project_id
        self.bq_client.create_table = lambda x: x
        job_id = f'{__name__}_12345abcd'
        load_job = mock.MagicMock()
        self.bq_client.load_table_from_uri.return_value = load_job
        load_job.result.return_value = 'None'
        load_job.job_id = job_id
        ls.load_folder(self.dataset_id, self.bq_client, self.bucket_name,
                       self.prefix, self.gcs_client, self.hpo_id)
        self.bq_client.load_table_from_uri.call_args_list = [
            (f'gs://{self.bucket_name}/{file}', ANY, ANY, ANY)
            for file in self.files
        ]
