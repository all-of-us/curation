"""
Integration test for GCS bucket retraction.
"""

# Python imports
import os
from io import open
from unittest import TestCase
from unittest.mock import patch

# Project imports
import app_identity
from common import BIGQUERY_DATASET_ID
from tests import test_util
from retraction import retract_data_gcs as rd
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient


class RetractDataGcsTest(TestCase):

    dataset_id = BIGQUERY_DATASET_ID
    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        test_util.setup_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    @patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def setUp(self):
        self.hpo_id = test_util.FAKE_HPO_ID
        self.site_bucket = 'test_bucket'
        self.folder_1 = '2019-01-01-v1/'
        self.folder_2 = '2019-02-02-v2/'
        self.storage_client = StorageClient(self.project_id)
        self.folder_prefix_1 = f'{self.hpo_id}/{self.site_bucket}/{self.folder_1}'
        self.folder_prefix_2 = f'{self.hpo_id}/{self.site_bucket}/{self.folder_2}'
        self.pids = [17, 20]
        self.skip_pids = [10, 25]
        self.project_id = 'project_id'
        self.sandbox_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.pid_table_id = 'pid_table'
        self.content_type = 'text/csv'
        self.gcs_bucket = self.storage_client.get_hpo_bucket(self.hpo_id)
        self.storage_client.empty_bucket(self.gcs_bucket)

    @patch('retraction.retract_data_gcs.extract_pids_from_table')
    def test_integration_five_person_data_retraction_skip(
        self, mock_extract_pids):
        """
        Test for GCS bucket retraction.
        When PIDs to retract are not in the CSV file, no records will be deleted
        from the file.
        """
        mock_extract_pids.return_value = self.skip_pids
        lines_to_remove = {}
        expected_lines_post = {}
        # Exclude note.jsonl until accounted for
        for file_path in test_util.FIVE_PERSONS_FILES[:-1]:
            # generate results files
            file_name = file_path.split('/')[-1]
            lines_to_remove[file_name] = 0
            with open(file_path, 'rb') as f:
                # skip header
                next(f)
                expected_lines_post[file_name] = []
                for line in f:
                    line = line.strip()
                    if line != b'':
                        expected_lines_post[file_name].append(line)

                # write file to cloud for testing
                blob = self.gcs_bucket.blob(
                    f'{self.folder_prefix_1}{file_name}')
                blob.upload_from_file(f,
                                      rewind=True,
                                      content_type=self.content_type)
                blob = self.gcs_bucket.blob(
                    f'{self.folder_prefix_2}{file_name}')
                blob.upload_from_file(f,
                                      rewind=True,
                                      content_type=self.content_type)

        rd.run_gcs_retraction(self.project_id,
                              self.sandbox_dataset_id,
                              self.pid_table_id,
                              self.hpo_id,
                              folder='all_folders',
                              force_flag=True,
                              bucket=self.gcs_bucket,
                              site_bucket=self.site_bucket)

        total_lines_post = {}
        # Exclude note.jsonl until accounted for
        for file_path in test_util.FIVE_PERSONS_FILES[:-1]:
            file_name = file_path.split('/')[-1]
            blob = self.gcs_bucket.blob(f'{self.folder_prefix_1}{file_name}')

            actual_result = blob.download_as_string()
            if b'\r\n' in actual_result:
                actual_result_contents = actual_result.split(b'\r\n')
            else:
                actual_result_contents = actual_result.split(b'\n')

            # convert to list and remove header and last list item since it is a newline
            total_lines_post[file_name] = actual_result_contents[1:-1]

        for key in expected_lines_post:
            self.assertEqual(lines_to_remove[key], 0)
            self.assertListEqual(expected_lines_post[key],
                                 total_lines_post[key])

    @patch('retraction.retract_data_gcs.extract_pids_from_table')
    def test_integration_five_person_data_retraction(self, mock_extract_pids):
        """
        Test for GCS bucket retraction.
        When PIDs to retract are in the CSV file, those records will be deleted
        from the file.
        """
        mock_extract_pids.return_value = self.pids
        expected_lines_post = {}
        # Exclude note.jsonl until accounted for
        for file_path in test_util.FIVE_PERSONS_FILES[:-1]:
            # generate results files
            file_name = file_path.split('/')[-1]
            table_name = file_name.split('.')[0]
            expected_lines_post[file_name] = []
            with open(file_path, 'rb') as f:
                # skip header
                next(f)
                expected_lines_post[file_name] = []
                for line in f:
                    line = line.strip()
                    if line != b'':
                        if not ((table_name in rd.PID_IN_COL1 and
                                 int(line.split(b",")[0]) in self.pids) or
                                (table_name in rd.PID_IN_COL2 and
                                 int(line.split(b",")[1]) in self.pids)):
                            expected_lines_post[file_name].append(line)

                # write file to cloud for testing
                blob = self.gcs_bucket.blob(
                    f'{self.folder_prefix_1}{file_name}')
                blob.upload_from_file(f,
                                      rewind=True,
                                      content_type=self.content_type)
                blob = self.gcs_bucket.blob(
                    f'{self.folder_prefix_2}{file_name}')
                blob.upload_from_file(f,
                                      rewind=True,
                                      content_type=self.content_type)

        rd.run_gcs_retraction(self.project_id,
                              self.sandbox_dataset_id,
                              self.pid_table_id,
                              self.hpo_id,
                              folder='all_folders',
                              force_flag=True,
                              bucket=self.gcs_bucket,
                              site_bucket=self.site_bucket)

        total_lines_post = {}
        # Exclude note.jsonl until accounted for
        for file_path in test_util.FIVE_PERSONS_FILES[:-1]:
            file_name = file_path.split('/')[-1]
            blob = self.gcs_bucket.blob(f'{self.folder_prefix_1}{file_name}')
            actual_result_contents = blob.download_as_string().split(b'\n')
            # convert to list and remove header and last list item since it is a newline
            total_lines_post[file_name] = actual_result_contents[1:-1]

        for key in expected_lines_post:
            self.assertListEqual(expected_lines_post[key],
                                 total_lines_post[key])

    def tearDown(self):
        self.storage_client.empty_bucket(self.gcs_bucket)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)
