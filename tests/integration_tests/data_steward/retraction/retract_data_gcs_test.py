# Python imports
import os
from io import open
from unittest import TestCase
from unittest.mock import patch

# Third party imports
from google.cloud import storage

# Project imports
import app_identity
from tests import test_util
from retraction import retract_data_gcs as rd


class RetractDataGcsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.hpo_id = test_util.FAKE_HPO_ID
        self.bucket = os.environ.get(f'BUCKET_NAME_FAKE')
        self.site_bucket = 'test_bucket'
        self.folder_1 = '2019-01-01-v1/'
        self.folder_2 = '2019-02-02-v2/'
        self.client = storage.Client(self.project_id)
        self.folder_prefix_1 = self.hpo_id + '/' + self.site_bucket + '/' + self.folder_1
        self.folder_prefix_2 = self.hpo_id + '/' + self.site_bucket + '/' + self.folder_2
        self.pids = [17, 20]
        self.skip_pids = [10, 25]
        self.project_id = 'project_id'
        self.sandbox_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.pid_table_id = 'pid_table'
        self.gcs_bucket = self.client.bucket(self.bucket)
        self._empty_bucket()

    def _empty_bucket(self):
        for blob in self.client.list_blobs(self.bucket):
            blob.delete()

    @patch('retraction.retract_data_gcs.extract_pids_from_table')
    @patch('gcs_utils.get_drc_bucket')
    @patch('gcs_utils.get_hpo_bucket')
    def test_integration_five_person_data_retraction_skip(
        self, mock_hpo_bucket, mock_bucket, mock_extract_pids):
        mock_hpo_bucket.return_value = self.site_bucket
        mock_bucket.return_value = self.bucket
        mock_extract_pids.return_value = self.skip_pids
        lines_to_remove = {}
        expected_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
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
                blob = self.gcs_bucket.blob(self.folder_prefix_1 + file_name)
                blob.upload_from_file(f, rewind=True, content_type='text/csv')
                blob = self.gcs_bucket.blob(self.folder_prefix_2 + file_name)
                blob.upload_from_file(f, rewind=True, content_type='text/csv')

        rd.run_gcs_retraction(self.project_id,
                              self.sandbox_dataset_id,
                              self.pid_table_id,
                              self.hpo_id,
                              folder='all_folders',
                              force_flag=True,
                              bucket=self.bucket,
                              site_bucket=self.site_bucket)

        total_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            file_name = file_path.split('/')[-1]
            blob = self.gcs_bucket.blob(self.folder_prefix_1 + file_name)
            actual_result_contents = blob.download_as_string().split(b'\n')
            # convert to list and remove header and last list item since it is a newline
            total_lines_post[file_name] = actual_result_contents[1:-1]

        for key in expected_lines_post:
            self.assertEqual(lines_to_remove[key], 0)
            self.assertListEqual(expected_lines_post[key],
                                 total_lines_post[key])

    @patch('retraction.retract_data_gcs.extract_pids_from_table')
    @patch('gcs_utils.get_drc_bucket')
    @patch('gcs_utils.get_hpo_bucket')
    def test_integration_five_person_data_retraction(self, mock_hpo_bucket,
                                                     mock_bucket,
                                                     mock_extract_pids):
        mock_hpo_bucket.return_value = self.site_bucket
        mock_bucket.return_value = self.bucket
        mock_extract_pids.return_value = self.pids
        expected_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
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
                blob = self.gcs_bucket.blob(self.folder_prefix_1 + file_name)
                blob.upload_from_file(f, rewind=True, content_type='text/csv')
                blob = self.gcs_bucket.blob(self.folder_prefix_2 + file_name)
                blob.upload_from_file(f, rewind=True, content_type='text/csv')

        rd.run_gcs_retraction(self.project_id,
                              self.sandbox_dataset_id,
                              self.pid_table_id,
                              self.hpo_id,
                              folder='all_folders',
                              force_flag=True,
                              bucket=self.bucket,
                              site_bucket=self.site_bucket)

        total_lines_post = {}
        for file_path in test_util.FIVE_PERSONS_FILES:
            file_name = file_path.split('/')[-1]
            blob = self.gcs_bucket.blob(self.folder_prefix_1 + file_name)
            actual_result_contents = blob.download_as_string().split(b'\n')
            # convert to list and remove header and last list item since it is a newline
            total_lines_post[file_name] = actual_result_contents[1:-1]

        for key in expected_lines_post:
            self.assertListEqual(expected_lines_post[key],
                                 total_lines_post[key])

    def tearDown(self):
        self._empty_bucket()
