import unittest

import os
import StringIO
import mock
from google.appengine.ext import testbed

import gcs_utils
from validation import main
import resources
import common

_FAKE_HPO = 'foo'

VALIDATE_HPO_FILES_URL = main.PREFIX + 'ValidateHpoFiles/' + _FAKE_HPO
TEST_DATA_PATH = os.path.join(resources.base_path, 'test', 'test_data')
EMPTY_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'empty_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'all_files_unparseable_validation_result.csv')


class ValidationTest(unittest.TestCase):
    def setUp(self):
        super(ValidationTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(_FAKE_HPO)
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _write_cloud_csv(self, bucket, name, contents_str):
        fp = StringIO.StringIO(contents_str)
        return gcs_utils.upload_object(bucket, name, fp)

    def _read_cloud_file(self, bucket, name):
        return gcs_utils.get_object(bucket, name)

    @mock.patch('api_util.check_cron')
    def test_validation_check_cron(self, mock_check_cron):
        main.validate_hpo_files(_FAKE_HPO)
        self.assertEquals(mock_check_cron.call_count, 1)

    @mock.patch('api_util.check_cron')
    def test_validate_missing_files_output(self, mock_check_cron):
        # enable exception propagation as described at https://goo.gl/LqDgnj
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
            self.assertEquals(1, len(bucket_items))
            self.assertEquals(main.RESULT_CSV, bucket_items[0]['name'])

            # check content of the file is correct
            actual = self._read_cloud_file(self.hpo_bucket, main.RESULT_CSV)
            with open(EMPTY_VALIDATION_RESULT, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual)

    @mock.patch('api_util.check_cron')
    def test_all_files_unparseable_output(self, mock_check_cron):
        for cdm_table in common.CDM_FILES:
            self._write_cloud_csv(self.hpo_bucket, cdm_table, ".")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            bucket_item_names = [item['name'] for item in list_bucket_result]
            expected_items = common.CDM_FILES + [main.RESULT_CSV]
            self.assertSetEqual(set(bucket_item_names), set(expected_items))

            # check content of the file is correct
            actual_result = self._read_cloud_file(self.hpo_bucket, main.RESULT_CSV)
            with open(ALL_FILES_UNPARSEABLE_VALIDATION_RESULT, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual_result)

    @mock.patch('api_util.check_cron')
    def test_bad_file_names(self, mock_check_cron):
        exclude_file_list = ["person_final.csv",
                             "condition_occurence.csv",   # misspelled
                             "avisit_occurrence.csv",
                             "observation.csv",           # not (currently) supported
                             "procedure_occurrence.tsv"]  # unsupported file extension

        expected_result_items = []
        for file_name in exclude_file_list:
            self._write_cloud_csv(self.hpo_bucket, file_name, ".")
            expected_item = dict(file_name=file_name, message=main.UNKNOWN_FILE)
            expected_result_items.append(expected_item)
            
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(VALIDATE_HPO_FILES_URL)

            # check content of the bucket is correct
            expected_bucket_items = exclude_file_list + [main.RESULT_CSV, main.WARNINGS_CSV]
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

            # check content of the warnings file is correct
            actual_result = self._read_cloud_file(self.hpo_bucket,
                                                  main.WARNINGS_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)

            # sort in order to compare
            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)

    def tearDown(self):
        self._empty_bucket()
        self.testbed.deactivate()
