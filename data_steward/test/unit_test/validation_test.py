import StringIO
import unittest
import os

import mock
from google.appengine.ext import testbed

import common
import gcs_utils
import resources
import test_util
from validation import main


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
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _write_cloud_file(self, bucket, f):
        name = os.path.basename(f)
        with open(f, 'r') as fp:
            return self._write_cloud_fp(bucket, name, fp)

    def _write_cloud_str(self, bucket, name, contents_str):
        fp = StringIO.StringIO(contents_str)
        return self._write_cloud_fp(bucket, name, fp)

    def _write_cloud_fp(self, bucket, name, fp):
        return gcs_utils.upload_object(bucket, name, fp)

    def _read_cloud_file(self, bucket, name):
        return gcs_utils.get_object(bucket, name)

    @mock.patch('api_util.check_cron')
    def test_validation_check_cron(self, mock_check_cron):
        main.validate_hpo_files(test_util.FAKE_HPO_ID)
        self.assertEquals(mock_check_cron.call_count, 1)

    @mock.patch('api_util.check_cron')
    def test_validate_missing_files_output(self, mock_check_cron):
        # enable exception propagation as described at https://goo.gl/LqDgnj
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
            self.assertEquals(3, len(bucket_items))
            self.assertEquals(common.IGNORE_LIST.sort(),
                              [item['name'].encode('utf-8') for item in
                               bucket_items].sort())

            # check content of the file is correct
            actual = self._read_cloud_file(self.hpo_bucket, common.RESULT_CSV)
            with open(test_util.EMPTY_VALIDATION_RESULT, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual)
            self.assertFalse(gcs_utils.check_results_for_include_list(test_util.FAKE_HPO_ID))

    @mock.patch('api_util.check_cron')
    def test_errors_csv(self, mock_check_cron):
        self._write_cloud_str(self.hpo_bucket, 'person.csv', ".\n .,.,.")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            bucket_item_names = [item['name'] for item in list_bucket_result]
            expected_items = ['person.csv'] + common.IGNORE_LIST
            self.assertSetEqual(set(bucket_item_names), set(expected_items))

            # check content of the file is correct
            actual_result = self._read_cloud_file(self.hpo_bucket,
                                                  common.ERRORS_CSV)
            with open(test_util.BAD_PERSON_FILE_BQ_LOAD_ERRORS_CSV, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual_result)

    @mock.patch('api_util.check_cron')
    def test_all_files_unparseable_output(self, mock_check_cron):
        for cdm_table in common.INCLUDE_FILES:
            self._write_cloud_str(self.hpo_bucket, cdm_table, ".\n .")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            bucket_item_names = [item['name'] for item in list_bucket_result]
            expected_items = common.INCLUDE_FILES + common.IGNORE_LIST
            self.assertSetEqual(set(bucket_item_names), set(expected_items))

            # check content of the file is correct
            actual_result = self._read_cloud_file(self.hpo_bucket, common.RESULT_CSV)
            with open(test_util.ALL_FILES_UNPARSEABLE_VALIDATION_RESULT, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual_result)

    @mock.patch('api_util.check_cron')
    def test_bad_file_names(self, mock_check_cron):
        exclude_file_list = ["person_final.csv",
                             "condition_occurence.csv",  # misspelled
                             "avisit_occurrence.csv",
                             "procedure_occurrence.tsv"]  # unsupported file extension

        expected_result_items = []
        for file_name in exclude_file_list:
            self._write_cloud_str(self.hpo_bucket, file_name, ".")
            expected_item = dict(file_name=file_name, message=main.UNKNOWN_FILE)
            expected_result_items.append(expected_item)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check content of the bucket is correct
            expected_bucket_items = exclude_file_list + common.IGNORE_LIST
            # [common.RESULT_CSV, common.WARNINGS_CSV]
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

            # check content of the warnings file is correct
            actual_result = self._read_cloud_file(self.hpo_bucket,
                                                  common.WARNINGS_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)
            # sort in order to compare
            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)

    @mock.patch('api_util.check_cron')
    def test_validate_five_persons_success(self, mock_check_cron):
        expected_result_items = resources._csv_to_list(test_util.FIVE_PERSONS_SUCCESS_RESULT_CSV)

        # upload all five_persons files
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            self._write_cloud_file(self.hpo_bucket, cdm_file)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was putin bucket
            expected_bucket_items = common.INCLUDE_FILES + common.IGNORE_LIST
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

            # result says file found, parsed, loaded
            actual_result = self._read_cloud_file(self.hpo_bucket, common.RESULT_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)

            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)
            self.assertTrue(gcs_utils.check_results_for_include_list(test_util.FAKE_HPO_ID))

    def tearDown(self):
        self._empty_bucket()
        self.testbed.deactivate()
