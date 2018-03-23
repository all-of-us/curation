import StringIO
import unittest

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

    @mock.patch('api_util.check_cron')
    def _test_validate_missing_files_output(self, mock_check_cron):
        # enable exception propagation as described at https://goo.gl/LqDgnj
        folder_prefix = 'dummy-prefix-2018-03-22/'
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result files were placed in bucket
            bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
            item_names = []
            for item in bucket_items:
                item_names.append(item['name'])
            for ignore_file in common.IGNORE_LIST:
                self.assertIn(folder_prefix + ignore_file, item_names)

            # check content of result.csv is correct
            # TODO fix this for all cdm files and use object comparison
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULT_CSV)
            actual = resources._csv_file_to_list(StringIO.StringIO(actual_result))
            expected = [{'cdm_file_name': cdm_file_name, 'found': '0', 'parsed': '0', 'loaded': '0'} for cdm_file_name
                        in common.REQUIRED_FILES]
            self.assertEqual(expected, actual)
            self.assertFalse(main.all_required_files_loaded(test_util.FAKE_HPO_ID, folder_prefix))

    @mock.patch('api_util.check_cron')
    def test_errors_csv(self, mock_check_cron):
        folder_prefix = 'dummy-prefix-2018-03-22/'
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + 'person.csv', ".\n .,.,.")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            bucket_item_names = [item['name'] for item in list_bucket_result if item['name'].startswith(folder_prefix)]
            expected_items = ['person.csv'] + common.IGNORE_LIST
            expected_items = [folder_prefix + item for item in expected_items]
            self.assertSetEqual(set(bucket_item_names), set(expected_items))

            # check content of the file is correct
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.ERRORS_CSV)
            with open(test_util.BAD_PERSON_FILE_BQ_LOAD_ERRORS_CSV, 'r') as f:
                expected = f.read()
                self.assertEqual(expected, actual_result)

    @mock.patch('api_util.check_cron')
    def test_all_files_unparseable_output(self, mock_check_cron):
        # TODO possible bug: if no pre-existing table, results in bq table not found error
        folder_prefix = 'dummy-prefix-2018-03-22/'
        for cdm_table in common.CDM_FILES:
            test_util.write_cloud_str(self.hpo_bucket, folder_prefix + cdm_table, ".\n .")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            bucket_item_names = [item['name'] for item in list_bucket_result if item['name'].startswith(folder_prefix)]
            expected_items = common.CDM_FILES + common.IGNORE_LIST
            expected_items = [folder_prefix + item_name for item_name in expected_items]
            self.assertSetEqual(set(bucket_item_names), set(expected_items))

            # check content of the file is correct
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULT_CSV)
            actual_result = resources._csv_file_to_list(StringIO.StringIO(actual_result))
            expected = [{'cdm_file_name': cdm_file_name, 'found': '1', 'parsed': '0', 'loaded': '0'} for cdm_file_name
                        in common.CDM_FILES]
            self.assertEqual(expected, actual_result)

    @mock.patch('api_util.check_cron')
    def test_bad_file_names(self, mock_check_cron):
        folder_prefix = 'dummy-prefix-2018-03-22/'
        exclude_file_list = ["person_final.csv",
                             "condition_occurence.csv",  # misspelled
                             "avisit_occurrence.csv",
                             "procedure_occurrence.tsv"]  # unsupported file extension

        exclude_file_list = [folder_prefix + item for item in exclude_file_list]
        expected_result_items = []
        for file_name in exclude_file_list:
            test_util.write_cloud_str(self.hpo_bucket, file_name, ".")
            expected_item = dict(file_name=file_name.split('/')[1], message=main.UNKNOWN_FILE)
            expected_result_items.append(expected_item)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check content of the bucket is correct
            expected_bucket_items = exclude_file_list + [folder_prefix + item for item in common.IGNORE_LIST]
            # [common.RESULT_CSV, common.WARNINGS_CSV]
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

            # check content of the warnings file is correct
            actual_result = test_util.read_cloud_file(self.hpo_bucket,
                                                      folder_prefix + common.WARNINGS_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)
            # sort in order to compare
            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)

    def get_json_export_files(self, hpo_id):
        json_export_files = [common.ACHILLES_EXPORT_DATASOURCES_JSON]
        for report_file in common.ALL_REPORT_FILES:
            hpo_report_file = common.ACHILLES_EXPORT_PREFIX_STRING + hpo_id + '/' + report_file
            json_export_files.append(hpo_report_file)
        return json_export_files

    @mock.patch('api_util.check_cron')
    def test_validate_five_persons_success(self, mock_check_cron):
        prefix = 'dummy-prefix-2018-03-22/'
        expected_result_items = resources._csv_to_list(test_util.FIVE_PERSONS_SUCCESS_RESULT_CSV)
        json_export_files = self.get_json_export_files(test_util.FAKE_HPO_ID)

        # upload all five_persons files
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=prefix)

        main.app.testing = True
        with main.app.test_client() as c:
            return_string = c.get(test_util.VALIDATE_HPO_FILES_URL).data
            self.assertTrue(prefix in return_string)

            # check the result file was putin bucket
            expected_bucket_items = common.REQUIRED_FILES + common.IGNORE_LIST + json_export_files
            # want to keep this test the same. So adding all the old required files.
            expected_bucket_items = expected_bucket_items + ['measurement.csv',
                                                             'procedure_occurrence.csv',
                                                             'drug_exposure.csv',
                                                             'condition_occurrence.csv',
                                                             'visit_occurrence.csv']
            expected_bucket_items = [prefix + item for item in expected_bucket_items]

            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

            # result says file found, parsed, loaded
            actual_result = test_util.read_cloud_file(self.hpo_bucket, prefix + common.RESULT_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)

            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)
            self.assertTrue(main.all_required_files_loaded(test_util.FAKE_HPO_ID, folder_prefix=prefix))

    @mock.patch('api_util.check_cron')
    def test_validation_done_folder(self, mock_check_cron):
        folder_prefix = 'dummy-prefix-2018-03-22/'

        # upload all five_persons files
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + 'validation_done.txt', contents_str='success')

        main.app.testing = True
        with main.app.test_client() as c:
            return_string = c.get(test_util.VALIDATE_HPO_FILES_URL).data
            self.assertFalse(folder_prefix in return_string)

    def tearDown(self):
        # self._empty_bucket()
        self.testbed.deactivate()
