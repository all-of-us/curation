import StringIO
import unittest

import mock
from google.appengine.ext import testbed

import json
import os
import common
import bq_utils
import gcs_utils
import resources
import test_util
from validation import main
import datetime


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
        self.hpo_id = test_util.FAKE_HPO_ID
        self.hpo_bucket = gcs_utils.get_hpo_bucket(self.hpo_id)
        self.folder_prefix = '2019-01-01/'
        self._empty_bucket()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def test_all_files_unparseable_output(self):
        # TODO possible bug: if no pre-existing table, results in bq table not found error
        for cdm_table in common.SUBMISSION_FILES:
            test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + cdm_table, ".\n .")
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        expected_results = [(f, 1, 0, 0) for f in common.SUBMISSION_FILES]
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    def test_bad_file_names(self):
        bad_file_names = ["avisit_occurrence.csv",
                          "condition_occurence.csv",  # misspelled
                          "person_final.csv",
                          "procedure_occurrence.tsv"]  # unsupported file extension
        expected_warnings = []
        for file_name in bad_file_names:
            test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + file_name, ".")
            expected_item = (file_name, main.UNKNOWN_FILE)
            expected_warnings.append(expected_item)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertListEqual(expected_warnings, r['warnings'])

    def test_retention_checks_list_submitted_bucket_items(self):
        outside_retention = datetime.datetime.today() - datetime.timedelta(days=29)
        outside_retention_str = outside_retention.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        bucket_items = [{'name': '2018-09-01/person.csv',
                         'timeCreated': outside_retention_str,
                         'updated': outside_retention_str}]
        # if the file expires within a day it should not be returned
        actual_result = main.list_submitted_bucket_items(bucket_items)
        expected_result = []
        self.assertListEqual(expected_result, actual_result)

        # if the file within retention period it should be returned
        within_retention = datetime.datetime.today() - datetime.timedelta(days=25)
        within_retention_str = within_retention.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        item_2 = {'name': '2018-09-01/visit_occurrence.csv',
                  'timeCreated': within_retention_str,
                  'updated': within_retention_str}
        bucket_items.append(item_2)
        expected_result = [item_2]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual(expected_result, actual_result)

        actual_result = main.list_submitted_bucket_items([])
        self.assertListEqual([], actual_result)

        unknown_item = {'name': '2018-09-01/nyc_cu_person.csv',
                        'timeCreated': within_retention_str,
                        'updated': within_retention_str}
        bucket_items = [unknown_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual(actual_result, bucket_items)

        ignored_item = dict(name='2018-09-01/' + common.RESULTS_HTML,
                            timeCreated=within_retention_str,
                            updated=within_retention_str)
        bucket_items = [ignored_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual([], actual_result)

    def table_has_clustering(self, table_info):
        clustering = table_info.get('clustering')
        self.assertIsNotNone(clustering)
        fields = clustering.get('fields')
        self.assertSetEqual(set(fields), {'person_id'})
        time_partitioning = table_info.get('timePartitioning')
        self.assertIsNotNone(time_partitioning)
        tpe = time_partitioning.get('type')
        self.assertEqual(tpe, 'DAY')

    @mock.patch('api_util.check_cron')
    def test_validate_five_persons_success(self, mock_check_cron):
        expected_results = []
        test_file_names = [os.path.basename(f) for f in test_util.FIVE_PERSONS_FILES]

        for cdm_file in common.SUBMISSION_FILES:
            if cdm_file in test_file_names:
                expected_result = (cdm_file, 1, 1, 1)
                test_file = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_file)
                test_util.write_cloud_file(self.hpo_bucket, test_file, prefix=self.folder_prefix)
            else:
                expected_result = (cdm_file, 0, 0, 0)
            expected_results.append(expected_result)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(r['results']), set(expected_results))

        # check tables exist and are clustered as expected
        for table in common.CDM_TABLES + common.PII_TABLES:
            fields_file = os.path.join(resources.fields_path, table + '.json')
            table_id = bq_utils.get_table_id(test_util.FAKE_HPO_ID, table)
            table_info = bq_utils.get_table_info(table_id)
            with open(fields_file, 'r') as fp:
                fields = json.load(fp)
                field_names = [field['name'] for field in fields]
                if 'person_id' in field_names:
                    self.table_has_clustering(table_info)

    def test_folder_list(self):
        folder_prefix_1 = '2018-03-22-v1/'
        folder_prefix_2 = '2018-03-22-v2/'
        folder_prefix_3 = '2018-03-22-v3/'
        file_list = [folder_prefix_1 + 'person.csv',
                     folder_prefix_2 + 'blah.csv',
                     folder_prefix_3 + 'visit_occurrence.csv',
                     'person.csv']

        for filename in file_list:
            test_util.write_cloud_str(self.hpo_bucket, filename, ".\n .")

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_prefix = main._get_submission_folder(self.hpo_bucket, bucket_items)
        self.assertEqual(folder_prefix, folder_prefix_3)

    def test_check_processed(self):
        test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + 'person.csv', '\n')
        test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + common.PROCESSED_TXT, '\n')

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        result = main._get_submission_folder(self.hpo_bucket, bucket_items, force_process=False)
        self.assertIsNone(result)
        result = main._get_submission_folder(self.hpo_bucket, bucket_items, force_process=True)
        self.assertEqual(result, self.folder_prefix)

    @mock.patch('api_util.check_cron')
    def test_copy_five_persons(self, mock_check_cron):
        # upload all five_persons files
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=self.folder_prefix)
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=self.folder_prefix + self.folder_prefix)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.COPY_HPO_FILES_URL)
            prefix = test_util.FAKE_HPO_ID + '/' + self.hpo_bucket + '/' + self.folder_prefix
            expected_bucket_items = [prefix + item.split(os.sep)[-1] for item in test_util.FIVE_PERSONS_FILES]
            expected_bucket_items.extend([prefix + self.folder_prefix + item.split(os.sep)[-1] for item in
                                          test_util.FIVE_PERSONS_FILES])

            list_bucket_result = gcs_utils.list_bucket(gcs_utils.get_drc_bucket())
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

    def test_target_bucket_upload(self):
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        folder_prefix = 'test-folder-fake/'
        test_util.empty_bucket(bucket_nyc)

        main._upload_achilles_files(hpo_id=None, folder_prefix=folder_prefix, target_bucket=bucket_nyc)
        actual_bucket_files = set([item['name'] for item in gcs_utils.list_bucket(bucket_nyc)])
        expected_bucket_files = set(['test-folder-fake/' + item for item in common.ALL_ACHILLES_INDEX_FILES])
        self.assertSetEqual(expected_bucket_files, actual_bucket_files)

    @mock.patch('api_util.check_cron')
    def test_pii_files_loaded(self, mock_check_cron):
        # tests if pii files are loaded
        test_file_paths = [test_util.PII_NAME_FILE, test_util.PII_MRN_BAD_PERSON_ID_FILE]
        test_file_names = [os.path.basename(f) for f in test_file_paths]
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_NAME_FILE, prefix=self.folder_prefix)
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_MRN_BAD_PERSON_ID_FILE, prefix=self.folder_prefix)

        rs = resources._csv_to_list(test_util.PII_FILE_LOAD_RESULT_CSV)
        expected_results = [(r['file_name'], int(r['found']), int(r['parsed']), int(r['loaded'])) for r in rs]
        for f in common.SUBMISSION_FILES:
            if f not in test_file_names:
                expected_result = (f, 0, 0, 0)
                expected_results.append(expected_result)

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    @mock.patch('api_util.check_cron')
    def test_html_report_person_only(self, mock_check_cron):
        folder_prefix = '2019-01-01/'
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + 'person.csv', ".\n .,.,.")

        with open(test_util.PERSON_ONLY_RESULTS_FILE, 'r') as f:
            expected_result_file = f.read()

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULTS_HTML)
            actual_result_file = StringIO.StringIO(actual_result).getvalue()
            self.assertEqual(expected_result_file, actual_result_file)

    @mock.patch('api_util.check_cron')
    def test_html_report_five_person(self, mock_check_cron):
        folder_prefix = '2019-01-01/'
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=folder_prefix)
        with open(test_util.FIVE_PERSON_RESULTS_FILE, 'r') as f:
            expected_result = f.read()
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULTS_HTML)
            actual_result_file = StringIO.StringIO(actual_result).getvalue()
            self.assertEqual(expected_result, actual_result_file)

    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)
        test_util.empty_bucket(gcs_utils.get_drc_bucket())
        self.testbed.deactivate()
