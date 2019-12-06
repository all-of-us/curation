"""
Unit test components of data_steward.validation.main
"""
from __future__ import print_function
from io import StringIO
import datetime
import json
import os
import re
import unittest
from io import open

import googleapiclient.errors
import mock

import bq_utils
import common
from constants import bq_utils as bq_consts
from constants.validation import hpo_report as report_consts
from constants.validation import main as main_constants
from constants.validation.participants import identity_match as id_match_consts
import gcs_utils
import resources
from tests import test_util as test_util
from validation import main


class ValidationMainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = test_util.FAKE_HPO_ID
        self.hpo_bucket = gcs_utils.get_hpo_bucket(self.hpo_id)
        mock_get_hpo_name = mock.patch(
            'validation.main.get_hpo_name'
        )

        self.mock_get_hpo_name = mock_get_hpo_name.start()
        self.mock_get_hpo_name.return_value = 'Fake HPO'
        self.addCleanup(mock_get_hpo_name.stop)

        self.bigquery_dataset_id = bq_utils.get_dataset_id()
        self.folder_prefix = '2019-01-01/'
        self._empty_bucket()
        test_util.delete_all_tables(self.bigquery_dataset_id)
#        self._create_drug_class_table()

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _create_drug_class_table(self):
        table_name = 'drug_class'
        fields = [{"type": "integer", "name": "concept_id", "mode": "required"},
                  {"type": "string", "name": "concept_name", "mode": "required"},
                  {"type": "string", "name": "drug_class_name", "mode": "required"}]
        bq_utils.create_table(table_id=table_name, fields=fields, drop_existing=True,
                              dataset_id=self.bigquery_dataset_id)

        bq_utils.query(q=main_constants.DRUG_CLASS_QUERY.format(dataset_id=self.bigquery_dataset_id),
                       use_legacy_sql=False,
                       destination_table_id='drug_class',
                       retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
                       write_disposition='WRITE_TRUNCATE',
                       destination_dataset_id=self.bigquery_dataset_id)

    # ignore the timestamp and folder tags from testing
    @staticmethod
    def _remove_timestamp_tags_from_results(result_file):
        # convert to list to avoid using regex
        result_list = result_file.split('\n')
        remove_start_index = result_list.index('</h1>') + 4
        # the folder tags span 3 indices starting immediately after h1 tag ends, timestamp tags span 3 indices after
        output_result_list = result_list[:remove_start_index] + result_list[remove_start_index + 3:]
        output_result_file = '\n'.join(output_result_list)
        return output_result_file

    def table_has_clustering(self, table_info):
        clustering = table_info.get('clustering')
        self.assertIsNotNone(clustering)
        fields = clustering.get('fields')
        self.assertSetEqual(set(fields), {'person_id'})
        time_partitioning = table_info.get('timePartitioning')
        self.assertIsNotNone(time_partitioning)
        tpe = time_partitioning.get('type')
        self.assertEqual(tpe, 'DAY')

    def test_all_files_unparseable_output(self):
        #  INTEGRATION TEST - UPLOADING OBJECTS TO CLOUD
        # TODO possible bug: if no pre-existing table, results in bq table not found error
        for cdm_table in common.SUBMISSION_FILES:
            test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + cdm_table, ".\n .")
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        expected_results = [(f, 1, 0, 0) for f in common.SUBMISSION_FILES]
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    def test_bad_file_names(self):
        # INTEGRATION TEST - UPLOADING OBJECTS TO CLOUD
        bad_file_names = ["avisit_occurrence.csv",
                          "condition_occurence.csv",  # misspelled
                          "person_final.csv",
                          "procedure_occurrence.tsv"]  # unsupported file extension
        expected_warnings = []
        for file_name in bad_file_names:
            test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + file_name, ".")
            expected_item = (file_name, common.UNKNOWN_FILE)
            expected_warnings.append(expected_item)
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertCountEqual(expected_warnings, r['warnings'])

    @mock.patch('api_util.check_cron')
    def test_validate_five_persons_success(self, mock_check_cron):
        # INTEGRATION TEST - writing cloud resources
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
        for table in resources.CDM_TABLES + common.PII_TABLES:
            fields_file = os.path.join(resources.fields_path, table + '.json')
            table_id = bq_utils.get_table_id(test_util.FAKE_HPO_ID, table)
            table_info = bq_utils.get_table_info(table_id)
            with open(fields_file, 'r') as fp:
                fields = json.load(fp)
                field_names = [field['name'] for field in fields]
                if 'person_id' in field_names:
                    self.table_has_clustering(table_info)

    def test_check_processed(self):
        # INTEGRATION TEST - writing resouces to cloud
        test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + 'person.csv', '\n')
        test_util.write_cloud_str(self.hpo_bucket, self.folder_prefix + common.PROCESSED_TXT, '\n')

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        result = main._get_submission_folder(self.hpo_bucket, bucket_items, force_process=False)
        self.assertIsNone(result)
        result = main._get_submission_folder(self.hpo_bucket, bucket_items, force_process=True)
        self.assertEqual(result, self.folder_prefix)

    @mock.patch('api_util.check_cron')
    def test_copy_five_persons(self, mock_check_cron):
        # INTEGRATION TEST - CREATING RESOURCES FOR THE TEST
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
        # INTEGRATION TEST
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        folder_prefix = 'test-folder-fake/'
        test_util.empty_bucket(bucket_nyc)

        main._upload_achilles_files(hpo_id=None, folder_prefix=folder_prefix, target_bucket=bucket_nyc)
        actual_bucket_files = set([item['name'] for item in gcs_utils.list_bucket(bucket_nyc)])
        expected_bucket_files = set(['test-folder-fake/' + item for item in resources.ALL_ACHILLES_INDEX_FILES])
        self.assertSetEqual(expected_bucket_files, actual_bucket_files)

    @mock.patch('api_util.check_cron')
    def test_pii_files_loaded(self, mock_check_cron):
        # INTEGRATION TEST - WRITING RESOURCES TO CLOUD
        # tests if pii files are loaded
        test_file_paths = [test_util.PII_NAME_FILE, test_util.PII_MRN_BAD_PERSON_ID_FILE]
        test_file_names = [os.path.basename(f) for f in test_file_paths]
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_NAME_FILE, prefix=self.folder_prefix)
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_MRN_BAD_PERSON_ID_FILE, prefix=self.folder_prefix)

        rs = resources.csv_to_list(test_util.PII_FILE_LOAD_RESULT_CSV)
        expected_results = [(r['file_name'], int(r['found']), int(r['parsed']), int(r['loaded'])) for r in rs]
        for f in common.SUBMISSION_FILES:
            if f not in test_file_names:
                expected_result = (f, 0, 0, 0)
                expected_results.append(expected_result)

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        r = main.validate_submission(self.hpo_id, self.hpo_bucket, bucket_items, self.folder_prefix)
        self.assertSetEqual(set(expected_results), set(r['results']))

    @mock.patch('api_util.check_cron')
    def _test_html_report_five_person(self, mock_check_cron):
        # INTEGRATION TEST - ACTUALLY BUILDS RESOURCES
        # Not sure this test is still relevant (see hpo_report module and tests)
        # TODO refactor or remove this test
        folder_prefix = '2019-01-01/'
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=folder_prefix)
        # achilles sometimes fails due to rate limits.
        # using both success and failure cases allow it to fail gracefully until there is a fix for achilles
        with open(test_util.FIVE_PERSON_RESULTS_FILE, 'r') as f:
            expected_result_achilles_success = self._remove_timestamp_tags_from_results(f.read())
        with open(test_util.FIVE_PERSON_RESULTS_ACHILLES_ERROR_FILE, 'r') as f:
            expected_result_achilles_failure = self._remove_timestamp_tags_from_results(f.read())
        expected_results = [expected_result_achilles_success, expected_result_achilles_failure]
        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULTS_HTML)
            actual_result_file = self._remove_timestamp_tags_from_results(StringIO(actual_result).getvalue())
            self.assertIn(actual_result_file, expected_results)

    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)
        test_util.empty_bucket(gcs_utils.get_drc_bucket())
        test_util.delete_all_tables(self.bigquery_dataset_id)
