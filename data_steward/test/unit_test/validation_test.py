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
            expected = [{'file_name': cdm_file_name, 'found': '0', 'parsed': '0', 'loaded': '0'} for cdm_file_name
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
            actual = resources._csv_file_to_list(StringIO.StringIO(actual_result))
            for row in actual:
                row.pop('message', None)
            expected = [{'file_name': 'person.csv', 'type': 'error'}]
            self.assertEqual(actual, expected)

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
            expected = [{'file_name': cdm_file_name, 'found': '1', 'parsed': '0', 'loaded': '0'} for cdm_file_name
                        in common.CDM_FILES]
            self.assertEqual(expected, actual_result)

    @mock.patch('api_util.check_cron')
    def test_curation_report_ignored(self, mock_check_cron):
        folder_prefix = 'dummy-prefix-2018-03-22/'
        exclude_file_list = ["person.csv"]
        exclude_file_list = [folder_prefix + item for item in exclude_file_list]
        expected_result_items = []
        for file_name in exclude_file_list:
            test_util.write_cloud_str(self.hpo_bucket, file_name, ".")

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            # check content of the bucket is correct
            expected_bucket_items = exclude_file_list + [folder_prefix + item for item in common.IGNORE_LIST]
            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_bucket_items = [item['name'] for item in list_bucket_result]
            actual_bucket_items = [item for item in actual_bucket_items
                                   if not main.is_string_excluded_file(item[len(folder_prefix):])]
            self.assertSetEqual(set(expected_bucket_items), set(actual_bucket_items))

            # check that the errors file is empty
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.ERRORS_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)
            self.assertListEqual(expected_result_items, actual_result_items)

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
            expected_item = dict(type="warning", file_name=file_name.split('/')[1], message=main.UNKNOWN_FILE)
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

            # check content of the errors file includes warnings and is correct
            actual_result = test_util.read_cloud_file(self.hpo_bucket,
                                                      folder_prefix + common.ERRORS_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)
            for expected_result_item in expected_result_items:
                self.assertIn(expected_result_item, actual_result_items)

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

        ignored_item = dict(name='2018-09-01/' + common.RESULT_CSV,
                            timeCreated=within_retention_str,
                            updated=within_retention_str)
        bucket_items = [ignored_item]
        actual_result = main.list_submitted_bucket_items(bucket_items)
        self.assertListEqual([], actual_result)

    def get_json_export_files(self, hpo_id):
        json_export_files = [common.ACHILLES_EXPORT_DATASOURCES_JSON]
        for report_file in common.ALL_REPORT_FILES:
            hpo_report_file = common.ACHILLES_EXPORT_PREFIX_STRING + hpo_id + '/' + report_file
            json_export_files.append(hpo_report_file)
        return json_export_files

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
        prefix = 'dummy-prefix-2018-03-22/'
        expected_result_items = resources._csv_to_list(test_util.FIVE_PERSONS_SUCCESS_RESULT_CSV)
        json_export_files = self.get_json_export_files(test_util.FAKE_HPO_ID)

        # upload all five_persons files
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=prefix)

        expected_tables = ['person',
                           'visit_occurrence',
                           'condition_occurrence',
                           'procedure_occurrence',
                           'drug_exposure',
                           'measurement']
        cdm_files = [table + '.csv' for table in expected_tables]

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)

            # check the result file was put in bucket
            expected_object_names = cdm_files + common.IGNORE_LIST + json_export_files
            expected_objects = [prefix + item for item in expected_object_names]

            list_bucket_result = gcs_utils.list_bucket(self.hpo_bucket)
            actual_objects = [item['name'] for item in list_bucket_result]
            self.assertSetEqual(set(expected_objects), set(actual_objects))

            # result says file found, parsed, loaded
            actual_result = test_util.read_cloud_file(self.hpo_bucket, prefix + common.RESULT_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)

            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)
            self.assertTrue(main.all_required_files_loaded(test_util.FAKE_HPO_ID, folder_prefix=prefix))

        # check tables exist and are clustered as expected
        for table in expected_tables:
            fields_file = os.path.join(resources.fields_path, table + '.json')
            table_id = bq_utils.get_table_id(test_util.FAKE_HPO_ID, table)
            table_info = bq_utils.get_table_info(table_id)
            with open(fields_file, 'r') as fp:
                fields = json.load(fp)
                field_names = [field['name'] for field in fields]
                if 'person_id' in field_names:
                    self.table_has_clustering(table_info)

    @mock.patch('api_util.check_cron')
    def test_validation_done_folder(self, mock_check_cron):
        folder_prefix_v1 = 'dummy-prefix-2018-03-22-v1/'
        folder_prefix = 'dummy-prefix-2018-03-22/'

        # upload all five_persons files
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix_v1 + 'person.csv', contents_str='.')
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + 'person.csv', contents_str='.')
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + common.PROCESSED_TXT, contents_str='.')

        main.app.testing = True
        with main.app.test_client() as c:
            return_string = c.get(test_util.VALIDATE_HPO_FILES_URL).data
            self.assertFalse(folder_prefix in return_string)
            self.assertFalse(folder_prefix_v1 in return_string)

    @mock.patch('api_util.check_cron')
    def test_latest_folder_validation(self, mock_check_cron):
        folder_prefix_1 = 'dummy-prefix-2018-03-22-v1/'
        folder_prefix_2 = 'dummy-prefix-2018-03-22-v2/'
        folder_prefix_3 = 'dummy-prefix-2018-03-22-v3/'
        exclude_file_list = [folder_prefix_1 + 'person.csv',
                             folder_prefix_2 + 'blah.csv',
                             folder_prefix_3 + 'visit_occurrence.csv']
        for filename in exclude_file_list:
            test_util.write_cloud_str(self.hpo_bucket, filename, ".\n .")

        main.app.testing = True
        with main.app.test_client() as c:
            return_string = c.get(test_util.VALIDATE_HPO_FILES_URL).data
            # TODO Check that folder_prefix_3 has expected results

    def test_folder_list(self):
        folder_prefix_1 = 'dummy-prefix-2018-03-22-v1/'
        folder_prefix_2 = 'dummy-prefix-2018-03-22-v2/'
        folder_prefix_3 = 'dummy-prefix-2018-03-22-v3/'
        file_list = [folder_prefix_1 + 'person.csv',
                     folder_prefix_2 + 'blah.csv',
                     folder_prefix_3 + 'visit_occurrence.csv',
                     'person.csv']

        for filename in file_list:
            test_util.write_cloud_str(self.hpo_bucket, filename, ".\n .")

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        folder_list = main._get_to_process_list(self.hpo_bucket, bucket_items)
        self.assertListEqual(folder_list, [folder_prefix_3])

    def test_check_processed(self):
        folder_prefix = 'folder/'
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + 'person.csv', '\n')
        test_util.write_cloud_str(self.hpo_bucket, folder_prefix + common.PROCESSED_TXT, '\n')

        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        result = main._get_to_process_list(self.hpo_bucket, bucket_items, force_process=False)
        self.assertListEqual([], result)
        result = main._get_to_process_list(self.hpo_bucket, bucket_items, force_process=True)
        self.assertListEqual(result, [folder_prefix])

    @mock.patch('api_util.check_cron')
    def test_copy_five_persons(self, mock_check_cron):
        folder_prefix = 'dummy-prefix-2018-03-22-v1/'
        # upload all five_persons files
        for cdm_file in test_util.FIVE_PERSONS_FILES:
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=folder_prefix)
            test_util.write_cloud_file(self.hpo_bucket, cdm_file, prefix=folder_prefix + folder_prefix)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.COPY_HPO_FILES_URL)
            prefix = test_util.FAKE_HPO_ID + '/' + self.hpo_bucket + '/' + folder_prefix
            expected_bucket_items = [prefix + item.split(os.sep)[-1] for item in test_util.FIVE_PERSONS_FILES]
            expected_bucket_items.extend([prefix + folder_prefix + item.split(os.sep)[-1] for item in
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
        folder_prefix = 'dummy-prefix-2018-03-22/'
        expected_result_items = resources._csv_to_list(test_util.PII_FILE_LOAD_RESULT_CSV)
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_NAME_FILE, prefix=folder_prefix)
        test_util.write_cloud_file(self.hpo_bucket, test_util.PII_MRN_BAD_PERSON_ID_FILE, prefix=folder_prefix)

        main.app.testing = True
        with main.app.test_client() as c:
            c.get(test_util.VALIDATE_HPO_FILES_URL)
            actual_result = test_util.read_cloud_file(self.hpo_bucket, folder_prefix + common.RESULT_CSV)
            actual_result_file = StringIO.StringIO(actual_result)
            actual_result_items = resources._csv_file_to_list(actual_result_file)
            # sort in order to compare
            expected_result_items.sort()
            actual_result_items.sort()
            self.assertListEqual(expected_result_items, actual_result_items)

    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)
        test_util.empty_bucket(gcs_utils.get_drc_bucket())
        self.testbed.deactivate()
