from __future__ import print_function
import json
import os
import unittest

from google.appengine.ext import testbed

import bq_utils
import common
import gcs_utils
import test_util
from test_util import FAKE_HPO_ID
from validation import export, main

BQ_TIMEOUT_RETRIES = 3


@unittest.skipIf(os.getenv('ALL_TESTS') == 'False', 'Skipping ExportTest cases')
class ExportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        cls.testbed = testbed.Testbed()
        cls.testbed.activate()
        cls.testbed.init_app_identity_stub()
        cls.testbed.init_memcache_stub()
        cls.testbed.init_urlfetch_stub()
        cls.testbed.init_blobstore_stub()
        cls.testbed.init_datastore_v3_stub()
        fake_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        dataset_id = bq_utils.get_dataset_id()
        test_util.delete_all_tables(dataset_id)
        test_util.get_synpuf_results_files()
        test_util.populate_achilles(fake_bucket)

    def setUp(self):
        super(ExportTest, self).setUp()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)

    def _empty_bucket(self):
        bucket_items = gcs_utils.list_bucket(self.hpo_bucket)
        for bucket_item in bucket_items:
            gcs_utils.delete_object(self.hpo_bucket, bucket_item['name'])

    def _test_report_export(self, report):
        data_density_path = os.path.join(export.EXPORT_PATH, report)
        result = export.export_from_path(data_density_path, FAKE_HPO_ID)
        return result
        # TODO more strict testing of result payload. The following doesn't work because field order is random.
        # actual_payload = json.dumps(result, sort_keys=True, indent=4, separators=(',', ': '))
        # expected_path = os.path.join(test_util.TEST_DATA_EXPORT_SYNPUF_PATH, report + '.json')
        # with open(expected_path, 'r') as f:
        #     expected_payload = f.read()
        #     self.assertEqual(actual_payload, expected_payload)
        # return result

    def test_export_data_density(self):
        export_result = self._test_report_export('datadensity')
        expected_keys = ['CONCEPTS_PER_PERSON', 'RECORDS_PER_PERSON', 'TOTAL_RECORDS']
        for expected_key in expected_keys:
            self.assertTrue(expected_key in export_result)
        self.assertEqual(len(export_result['TOTAL_RECORDS']['X_CALENDAR_MONTH']), 283)

    def test_export_person(self):
        export_result = self._test_report_export('person')
        expected_keys = ['BIRTH_YEAR_HISTOGRAM', 'ETHNICITY_DATA', 'GENDER_DATA', 'RACE_DATA', 'SUMMARY']
        for expected_key in expected_keys:
            self.assertTrue(expected_key in export_result)
        self.assertEqual(len(export_result['BIRTH_YEAR_HISTOGRAM']['DATA']['COUNT_VALUE']), 72)

    def test_export_achillesheel(self):
        export_result = self._test_report_export('achillesheel')
        self.assertTrue('MESSAGES' in export_result)
        self.assertEqual(len(export_result['MESSAGES']['ATTRIBUTENAME']), 14)

    def test_run_export(self):
        folder_prefix = 'dummy-prefix-2018-03-24/'
        main._upload_achilles_files(test_util.FAKE_HPO_ID, folder_prefix)
        main.run_export(hpo_id=test_util.FAKE_HPO_ID, folder_prefix=folder_prefix)
        bucket_objects = gcs_utils.list_bucket(self.hpo_bucket)
        actual_object_names = [obj['name'] for obj in bucket_objects]
        for report in common.ALL_REPORT_FILES:
            prefix = folder_prefix + common.ACHILLES_EXPORT_PREFIX_STRING + test_util.FAKE_HPO_ID + '/'
            expected_object_name = prefix + report
            self.assertIn(expected_object_name, actual_object_names)

        datasources_json_path = folder_prefix + common.ACHILLES_EXPORT_DATASOURCES_JSON
        self.assertIn(datasources_json_path, actual_object_names)
        datasources_json = gcs_utils.get_object(self.hpo_bucket, datasources_json_path)
        datasources_actual = json.loads(datasources_json)
        datasources_expected = {
            'datasources': [
                {'name': test_util.FAKE_HPO_ID, 'folder': test_util.FAKE_HPO_ID, 'cdmVersion': 5}
            ]
        }
        self.assertDictEqual(datasources_expected, datasources_actual)

    def test_run_export_with_target_bucket(self):
        folder_prefix = 'dummy-prefix-2018-03-24/'
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.get_synpuf_results_files()
        test_util.populate_achilles(self.hpo_bucket, hpo_id=None)
        main.run_export(folder_prefix=folder_prefix, target_bucket=bucket_nyc)
        bucket_objects = gcs_utils.list_bucket(bucket_nyc)
        actual_object_names = [obj['name'] for obj in bucket_objects]
        for report in common.ALL_REPORT_FILES:
            expected_object_name = folder_prefix + common.ACHILLES_EXPORT_PREFIX_STRING + 'default' + '/' + report
            self.assertIn(expected_object_name, actual_object_names)

        datasources_json_path = folder_prefix + common.ACHILLES_EXPORT_DATASOURCES_JSON
        self.assertIn(datasources_json_path, actual_object_names)
        datasources_json = gcs_utils.get_object(bucket_nyc, datasources_json_path)
        datasources_actual = json.loads(datasources_json)
        datasources_expected = {
            'datasources': [
                {'name': 'default', 'folder': 'default', 'cdmVersion': 5}
            ]
        }
        self.assertDictEqual(datasources_expected, datasources_actual)

    def test_run_export_with_target_bucket_and_hpo_id(self):
        folder_prefix = 'dummy-prefix-2018-03-24/'
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        main.run_export(hpo_id=test_util.FAKE_HPO_ID, folder_prefix=folder_prefix, target_bucket=bucket_nyc)
        bucket_objects = gcs_utils.list_bucket(bucket_nyc)
        actual_object_names = [obj['name'] for obj in bucket_objects]
        for report in common.ALL_REPORT_FILES:
            prefix = folder_prefix + common.ACHILLES_EXPORT_PREFIX_STRING + test_util.FAKE_HPO_ID + '/'
            expected_object_name = prefix + report
            self.assertIn(expected_object_name, actual_object_names)
        datasources_json_path = folder_prefix + common.ACHILLES_EXPORT_DATASOURCES_JSON
        self.assertIn(datasources_json_path, actual_object_names)
        datasources_json = gcs_utils.get_object(bucket_nyc, datasources_json_path)
        datasources_actual = json.loads(datasources_json)
        datasources_expected = {
            'datasources': [
                {'name': test_util.FAKE_HPO_ID, 'folder': test_util.FAKE_HPO_ID, 'cdmVersion': 5}
            ]
        }
        self.assertDictEqual(datasources_expected, datasources_actual)

    def tearDown(self):
        self._empty_bucket()
        bucket_nyc = gcs_utils.get_hpo_bucket('nyc')
        test_util.empty_bucket(bucket_nyc)

    @classmethod
    def tearDownClass(cls):
        dataset_id = bq_utils.get_dataset_id()
        test_util.delete_all_tables(dataset_id)
        cls.testbed.deactivate()
