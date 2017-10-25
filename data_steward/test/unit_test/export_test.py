import unittest
import os

from google.appengine.ext import testbed

import gcs_utils
from validation import export
from test_util import FAKE_HPO_ID
import test_util

BQ_TIMEOUT_RETRIES = 3


class ExportTest(unittest.TestCase):
    def setUp(self):
        super(ExportTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)

    def _test_report_export(self, report):
        test_util.get_synpuf_results_files()
        test_util.populate_achilles(self.hpo_bucket)
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

    def tearDown(self):
        self.testbed.deactivate()
