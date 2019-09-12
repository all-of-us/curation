import os
import unittest

from google.appengine.ext import testbed

import bq_utils
import gcs_utils
import resources
import test_util
import validation.sql_wrangle
from test_util import FAKE_HPO_ID
from validation import achilles_heel

ACHILLES_HEEL_RESULTS_COUNT = 19
ACHILLES_HEEL_RESULTS_ERROR_COUNT = 2
ACHILLES_HEEL_RESULTS_WARNING_COUNT = 12
ACHILLES_HEEL_RESULTS_NOTIFICATION_COUNT = 5
ACHILLES_RESULTS_DERIVED_COUNT = 282
BQ_TIMEOUT_RETRIES = 3


@unittest.skipIf(os.getenv('ALL_TESTS') == 'False', 'Skipping AchillesHeelTest cases')
class AchillesHeelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(bq_utils.get_dataset_id())

    def _load_dataset(self):
        for cdm_table in resources.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                test_util.write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                test_util.write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_cdm_csv(FAKE_HPO_ID, cdm_table)

    def test_heel_analyses(self):
        # Long-running test
        self._load_dataset()

        # populate achilles first
        test_util.get_synpuf_results_files()
        test_util.populate_achilles(self.hpo_bucket, include_heel=False)

        achilles_heel.create_tables(FAKE_HPO_ID, True)
        achilles_heel.run_heel(hpo_id=FAKE_HPO_ID)
        cmd = validation.sql_wrangle.qualify_tables(
            'SELECT COUNT(1) FROM %sachilles_heel_results' % validation.sql_wrangle.PREFIX_PLACEHOLDER, FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(ACHILLES_HEEL_RESULTS_COUNT, int(result['rows'][0]['f'][0]['v']))
        cmd = validation.sql_wrangle.qualify_tables(
            'SELECT COUNT(1) FROM %sachilles_results_derived' % validation.sql_wrangle.PREFIX_PLACEHOLDER, FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(ACHILLES_RESULTS_DERIVED_COUNT, int(result['rows'][0]['f'][0]['v']))

        # test new heel re-categorization
        errors = [2, 4, 5, 101, 200, 206, 207, 209, 400, 405, 406, 409, 411, 413, 500, 505, 506, 509,
                  600, 605, 606, 609, 613, 700, 705, 706, 709, 711, 713, 715, 716, 717, 800, 805, 806,
                  809, 813, 814, 906, 1006, 1609, 1805]
        cmd = validation.sql_wrangle.qualify_tables(
            """SELECT analysis_id FROM {prefix}achilles_heel_results
            WHERE achilles_heel_warning like 'ERROR:%'
            GROUP BY analysis_id""".format(prefix=validation.sql_wrangle.PREFIX_PLACEHOLDER),
            FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        # self.assertIsNone(result.get('analysis_id', None))
        actual_result = [int(row['f'][0]['v']) for row in result['rows']]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, errors)
        # self.assertEqual(ACHILLES_HEEL_RESULTS_ERROR_COUNT, int(result['rows'][0]['f'][0]['v']))

        warnings = [4, 5, 7, 8, 9, 200, 210, 302, 400, 402, 412, 420,
                    500, 511, 512, 513, 514, 515,
                    602, 612, 620, 702, 712, 720, 802, 812, 820]
        cmd = validation.sql_wrangle.qualify_tables(
            """SELECT analysis_id FROM {prefix}achilles_heel_results
            WHERE achilles_heel_warning like 'WARNING:%'
            GROUP BY analysis_id""".format(prefix=validation.sql_wrangle.PREFIX_PLACEHOLDER),
            FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        # self.assertIsNone(result.get('analysis_id', None))
        actual_result = [int(row['f'][0]['v']) for row in result['rows']]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, warnings)
        # self.assertEqual(ACHILLES_HEEL_RESULTS_WARNING_COUNT, int(result['rows'][0]['f'][0]['v']))

        notifications = [101, 103, 105, 114, 115, 118, 208, 301, 410, 610,
                         710, 810, 900, 907, 1000, 1800, 1807]
        cmd = validation.sql_wrangle.qualify_tables(
            """SELECT analysis_id FROM {prefix}achilles_heel_results
            WHERE achilles_heel_warning like 'NOTIFICATION:%' and analysis_id is not null
            GROUP BY analysis_id""".format(prefix=validation.sql_wrangle.PREFIX_PLACEHOLDER),
            FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertIsNone(result.get('analysis_id', None))
        actual_result = [int(row['f'][0]['v']) for row in result['rows']]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, notifications)
        # self.assertEqual(ACHILLES_HEEL_RESULTS_NOTIFICATION_COUNT, int(result['rows'][0]['f'][0]['v']))

    def test_qualify_tables(self):
        r = validation.sql_wrangle.qualify_tables('temp.some_table', hpo_id='fake')
        self.assertEqual(r, 'fake_temp_some_table')

        r = validation.sql_wrangle.qualify_tables('synpuf_100.achilles_results', hpo_id='fake')
        self.assertEqual(r, 'fake_achilles_results')

        r = validation.sql_wrangle.qualify_tables('temp.some_table', hpo_id='pitt_temple')
        self.assertEqual(r, 'pitt_temple_temp_some_table')

        r = validation.sql_wrangle.qualify_tables('synpuf_100.achilles_results', hpo_id='pitt_temple')
        self.assertEqual(r, 'pitt_temple_achilles_results')

    def tearDown(self):
        test_util.delete_all_tables(bq_utils.get_dataset_id())
        test_util.empty_bucket(self.hpo_bucket)
        self.testbed.deactivate()
