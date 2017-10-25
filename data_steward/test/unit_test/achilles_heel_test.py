import unittest

import os
from google.appengine.ext import testbed

import bq_utils
import common
import validation.sql_wrangle
from validation import achilles_heel
from test_util import FAKE_HPO_ID
import gcs_utils
import test_util

ACHILLES_HEEL_RESULTS_COUNT = 24
ACHILLES_RESULTS_DERIVED_COUNT = 282
BQ_TIMEOUT_RETRIES = 3


class AchillesHeelTest(unittest.TestCase):
    def setUp(self):
        super(AchillesHeelTest, self).setUp()
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_urlfetch_stub()
        self.testbed.init_blobstore_stub()
        self.testbed.init_datastore_v3_stub()
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        test_util.empty_bucket(self.hpo_bucket)

    def _load_dataset(self):
        for cdm_table in common.CDM_TABLES:
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
        self.assertEqual(int(result['rows'][0]['f'][0]['v']), ACHILLES_HEEL_RESULTS_COUNT)
        cmd = validation.sql_wrangle.qualify_tables(
            'SELECT COUNT(1) FROM %sachilles_results_derived' % validation.sql_wrangle.PREFIX_PLACEHOLDER, FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(int(result['rows'][0]['f'][0]['v']), ACHILLES_RESULTS_DERIVED_COUNT)

    def tearDown(self):
        test_util.empty_bucket(self.hpo_bucket)
        self.testbed.deactivate()
