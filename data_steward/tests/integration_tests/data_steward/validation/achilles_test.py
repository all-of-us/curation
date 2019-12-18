import os
import unittest

from google.appengine.ext import testbed

import bq_utils
import gcs_utils
import resources
import tests.test_util
from tests.test_util import FAKE_HPO_ID
from validation import achilles
import validation.sql_wrangle

# This may change if we strip out unused analyses
ACHILLES_LOOKUP_COUNT = 215
ACHILLES_RESULTS_COUNT = 2497


class AchillesTest(unittest.TestCase):
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

    def tearDown(self):
        test_util.delete_all_tables(bq_utils.get_dataset_id())
        test_util.empty_bucket(self.hpo_bucket)
        self.testbed.deactivate()

    def _load_dataset(self):
        for cdm_table in resources.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                test_util.write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                test_util.write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_cdm_csv(FAKE_HPO_ID, cdm_table)

    def test_load_analyses(self):
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)
        cmd = validation.sql_wrangle.qualify_tables(
            'SELECT DISTINCT(analysis_id) FROM %sachilles_analysis' % validation.sql_wrangle.PREFIX_PLACEHOLDER,
            FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(ACHILLES_LOOKUP_COUNT, int(result['totalRows']))

    def test_run_analyses(self):
        # Long-running test
        self._load_dataset()
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)
        achilles.run_analyses(hpo_id=FAKE_HPO_ID)
        cmd = validation.sql_wrangle.qualify_tables(
            'SELECT COUNT(1) FROM %sachilles_results' % validation.sql_wrangle.PREFIX_PLACEHOLDER, FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(int(result['rows'][0]['f'][0]['v']), ACHILLES_RESULTS_COUNT)
