import unittest

import os
import StringIO

import time
from google.appengine.ext import testbed

import bq_utils
import common
import resources
import validation.sql_wrangle
from validation import achilles
from validation import achilles_heel
from test_util import FAKE_HPO_ID
import gcs_utils
import test_util

ACHILLES_HEEL_RESULTS_COUNT = 24
ACHILLES_RESULTS_DERIVED_COUNT = 282
BQ_TIMEOUT_SECONDS = 5


@unittest.skip("skipping heel")
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

    def _write_cloud_str(self, bucket, name, contents_str):
        fp = StringIO.StringIO(contents_str)
        return self._write_cloud_fp(bucket, name, fp)

    def _write_cloud_file(self, bucket, f):
        name = os.path.basename(f)
        with open(f, 'r') as fp:
            return self._write_cloud_fp(bucket, name, fp)

    def _write_cloud_fp(self, bucket, name, fp):
        return gcs_utils.upload_object(bucket, name, fp)

    def _load_dataset(self):
        for cdm_table in common.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                self._write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                self._write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_cdm_csv(FAKE_HPO_ID, cdm_table)

    def _populate_achilles(self):
        from google.appengine.api import app_identity

        app_id = app_identity.get_application_id()

        test_file_name = achilles.ACHILLES_ANALYSIS + '.csv'
        achilles_analysis_file_path = os.path.join(test_util.TEST_DATA_EXPORT_PATH, test_file_name)
        schema_path = os.path.join(resources.fields_path, achilles.ACHILLES_ANALYSIS + '.json')
        self._write_cloud_file(self.hpo_bucket, achilles_analysis_file_path)
        gcs_path = 'gs://' + self.hpo_bucket + '/' + test_file_name
        dataset_id = bq_utils.get_dataset_id()
        table_id = bq_utils.get_table_id(FAKE_HPO_ID, achilles.ACHILLES_ANALYSIS)
        bq_utils.load_csv(schema_path, gcs_path, app_id, dataset_id, table_id)

        for table_name in [achilles.ACHILLES_RESULTS, achilles.ACHILLES_RESULTS_DIST]:
            schema_file_name = table_name + '.json'
            schema_path = os.path.join(resources.fields_path, schema_file_name)
            test_file_name = table_name + '.csv'
            test_file_path = os.path.join(test_util.TEST_DATA_EXPORT_SYNPUF_PATH, table_name + '.csv')
            self._write_cloud_file(self.hpo_bucket, test_file_path)
            gcs_path = 'gs://' + self.hpo_bucket + '/' + test_file_name
            dataset_id = bq_utils.get_dataset_id()
            table_id = bq_utils.get_table_id(FAKE_HPO_ID, table_name)
            bq_utils.load_csv(schema_path, gcs_path, app_id, dataset_id, table_id)
        time.sleep(BQ_TIMEOUT_SECONDS)

    def test_heel_analyses(self):
        # Long-running test
        self._load_dataset()

        # populate achilles first
        test_util.get_synpuf_results_files()
        self._populate_achilles()

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
