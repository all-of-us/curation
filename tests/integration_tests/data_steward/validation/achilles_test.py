import os
import unittest

# Third-party imports
from google.cloud import storage

import bq_utils
from gcloud.gcs import StorageClient
import gcs_utils
import resources
import tests.test_util as test_util
from tests.test_util import FAKE_HPO_ID
from validation import achilles
import validation.sql_wrangle as sql_wrangle

# This may change if we strip out unused analyses
ACHILLES_LOOKUP_COUNT = 215
ACHILLES_RESULTS_COUNT = 2779


class AchillesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        self.storage_client = StorageClient()

        test_util.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(bq_utils.get_dataset_id())

    def tearDown(self):
        test_util.delete_all_tables(bq_utils.get_dataset_id())
        test_util.empty_bucket(self.hpo_bucket)

    def _load_dataset(self):
        for cdm_table in resources.CDM_TABLES:
            cdm_filename: str = f'{cdm_table}.csv'
            cdm_filepath: str = os.path.join(test_util.FIVE_PERSONS_PATH,
                                             cdm_filename)

            bucket = self.storage_client.get_bucket(self.hpo_bucket)
            cdm_blob = storage.Blob(cdm_filename, bucket)
            if os.path.exists(cdm_filepath):
                cdm_blob.upload_from_filename(cdm_filepath)
                # test_util.write_cloud_file(self.hpo_bucket, cdm_filepath)
            else:
                cdm_blob.upload_from_string('dummy\n')
                # test_util.write_cloud_str(self.hpo_bucket, cdm_table + '.csv',
                #                           'dummy\n')
            bq_utils.load_cdm_csv(FAKE_HPO_ID, cdm_table)

    def test_load_analyses(self):
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)
        cmd = sql_wrangle.qualify_tables(
            'SELECT DISTINCT(analysis_id) FROM %sachilles_analysis' %
            sql_wrangle.PREFIX_PLACEHOLDER, FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(ACHILLES_LOOKUP_COUNT, int(result['totalRows']))

    def test_run_analyses(self):
        # Long-running test
        self._load_dataset()
        achilles.create_tables(FAKE_HPO_ID, True)
        achilles.load_analyses(FAKE_HPO_ID)
        achilles.run_analyses(hpo_id=FAKE_HPO_ID)
        cmd = sql_wrangle.qualify_tables(
            'SELECT COUNT(1) FROM %sachilles_results' %
            sql_wrangle.PREFIX_PLACEHOLDER, FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(int(result['rows'][0]['f'][0]['v']),
                         ACHILLES_RESULTS_COUNT)
