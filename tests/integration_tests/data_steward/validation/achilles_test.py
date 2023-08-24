# Python imports
import os
import unittest

# Third party imports
import mock

# Project imports
import app_identity
import bq_utils
import resources
import validation.sql_wrangle as sql_wrangle
from gcloud.bq import BigQueryClient
from gcloud.gcs import StorageClient
from tests import test_util
from validation import achilles
from common import BIGQUERY_DATASET_ID

# This may change if we strip out unused analyses
ACHILLES_LOOKUP_COUNT = 215
ACHILLES_RESULTS_COUNT = 2773


class AchillesTest(unittest.TestCase):

    dataset_id = BIGQUERY_DATASET_ID
    project_id = app_identity.get_application_id()
    bq_client = BigQueryClient(project_id)

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
        test_util.setup_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def setUp(self):
        self.storage_client = StorageClient(self.project_id)
        self.hpo_bucket = self.storage_client.get_hpo_bucket(
            test_util.FAKE_HPO_ID)

        self.storage_client.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(self.bq_client, self.dataset_id)

    def tearDown(self):
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.storage_client.empty_bucket(self.hpo_bucket)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def _load_dataset(self):
        for cdm_table in resources.CDM_TABLES:
            cdm_filename: str = f'{cdm_table}.csv'
            cdm_filepath: str = os.path.join(test_util.FIVE_PERSONS_PATH,
                                             cdm_filename)

            cdm_blob = self.hpo_bucket.blob(cdm_filename)
            if os.path.exists(cdm_filepath):
                cdm_blob.upload_from_filename(cdm_filepath)
            else:
                cdm_blob.upload_from_string('dummy\n')

            bq_utils.load_cdm_csv(test_util.FAKE_HPO_ID, cdm_table)

    def test_load_analyses(self):
        achilles.create_tables(test_util.FAKE_HPO_ID, True)
        achilles.load_analyses(test_util.FAKE_HPO_ID)
        cmd = sql_wrangle.qualify_tables(
            'SELECT DISTINCT(analysis_id) FROM %sachilles_analysis' %
            sql_wrangle.PREFIX_PLACEHOLDER, test_util.FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(ACHILLES_LOOKUP_COUNT, int(result['totalRows']))

    def test_run_analyses(self):
        # Long-running test
        self._load_dataset()
        achilles.create_tables(test_util.FAKE_HPO_ID, True)
        achilles.load_analyses(test_util.FAKE_HPO_ID)
        achilles.run_analyses(client=self.bq_client,
                              hpo_id=test_util.FAKE_HPO_ID)
        cmd = sql_wrangle.qualify_tables(
            'SELECT COUNT(1) FROM %sachilles_results' %
            sql_wrangle.PREFIX_PLACEHOLDER, test_util.FAKE_HPO_ID)
        result = bq_utils.query(cmd)
        self.assertEqual(int(result['rows'][0]['f'][0]['v']),
                         ACHILLES_RESULTS_COUNT)
