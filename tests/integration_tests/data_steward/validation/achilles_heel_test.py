# Python imports
import os
import unittest
from random import randint
from unittest import mock

# Project imports
import app_identity
import bq_utils
import common
import resources
from gcloud.gcs import StorageClient
from gcloud.bq import BigQueryClient
import tests.test_util as test_util
from tests.test_util import FAKE_HPO_ID
from validation import achilles_heel
import validation.sql_wrangle as sql_wrangle

ACHILLES_HEEL_RESULTS_COUNT = 19
ACHILLES_RESULTS_DERIVED_COUNT = 282


class AchillesHeelTest(unittest.TestCase):

    dataset_id = bq_utils.get_dataset_id()
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
        self.hpo_bucket = self.storage_client.get_hpo_bucket(FAKE_HPO_ID)
        self.dataset = bq_utils.get_dataset_id()
        self.storage_client.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(self.bq_client, self.dataset)

    def tearDown(self):
        test_util.delete_all_tables(self.bq_client, self.dataset_id)
        self.storage_client.empty_bucket(self.hpo_bucket)

    @classmethod
    def tearDownClass(cls):
        test_util.drop_hpo_id_bucket_name_table(cls.bq_client, cls.dataset_id)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def _load_dataset(self, hpo_id):
        for cdm_table in resources.CDM_TABLES:

            cdm_filename: str = f'{cdm_table}.csv'
            cdm_filepath: str = os.path.join(test_util.FIVE_PERSONS_PATH,
                                             cdm_filename)

            cdm_blob = self.hpo_bucket.blob(cdm_filename)
            if os.path.exists(cdm_filepath):
                cdm_blob.upload_from_filename(cdm_filepath)
            else:
                cdm_blob.upload_from_string('dummy\n')

            bq_utils.load_cdm_csv(hpo_id, cdm_table)

        # ensure concept table exists
        if not self.bq_client.table_exists(common.CONCEPT):
            bq_utils.create_standard_table(common.CONCEPT, common.CONCEPT)
            q = """INSERT INTO {dataset}.concept
            SELECT * FROM {vocab}.concept""".format(
                dataset=self.dataset, vocab=common.VOCABULARY_DATASET)
            bq_utils.query(q)

    @mock.patch("gcloud.gcs.LOOKUP_TABLES_DATASET_ID", dataset_id)
    def get_mock_hpo_bucket_id(self):
        """
        :returns: `string` the string of a hpo bucket's name
        """
        hpo_bucket_name: str = self.storage_client._get_hpo_bucket_id(
            FAKE_HPO_ID)
        if not hpo_bucket_name:
            raise EnvironmentError()
        return hpo_bucket_name

    @mock.patch.object(StorageClient, 'get_hpo_bucket')
    def test_heel_analyses(self, mock_hpo_bucket):
        # Long-running test
        mock_bucket = mock.MagicMock()
        mock_hpo_bucket.return_value = mock_bucket

        mock_bucket_name: str = self.get_mock_hpo_bucket_id()
        mock_bucket.name = mock_bucket_name

        # create randomized tables to bypass BQ rate limits
        random_string = str(randint(10000, 99999))
        randomized_hpo_id = f'{FAKE_HPO_ID}_{random_string}'

        # prepare
        self._load_dataset(randomized_hpo_id)
        test_util.populate_achilles(hpo_id=randomized_hpo_id,
                                    include_heel=False)

        # define tables
        achilles_heel_results = f'{randomized_hpo_id}_{achilles_heel.ACHILLES_HEEL_RESULTS}'
        achilles_results_derived = f'{randomized_hpo_id}_{achilles_heel.ACHILLES_RESULTS_DERIVED}'

        # run achilles heel
        achilles_heel.create_tables(randomized_hpo_id, True)
        achilles_heel.run_heel(self.bq_client, hpo_id=randomized_hpo_id)

        # validate
        query = sql_wrangle.qualify_tables(
            'SELECT COUNT(1) as num_rows FROM %s' % achilles_heel_results)
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        self.assertEqual(ACHILLES_HEEL_RESULTS_COUNT, rows[0]['num_rows'])
        query = sql_wrangle.qualify_tables(
            'SELECT COUNT(1) as num_rows FROM %s' % achilles_results_derived)
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        self.assertEqual(ACHILLES_RESULTS_DERIVED_COUNT, rows[0]['num_rows'])

        # test new heel re-categorization
        errors = [
            2, 4, 5, 101, 200, 206, 207, 209, 400, 405, 406, 409, 411, 413, 500,
            505, 506, 509, 600, 605, 606, 609, 613, 700, 705, 706, 709, 711,
            713, 715, 716, 717, 800, 805, 806, 809, 813, 814, 906, 1006, 1609,
            1805
        ]
        query = sql_wrangle.qualify_tables("""SELECT analysis_id FROM {table_id}
            WHERE achilles_heel_warning LIKE 'ERROR:%'
            GROUP BY analysis_id""".format(table_id=achilles_heel_results))
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        actual_result = [row["analysis_id"] for row in rows]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, errors)

        warnings = [
            4, 5, 7, 8, 9, 200, 210, 302, 400, 402, 412, 420, 500, 511, 512,
            513, 514, 515, 602, 612, 620, 702, 712, 720, 802, 812, 820
        ]
        query = sql_wrangle.qualify_tables("""SELECT analysis_id FROM {table_id}
            WHERE achilles_heel_warning LIKE 'WARNING:%'
            GROUP BY analysis_id""".format(table_id=achilles_heel_results))
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        actual_result = [row["analysis_id"] for row in rows]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, warnings)

        notifications = [
            101, 103, 105, 114, 115, 118, 208, 301, 410, 610, 710, 810, 900,
            907, 1000, 1800, 1807
        ]
        query = sql_wrangle.qualify_tables("""SELECT analysis_id FROM {table_id}
            WHERE achilles_heel_warning LIKE 'NOTIFICATION:%' and analysis_id is not null
            GROUP BY analysis_id""".format(table_id=achilles_heel_results))
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        actual_result = [row["analysis_id"] for row in rows]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, notifications)
