import os
import unittest
from random import randint
from unittest import mock

import bq_utils
import common
import gcs_utils
import resources
from test.unit_test import test_util
from validation import sql_wrangle
from test.unit_test.test_util import FAKE_HPO_ID
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
        self.hpo_bucket = gcs_utils.get_hpo_bucket(test_util.FAKE_HPO_ID)
        self.dataset = bq_utils.get_dataset_id()
        test_util.empty_bucket(self.hpo_bucket)
        test_util.delete_all_tables(self.dataset)

    def _load_dataset(self, hpo_id):
        for cdm_table in resources.CDM_TABLES:
            cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
            if os.path.exists(cdm_file_name):
                test_util.write_cloud_file(self.hpo_bucket, cdm_file_name)
            else:
                test_util.write_cloud_str(self.hpo_bucket, cdm_table + '.csv', 'dummy\n')
            bq_utils.load_cdm_csv(hpo_id, cdm_table)

        # ensure concept table exists
        if not bq_utils.table_exists(common.CONCEPT):
            bq_utils.create_standard_table(common.CONCEPT, common.CONCEPT)
            q = """INSERT INTO {dataset}.concept
            SELECT * FROM {vocab}.concept""".format(dataset=self.dataset, vocab=common.VOCABULARY_DATASET)
            bq_utils.query(q)

    @staticmethod
    def get_mock_hpo_bucket():
        bucket_env = 'BUCKET_NAME_' + FAKE_HPO_ID.upper()
        hpo_bucket_name = os.getenv(bucket_env)
        if hpo_bucket_name is None:
            raise EnvironmentError()
        return hpo_bucket_name

    @mock.patch('gcs_utils.get_hpo_bucket')
    def test_heel_analyses(self, mock_hpo_bucket):
        # Long-running test
        mock_hpo_bucket.return_value = self.get_mock_hpo_bucket()
        test_util.get_synpuf_results_files()

        # create randomized tables to bypass BQ rate limits
        random_string = str(randint(10000, 99999))
        randomized_hpo_id = FAKE_HPO_ID + '_' + random_string

        # prepare
        self._load_dataset(randomized_hpo_id)
        test_util.populate_achilles(self.hpo_bucket, hpo_id=randomized_hpo_id, include_heel=False)

        # define tables
        achilles_heel_results = randomized_hpo_id + '_' + achilles_heel.ACHILLES_HEEL_RESULTS
        achilles_results_derived = randomized_hpo_id + '_' + achilles_heel.ACHILLES_RESULTS_DERIVED

        # run achilles heel
        achilles_heel.create_tables(randomized_hpo_id, True)
        achilles_heel.run_heel(hpo_id=randomized_hpo_id)

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
        errors = [2, 4, 5, 101, 200, 206, 207, 209, 400, 405, 406, 409, 411, 413, 500, 505, 506, 509,
                  600, 605, 606, 609, 613, 700, 705, 706, 709, 711, 713, 715, 716, 717, 800, 805, 806,
                  809, 813, 814, 906, 1006, 1609, 1805]
        query = sql_wrangle.qualify_tables(
            """SELECT analysis_id FROM {table_id}
            WHERE achilles_heel_warning LIKE 'ERROR:%'
            GROUP BY analysis_id""".format(table_id=achilles_heel_results))
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        actual_result = [row["analysis_id"] for row in rows]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, errors)

        warnings = [4, 5, 7, 8, 9, 200, 210, 302, 400, 402, 412, 420, 500, 511, 512, 513, 514, 515,
                    602, 612, 620, 702, 712, 720, 802, 812, 820]
        query = sql_wrangle.qualify_tables(
            """SELECT analysis_id FROM {table_id}
            WHERE achilles_heel_warning LIKE 'WARNING:%'
            GROUP BY analysis_id""".format(table_id=achilles_heel_results))
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        actual_result = [row["analysis_id"] for row in rows]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, warnings)

        notifications = [101, 103, 105, 114, 115, 118, 208, 301, 410, 610,
                         710, 810, 900, 907, 1000, 1800, 1807]
        query = sql_wrangle.qualify_tables(
            """SELECT analysis_id FROM {table_id}
            WHERE achilles_heel_warning LIKE 'NOTIFICATION:%' and analysis_id is not null
            GROUP BY analysis_id""".format(table_id=achilles_heel_results))
        response = bq_utils.query(query)
        rows = bq_utils.response2rows(response)
        actual_result = [row["analysis_id"] for row in rows]
        for analysis_id in actual_result:
            self.assertIn(analysis_id, notifications)

    def test_qualify_tables(self):
        r = sql_wrangle.qualify_tables('temp.some_table', hpo_id='fake')
        self.assertEqual(r, 'fake_temp_some_table')

        r = sql_wrangle.qualify_tables('synpuf_100.achilles_results', hpo_id='fake')
        self.assertEqual(r, 'fake_achilles_results')

        r = sql_wrangle.qualify_tables('temp.some_table', hpo_id='pitt_temple')
        self.assertEqual(r, 'pitt_temple_temp_some_table')

        r = sql_wrangle.qualify_tables('synpuf_100.achilles_results', hpo_id='pitt_temple')
        self.assertEqual(r, 'pitt_temple_achilles_results')

    def tearDown(self):
        test_util.delete_all_tables(self.dataset)
        test_util.empty_bucket(self.hpo_bucket)
