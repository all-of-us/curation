"""
Unit test for remove_fitbit_data_if_max_age_exceeded module

Original Issue: DC-1001

Ensures any participant with FitBit data who is over the age of 89 is dropped from
activity_summary, steps_intraday, heart_rate_summary, and heart_rate_minute_level
FitBit tables.
"""

# Python imports
import unittest

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.remove_fitbit_data_if_max_age_exceeded import RemoveFitbitDataIfMaxAgeExceeded, \
    TABLES, SAVE_ROWS_TO_BE_DROPPED_QUERY, DROP_MAX_AGE_EXCEEDED_ROWS_QUERY
from constants.cdr_cleaner import clean_cdr as clean_consts
from constants.bq_utils import WRITE_TRUNCATE


class RemoveFitbitDataIfMaxAgeExceededTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox_dataset'
        self.client = None

        self.query_class = RemoveFitbitDataIfMaxAgeExceeded(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.query_class.setup_rule(self.client)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.query_class.affected_datasets, [clean_consts.RDR])

        # Test
        results_list = self.query_class.get_query_specs()

        # Post conditions
        expected_sandbox_queries_list = []
        expected_drop_queries_list = []

        for i, table in enumerate(TABLES):
            expected_sandbox_queries_list.append({
                clean_consts.QUERY:
                    SAVE_ROWS_TO_BE_DROPPED_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.query_class.get_sandbox_tablenames()
                        [i],
                        dataset=self.dataset_id,
                        table=table)
            })

            expected_drop_queries_list.append({
                clean_consts.QUERY:
                    DROP_MAX_AGE_EXCEEDED_ROWS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.query_class.get_sandbox_tablenames()
                        [i]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            })

        self.assertEqual(
            results_list,
            expected_sandbox_queries_list + expected_drop_queries_list)
