"""
Unit test for the truncate_fitbit_data module

Original Issue: DC-1046

Ensures there is no data newer than cutoff date for participants in
Activity Summary, Heart Rate Minute Level, Heart Rate Summary, and Steps Intraday tables
by sandboxing the applicable records and then dropping them.
"""

# Python imports
import unittest

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.truncate_fitbit_data as truncate_fitbit
from constants.cdr_cleaner import clean_cdr as clean_consts
from constants.bq_utils import WRITE_TRUNCATE


class TruncateFitbitDataTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.query_class = truncate_fitbit.TruncateFitbitData(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.query_class.affected_datasets,
                         [clean_consts.FITBIT])

        # Test
        results_list = self.query_class.get_query_specs()

        # Post conditions
        date_table_counter = 0
        datetime_table_counter = 1
        sandbox_queries = []
        truncate_queries = []

        # Sandboxes and truncates data from FitBit tables with date
        for table in truncate_fitbit.FITBIT_DATE_TABLES:
            save_dropped_date_rows = {
                cdr_consts.QUERY:
                    truncate_fitbit.SANDBOX_QUERY.render(
                        project=self.project_id,
                        sandbox=self.sandbox_id,
                        intermediary_table=self.query_class.
                        get_sandbox_tablenames()[date_table_counter],
                        dataset=self.dataset_id,
                        table_name=table,
                        date_field=truncate_fitbit.
                        FITBIT_TABLES_DATE_FIELDS[table],
                        cutoff_date=truncate_fitbit.CUTOFF_DATE)
            }
            sandbox_queries.append(save_dropped_date_rows)

            truncate_date_query = {
                cdr_consts.QUERY:
                    truncate_fitbit.TRUNCATE_FITBIT_DATA_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table_name=table,
                        sandbox=self.sandbox_id,
                        intermediary_table=self.query_class.
                        get_sandbox_tablenames()[date_table_counter]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            truncate_queries.append(truncate_date_query)
            date_table_counter += 2

        # Sandboxes and truncates data from FitBit tables with datetime
        for table in truncate_fitbit.FITBIT_DATETIME_TABLES:
            save_dropped_datetime_rows = {
                cdr_consts.QUERY:
                    truncate_fitbit.SANDBOX_QUERY.render(
                        project=self.project_id,
                        sandbox=self.sandbox_id,
                        intermediary_table=self.query_class.
                        get_sandbox_tablenames()[datetime_table_counter],
                        dataset=self.dataset_id,
                        table_name=table,
                        date_field=truncate_fitbit.
                        FITBIT_TABLES_DATETIME_FIELDS[table],
                        cutoff_date=truncate_fitbit.CUTOFF_DATETIME)
            }
            sandbox_queries.append(save_dropped_datetime_rows)

            truncate_date_query = {
                cdr_consts.QUERY:
                    truncate_fitbit.TRUNCATE_FITBIT_DATA_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table_name=table,
                        sandbox=self.sandbox_id,
                        intermediary_table=self.query_class.
                        get_sandbox_tablenames()[datetime_table_counter]),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            truncate_queries.append(truncate_date_query)
            datetime_table_counter += 2

        expected_list = sandbox_queries + truncate_queries

        self.assertEqual(expected_list, results_list)
