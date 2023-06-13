"""
Unit test for remove_fitbit_data_if_max_age_exceeded module

Original Issue: DC-1001, DC-1037, DC-2429, DC-2135, DC-3165

Ensures any participant with FitBit data who is over the age of 89 is dropped from
Activity Summary, Heart Rate Minute Level, Heart Rate Summary, Steps Intraday,
Sleep Daily Summary, Sleep Level, and Device FitBit tables.
"""

# Python imports
import unittest
import mock

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.remove_fitbit_data_if_max_age_exceeded import RemoveFitbitDataIfMaxAgeExceeded, \
    SAVE_ROWS_TO_BE_DROPPED_QUERY, DROP_MAX_AGE_EXCEEDED_ROWS_QUERY, PIPELINE_TABLES
from constants.cdr_cleaner import clean_cdr as clean_consts
from constants.bq_utils import WRITE_TRUNCATE
from common import FITBIT_TABLES


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
        self.combined_dataset_id = 'combined_dataset'
        self.client = None

        self.rule_instance = RemoveFitbitDataIfMaxAgeExceeded(
            self.project_id,
            self.dataset_id,
            self.sandbox_id,
            combined_dataset_id=self.combined_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    @mock.patch('google.cloud.bigquery.table.TableReference.from_string')
    def test_get_query_specs(self, mock_table_reference):
        # Pre conditions
        mock_table_reference = mock.MagicMock()
        type(mock_table_reference).project = mock.PropertyMock(
            return_value=self.project_id)
        type(mock_table_reference).dataset_id = mock.PropertyMock(
            return_value=self.combined_dataset_id)
        type(mock_table_reference).table_id = mock.PropertyMock(
            return_value='person')

        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.FITBIT])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_sandbox_queries_list = []
        expected_drop_queries_list = []

        for i, table in enumerate(FITBIT_TABLES):
            expected_sandbox_queries_list.append({
                clean_consts.QUERY:
                    SAVE_ROWS_TO_BE_DROPPED_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.rule_instance.get_sandbox_tablenames(
                        )[i],
                        dataset=self.dataset_id,
                        table=table,
                        PIPELINE_TABLES=PIPELINE_TABLES,
                        combined_dataset=mock_table_reference)
            })

            expected_drop_queries_list.append({
                clean_consts.QUERY:
                    DROP_MAX_AGE_EXCEEDED_ROWS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.rule_instance.get_sandbox_tablenames(
                        )[i]),
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
