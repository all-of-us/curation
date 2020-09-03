"""
Unit test for clean_ppi_numeric_fields_using_parameters module

Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-827, DC-502, DC-487

The intent is to ensure that numeric free-text fields that are not manipulated by de-id
have value range restrictions applied to the value_as_number field across the entire dataset.
"""

# Python imports
import unittest

# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import (
    CleanPPINumericFieldsUsingParameters, SANDBOX_QUERY,
    CLEAN_PPI_NUMERIC_FIELDS_QUERY)


class CleanPPINumericFieldsUsingParametersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'
        self.client = None

        self.rule_instance = CleanPPINumericFieldsUsingParameters(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEquals(self.rule_instance.project_id, self.project_id)
        self.assertEquals(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEquals(self.rule_instance.sandbox_dataset_id,
                          self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                SANDBOX_QUERY.render(project=self.project_id,
                                     dataset=self.dataset_id,
                                     sandbox_dataset=self.sandbox_id,
                                     intermediary_table=self.rule_instance.
                                     get_sandbox_tablenames()[0])
        }, {
            clean_consts.QUERY:
                CLEAN_PPI_NUMERIC_FIELDS_QUERY.render(project=self.project_id,
                                                      dataset=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                'observation',
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        store_rows_to_be_changed = SANDBOX_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            intermediary_table=self.rule_instance.get_sandbox_tablenames()[0])

        select_rows_to_be_changed = CLEAN_PPI_NUMERIC_FIELDS_QUERY.render(
            project=self.project_id, dataset=self.dataset_id)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_rows_to_be_changed,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + select_rows_to_be_changed
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
