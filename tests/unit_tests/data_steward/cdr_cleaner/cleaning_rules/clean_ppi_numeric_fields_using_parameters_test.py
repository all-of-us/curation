"""
Unit test for clean_ppi_numeric_fields_using_parameters module

Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-1061, DC-827, DC-502, DC-487

The intent is to ensure that numeric free-text fields that are not manipulated by de-id
have value range restrictions applied to the value_as_number field across the entire dataset.
"""

# Python imports
import unittest

# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
import cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters as clean_ppi


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

        self.query_class = clean_ppi.CleanPPINumericFieldsUsingParameters(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.query_class.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.query_class.affected_datasets, [clean_consts.RDR])

        # Test
        results_list = self.query_class.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                clean_ppi.INVALID_VALUES_SANDBOX_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_id,
                    intermediary_table=clean_ppi.INVALID_VALUES_SANDBOX)
        }, {
            clean_consts.QUERY:
                clean_ppi.CLEAN_INVALID_VALUES_QUERY.render(
                    project=self.project_id, dataset=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                'observation',
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)
