"""
Unit Test for the null_concept_ids_for_numeric_ppi module.

Nullify concept ids for numeric PPIs from the RDR observation dataset

Original Issues: DC-537, DC-703

The intent is to null concept ids (value_source_concept_id, value_as_concept_id, value_source_value,
value_as_string) from the RDR observation dataset. The changed records should be archived in the
dataset sandbox.
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.null_concept_ids_for_numeric_ppi import NullConceptIDForNumericPPI, \
    SANDBOX_QUERY, CLEAN_NUMERIC_PPI_QUERY
from constants.cdr_cleaner import clean_cdr as clean_consts

# Third party imports


class NullConceptIDForNumericPPITest(unittest.TestCase):

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

        self.rule_instance = NullConceptIDForNumericPPI(self.project_id,
                                                        self.dataset_id,
                                                        self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_query_spec(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        result_list = self.rule_instance.get_query_specs()

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
                CLEAN_NUMERIC_PPI_QUERY.render(project=self.project_id,
                                               dataset=self.dataset_id),
        }]

        self.assertEqual(result_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        store_rows_to_be_changed = SANDBOX_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            intermediary_table=self.rule_instance.get_sandbox_tablenames()[0])

        select_rows_to_be_changed = CLEAN_NUMERIC_PPI_QUERY.render(
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
