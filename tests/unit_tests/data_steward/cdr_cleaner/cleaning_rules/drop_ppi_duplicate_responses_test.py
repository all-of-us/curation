import unittest

from cdr_cleaner.cleaning_rules.drop_ppi_duplicate_responses import (
    DropPpiDuplicateResponses, get_select_statement, get_delete_statement)
from constants.cdr_cleaner import clean_cdr as clean_consts


class DropPpiDuplicateResponsesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox'
        self.client = None

        self.rule_instance = DropPpiDuplicateResponses(self.project_id,
                                                       self.dataset_id,
                                                       self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        results_list = self.rule_instance.get_query_specs()
        # Post conditions
        sandbox_query = dict()
        sandbox_query[clean_consts.QUERY] = get_select_statement(
            self.project_id, self.dataset_id, self.sandbox_id,
            self.rule_instance.get_sandbox_tablenames()[0])

        update_query = dict()
        update_query[clean_consts.QUERY] = get_delete_statement(
            self.project_id, self.dataset_id)

        expected_list = [sandbox_query, update_query]

        for ex_dict, rs_dict in zip(expected_list, results_list):
            self.assertDictEqual(ex_dict, rs_dict)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        store_duplicate_rows = get_select_statement(
            self.project_id, self.dataset_id, self.sandbox_id,
            self.rule_instance.get_sandbox_tablenames()[0])

        delete_duplicate_rows = get_delete_statement(self.project_id,
                                                     self.dataset_id)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_duplicate_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + delete_duplicate_rows
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
