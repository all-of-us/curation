# Python imports
import unittest
from mock import patch

# Project imports
from cdr_cleaner.cleaning_rules.id_deduplicate import (
    DeduplicateIdColumn, ID_DE_DUP_QUERY_TEMPLATE,
    ID_DE_DUP_SANDBOX_QUERY_TEMPLATE)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts


class DeduplicateIdColumnTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.client = None

        self.condition_table = 'condition_occurrence'
        self.procedure_table = 'procedure_occurrence'
        self.domain_tables = [self.condition_table, self.procedure_table]
        self.tables_to_map_patcher = patch('cdm.tables_to_map')
        self.mock_tables_to_map = self.tables_to_map_patcher.start()
        self.mock_tables_to_map.return_value = self.domain_tables

        self.rule_instance = DeduplicateIdColumn(self.project_id,
                                                 self.dataset_id,
                                                 self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def tearDown(self):
        self.tables_to_map_patcher.stop()

    @patch('cdr_cleaner.cleaning_rules.id_deduplicate.get_tables_in_dataset')
    def test_setup_rule(self, mock_get_tables):
        # Test
        mock_get_tables.return_value = self.domain_tables
        self.rule_instance.setup_rule(self.client)

    def test_get_sandbox_tablenames(self):
        expected_sandbox_tablenames = list(
            map(self.rule_instance.sandbox_table_for, self.domain_tables))
        self.assertListEqual(self.rule_instance.get_sandbox_tablenames(),
                             expected_sandbox_tablenames)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.UNIONED])

        # Test
        results_list = self.rule_instance.get_query_specs()

        sandbox_queries = [{
            cdr_consts.QUERY:
                ID_DE_DUP_SANDBOX_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_name=self.condition_table),
            cdr_consts.DESTINATION_TABLE:
                self.rule_instance.sandbox_table_for(self.condition_table),
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                self.sandbox_id
        }, {
            cdr_consts.QUERY:
                ID_DE_DUP_SANDBOX_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_name=self.procedure_table),
            cdr_consts.DESTINATION_TABLE:
                self.rule_instance.sandbox_table_for(self.procedure_table),
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                self.sandbox_id
        }]

        queries = [{
            cdr_consts.QUERY:
                ID_DE_DUP_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_name=self.condition_table),
            cdr_consts.DESTINATION_TABLE:
                self.condition_table,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id
        }, {
            cdr_consts.QUERY:
                ID_DE_DUP_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_name=self.procedure_table),
            cdr_consts.DESTINATION_TABLE:
                self.procedure_table,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id
        }]

        self.assertEqual(results_list, sandbox_queries + queries)
