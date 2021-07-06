"""
Unit test for remove_extra_tables module

Remove any tables that are not OMOP, OMOP extension (created by curation), or Vocabulary tables.

Sandbox any tables that are removed.
Should be final cleaning rule.


Original Issue: DC1441
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.remove_extra_tables import (
    RemoveExtraTables, SANDBOX_TABLES_QUERY, DROP_TABLES_QUERY)
from constants.cdr_cleaner import clean_cdr as clean_consts


class RemoveExtraTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'

        self.rule_instance = RemoveExtraTables(self.project_id, self.dataset_id,
                                               self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):

        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        #Test extra tables exist

        self.rule_instance.extra_tables = ['my_extra_table', 'my_extra_table2']
        results_list = self.rule_instance.get_query_specs()

        expected_list = [{
            clean_consts.QUERY:
                SANDBOX_TABLES_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_id=self.sandbox_id,
                    extra_tables=self.rule_instance.extra_tables,
                    sandboxed_extra_tables=[
                        self.rule_instance.sandbox_table_for(table)
                        for table in self.rule_instance.extra_tables
                    ])
        }, {
            clean_consts.QUERY:
                DROP_TABLES_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    extra_tables=self.rule_instance.extra_tables)
        }]

        self.assertEqual(results_list, expected_list)

        #Test no extra tables exist

        self.rule_instance.extra_tables = []
        results_list = self.rule_instance.get_query_specs()

        expected_list = []

        self.assertEqual(results_list, expected_list)