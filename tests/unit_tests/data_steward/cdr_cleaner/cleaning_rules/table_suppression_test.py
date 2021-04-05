"""
Unit test for table_suppression module

Original Issues: DC-1360

As part of the controlled tier, some table data will be entirely suppressed.  When suppression happens, the table
needs to maintain it’s expected schema, but drop all of its data.

Apply table suppression to note, location, provider, and care_site tables.
table schemas should remain intact and match their data_steward/resource_files/schemas/<table>.json schema definition.

Should be added to list of CONTROLLED_TIER_DEID_CLEANING_CLASSES in data_steward/cdr_cleaner/clean_cdr.py
all data should be dropped from the tables
sandboxing not required
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.table_suppression import TableSuppression, tables, TABLE_SUPPRESSION_QUERY
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts


class TableSuppressionTest(unittest.TestCase):

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

        self.rule_instance = TableSuppression(self.project_id, self.dataset_id,
                                              self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.CONTROLLED_TIER_DEID])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_query_list = []

        for table in tables:
            query = dict()
            query[cdr_consts.QUERY] = TABLE_SUPPRESSION_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table=table,
            )
            expected_query_list.append(query)
        self.assertEqual(results_list, expected_query_list)
