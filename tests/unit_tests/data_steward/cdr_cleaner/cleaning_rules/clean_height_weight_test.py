"""
Unit test for the clean_weight_height module.

Normalizes all height and weight data into cm and kg and removes invalid/implausible data points (rows)

Original Issue: DC-701

The intent is to delete zero/null/implausible height/weight rows and inserting normalized rows (cm and kg)
"""

# Python imports
import unittest

# Project imports
from common import MEASUREMENT
from cdr_cleaner.cleaning_rules.clean_height_weight import (
    CleanHeightAndWeight, HEIGHT_TABLE, WEIGHT_TABLE, NEW_HEIGHT_ROWS,
    NEW_WEIGHT_ROWS, CREATE_HEIGHT_SANDBOX_QUERY, NEW_HEIGHT_ROWS_QUERY,
    DROP_HEIGHT_ROWS_QUERY, CREATE_WEIGHT_SANDBOX_QUERY, NEW_WEIGHT_ROWS_QUERY,
    DROP_WEIGHT_ROWS_QUERY, INSERT_NEW_ROWS_QUERY)
from constants.bq_utils import WRITE_TRUNCATE, WRITE_APPEND
from constants.cdr_cleaner import clean_cdr as clean_consts


class CleanHeightAndWeightTest(unittest.TestCase):

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

        self.rule_instance = CleanHeightAndWeight(self.project_id,
                                                  self.dataset_id,
                                                  self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_query_specs(self):
        # Pre-conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.DEID_BASE])

        # Test
        result_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                CREATE_HEIGHT_SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_id,
                    height_table=HEIGHT_TABLE)
        }, {
            clean_consts.QUERY:
                NEW_HEIGHT_ROWS_QUERY.render(project_id=self.project_id,
                                             dataset_id=self.dataset_id,
                                             sandbox_dataset_id=self.sandbox_id,
                                             new_height_rows=NEW_HEIGHT_ROWS,
                                             height_table=HEIGHT_TABLE)
        }, {
            clean_consts.QUERY:
                DROP_HEIGHT_ROWS_QUERY.render(project_id=self.project_id,
                                              dataset_id=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }, {
            clean_consts.QUERY:
                INSERT_NEW_ROWS_QUERY.render(project_id=self.project_id,
                                             dataset_id=self.dataset_id,
                                             new_rows=NEW_HEIGHT_ROWS),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_APPEND
        }, {
            clean_consts.QUERY:
                CREATE_WEIGHT_SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_id,
                    weight_table=WEIGHT_TABLE,
                    dataset_id=self.dataset_id),
        }, {
            clean_consts.QUERY:
                NEW_WEIGHT_ROWS_QUERY.render(project_id=self.project_id,
                                             sandbox_dataset_id=self.sandbox_id,
                                             new_weight_rows=NEW_WEIGHT_ROWS,
                                             weight_table=WEIGHT_TABLE,
                                             dataset_id=self.dataset_id)
        }, {
            clean_consts.QUERY:
                DROP_WEIGHT_ROWS_QUERY.render(project_id=self.project_id,
                                              dataset_id=self.dataset_id),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }, {
            clean_consts.QUERY:
                INSERT_NEW_ROWS_QUERY.render(project_id=self.project_id,
                                             dataset_id=self.dataset_id,
                                             sandbox_dataset_id=self.sandbox_id,
                                             new_rows=NEW_WEIGHT_ROWS),
            clean_consts.DESTINATION_TABLE:
                MEASUREMENT,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_APPEND
        }]

        # Test
        self.assertEqual(result_list, expected_list)

    def test_log_queries(self):
        # Pre-conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.DEID_BASE])

        store_height_table = CREATE_HEIGHT_SANDBOX_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_id,
            height_table=HEIGHT_TABLE)

        store_height_rows = NEW_HEIGHT_ROWS_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_id,
            new_height_rows=NEW_HEIGHT_ROWS,
            height_table=HEIGHT_TABLE)

        delete_height_rows = DROP_HEIGHT_ROWS_QUERY.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_new_height_rows = INSERT_NEW_ROWS_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            new_rows=NEW_HEIGHT_ROWS)

        store_weight_table = CREATE_WEIGHT_SANDBOX_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_id,
            weight_table=WEIGHT_TABLE,
            dataset_id=self.dataset_id)

        store_weight_rows = NEW_WEIGHT_ROWS_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_id,
            new_weight_rows=NEW_WEIGHT_ROWS,
            weight_table=WEIGHT_TABLE,
            dataset_id=self.dataset_id)

        delete_weight_rows = DROP_WEIGHT_ROWS_QUERY.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_new_weight_rows = INSERT_NEW_ROWS_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_id,
            new_rows=NEW_WEIGHT_ROWS)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_height_table,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_height_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + delete_height_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + insert_new_height_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_weight_table,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_weight_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + delete_weight_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + insert_new_weight_rows
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
