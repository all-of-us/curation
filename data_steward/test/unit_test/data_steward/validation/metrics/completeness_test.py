import unittest

from mock import patch

import bq_utils
from constants.validation.metrics import completeness as consts
import resources
from test.unit_test import test_util
from validation.metrics import completeness


class CompletenessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'nyc_cu'
        self.dataset_id = bq_utils.get_dataset_id()

    def test_is_omop_col(self):
        col = dict()
        col[consts.TABLE_NAME] = 'condition_occurrence'
        self.assertTrue(completeness.is_omop_col(col))
        col[consts.TABLE_NAME] = 'hpo1_condition_occurrence'
        self.assertTrue(completeness.is_omop_col(col))
        col[consts.TABLE_NAME] = 'condition_occurrence_hpo1'
        self.assertFalse(completeness.is_omop_col(col))
        col[consts.TABLE_NAME] = 'condition_occurrence_hpo1'
        self.assertFalse(completeness.is_omop_col(col))

    def test_is_hpo_col(self):
        col = dict()
        hpo_id = 'bogus_hpo'
        table_name = 'condition_occurrence'
        col[consts.TABLE_NAME] = bq_utils.get_table_id(hpo_id, table_name)
        self.assertTrue(completeness.is_hpo_col(hpo_id, col))
        col[consts.TABLE_NAME] = table_name
        self.assertFalse(completeness.is_hpo_col(hpo_id, col))

    @patch('validation.participants.writers.bq_utils.query')
    def test_get_cols(self, mock_query):
        cols = [
            {
                'column_name': 'condition_start_datetime',
                'concept_zero_count': 0,
                'null_count': 0,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': 1.0,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 2105898
            },
            {
                'column_name': 'visit_occurrence_id',
                'concept_zero_count': 0,
                'null_count': None,
                'omop_table_name': 'condition_occurrence',
                'percent_populated': None,
                'table_name': 'nyc_cu_condition_occurrence',
                'table_row_count': 0
            }
        ]
        dataset_id = 'some_dataset_id'
        completeness.get_cols(dataset_id)
        self.assertEqual(mock_query.call_count, 1)
        mock_query.assert_called_with(consts.COLUMNS_QUERY_FMT.format(dataset_id=dataset_id))
        with patch('validation.participants.writers.bq_utils.response2rows') as mock_response2rows:
            mock_response2rows.return_value = cols
            expected_result = [cols[0]]
            actual_result = completeness.get_cols(dataset_id)
            self.assertCountEqual(expected_result, actual_result)

    def test_create_completeness_query(self):
        dataset_id = 'some_dataset_id'
        table_name = 'hpo1_condition_occurrence'
        omop_table_name = 'condition_occurrence',
        table_row_count = 100
        col1_name = 'condition_occurrence_id'
        col2_name = 'condition_concept_id'
        col1 = dict(table_name=table_name,
                    omop_table_name=omop_table_name,
                    table_row_count=table_row_count,
                    column_name=col1_name)
        col2 = dict(table_name=table_name,
                    omop_table_name=omop_table_name,
                    table_row_count=table_row_count,
                    column_name=col2_name)
        expected_q1 = consts.COMPLETENESS_SUBQUERY_FMT.format(dataset_id=dataset_id,
                                                              table_name=table_name,
                                                              omop_table_name=omop_table_name,
                                                              table_row_count=table_row_count,
                                                              column_name=col1_name,
                                                              concept_zero_expr='0')
        col2_concept_zero = consts.CONCEPT_ZERO_CLAUSE.format(column_name=col2_name)
        expected_q2 = consts.COMPLETENESS_SUBQUERY_FMT.format(dataset_id=dataset_id,
                                                              table_name=table_name,
                                                              omop_table_name=omop_table_name,
                                                              table_row_count=table_row_count,
                                                              column_name=col2_name,
                                                              concept_zero_expr=col2_concept_zero)
        union_all_subqueries = consts.UNION_ALL.join([expected_q1, expected_q2])
        expected_result = consts.COMPLETENESS_QUERY_FMT.format(union_all_subqueries=union_all_subqueries)
        actual_result = completeness.create_completeness_query(dataset_id, [col1, col2])
        self.assertEqual(expected_result, actual_result)

    def test_get_standard_table_name(self):
        device_cost = 'device_cost'
        t = completeness.get_standard_table_name(device_cost)
        # e.g. shouldn't match 'cost'
        self.assertEqual(device_cost, t)
        t = completeness.get_standard_table_name(self.hpo_id + '_' + device_cost)
        self.assertEqual(device_cost, t)

    @staticmethod
    def get_nyc_cu_cols():
        result = []
        cols = resources._csv_to_list(test_util.TEST_NYC_CU_COLS_CSV)
        for col in cols:
            omop_table_name = completeness.get_standard_table_name(col[consts.TABLE_NAME])
            if omop_table_name:
                col[consts.OMOP_TABLE_NAME] = omop_table_name
                result.append(col)
        return result

    def test_hpo_query(self):
        with patch('validation.metrics.completeness.get_cols') as mock_get_cols:
            nyc_cu_cols = self.get_nyc_cu_cols()
            mock_get_cols.return_value = self.get_nyc_cu_cols()
            query = completeness.get_hpo_completeness_query(self.hpo_id)
            # For now checking for expected column expressions
            # TODO find more robust way to test output
            for nyc_cu_col in nyc_cu_cols:
                column_exp = "'%s' AS column_name" % nyc_cu_col[consts.COLUMN_NAME]
                self.assertTrue(column_exp in query)
