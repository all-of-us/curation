import unittest

from mock import patch

import bq_utils
import constants.validation.metrics.completeness as consts
from validation.metrics import completeness


class CompletenessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

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
        dataset_id = 'some_dataset_id'
        completeness.get_cols(dataset_id)
        self.assertEqual(mock_query.call_count, 1)
        mock_query.assert_called_with(consts.COLUMNS_QUERY_FMT.format(dataset_id=dataset_id))

    def test_create_completeness_query(self):
        dataset_id = 'some_dataset_id'
        table_name = 'hpo1_condition_occurrence'
        table_row_count = 100
        col1_name = 'condition_occurrence_id'
        col2_name = 'condition_concept_id'
        col1 = dict(table_name=table_name,
                    table_row_count=table_row_count,
                    column_name=col1_name)
        col2 = dict(table_name=table_name,
                    table_row_count=table_row_count,
                    column_name=col2_name)
        expected_q1 = consts.COMPLETENESS_QUERY_FMT.format(dataset_id=dataset_id,
                                                           table_name=table_name,
                                                           table_row_count=table_row_count,
                                                           column_name=col1_name,
                                                           concept_zero_expr='0')
        col2_concept_zero = consts.CONCEPT_ZERO_CLAUSE.format(column_name=col2_name)
        expected_q2 = consts.COMPLETENESS_QUERY_FMT.format(dataset_id=dataset_id,
                                                           table_name=table_name,
                                                           table_row_count=table_row_count,
                                                           column_name=col2_name,
                                                           concept_zero_expr=col2_concept_zero)
        expected_result = consts.UNION_ALL.join([expected_q1, expected_q2])
        actual_result = completeness.create_completeness_query(dataset_id, [col1, col2])
        self.assertEqual(expected_result, actual_result)
