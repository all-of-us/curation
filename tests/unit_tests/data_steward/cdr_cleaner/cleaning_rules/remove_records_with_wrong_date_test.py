import unittest
from mock import mock

from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import OBSERVATION_TABLE
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import DEFAULT_YEAR_THRESHOLD
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import OBSERVATION_DEFAULT_YEAR_THRESHOLD
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import NULLABLE_DATE_FIELD_EXPRESSION
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import WHERE_CLAUSE_REQUIRED_FIELD
import cdr_cleaner.cleaning_rules.remove_records_with_wrong_date as remove_records_with_wrong_date
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts


class RemoveRecordsWithWrongDateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.condition_occurrence = 'condition_occurrence'
        self.condition_concept_id = 'condition_concept_id'
        self.condition_start_date = 'condition_start_date'
        self.condition_start_datetime = 'condition_start_datetime'
        self.condition_end_datetime = 'condition_end_datetime'
        self.condition_end_date = 'condition_end_date'
        self.year_threshold = 1980
        self.cutoff_date = '2019-01-01'
        self.condition_start_date_where_clause = '(condition_start_date where clause)'
        self.condition_end_date_where_clause = '(condition_end_date where clause)'
        self.observation_query = 'SELECT * FROM observation'
        self.condition_query = 'SELECT * FROM condition'
        self.condition_field_names = [
            'condition_concept_id', 'condition_start_date',
            'condition_start_datetime', 'condition_end_date',
            'condition_end_datetime'
        ]
        self.condition_date_field_names = [
            'condition_start_date', 'condition_start_datetime',
            'condition_end_date', 'condition_end_datetime'
        ]
        self.col_expression = 'col_expression'

    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain_fields')
    def test_get_date_fields(self, mock_get_domain_fields):
        mock_get_domain_fields.side_effect = [self.condition_field_names]
        actual = remove_records_with_wrong_date.get_date_fields(
            self.condition_occurrence)
        self.assertCountEqual(self.condition_date_field_names, actual)

    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.is_field_required')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain_fields')
    def test_generate_field_expr(self, mock_get_domain_fields,
                                 mock_is_field_required):
        mock_get_domain_fields.side_effect = [
            self.condition_field_names, self.condition_field_names
        ]
        mock_is_field_required.side_effect = [True, True, False, False]

        actual = remove_records_with_wrong_date.generate_field_expr(
            self.condition_occurrence, self.year_threshold, self.cutoff_date)

        condition_end_date_col_expr = NULLABLE_DATE_FIELD_EXPRESSION.format(
            date_field_name=self.condition_end_date,
            year_threshold=self.year_threshold,
            cutoff_date=self.cutoff_date)
        condition_end_datetime_col_expr = NULLABLE_DATE_FIELD_EXPRESSION.format(
            date_field_name=self.condition_end_datetime,
            year_threshold=self.year_threshold,
            cutoff_date=self.cutoff_date)
        expected_col_expr_list = [
            self.condition_concept_id, self.condition_start_date,
            self.condition_start_datetime, condition_end_date_col_expr,
            condition_end_datetime_col_expr
        ]

        self.assertEqual(','.join(expected_col_expr_list), actual)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.generate_field_expr'
    )
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.is_field_required')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.get_date_fields'
    )
    def test_parse_remove_records_with_wrong_date_query(
        self, mock_get_date_fields, mock_is_field_required,
        mock_generate_field_expr):

        mock_get_date_fields.side_effect = [self.condition_date_field_names]
        mock_is_field_required.side_effect = [True, True, False, False]
        mock_generate_field_expr.side_effect = [self.col_expression]

        actual = remove_records_with_wrong_date.parse_remove_records_with_wrong_date_query(
            self.project_id, self.dataset_id, self.condition_occurrence,
            self.year_threshold, self.cutoff_date)

        condition_start_date_where_clause = WHERE_CLAUSE_REQUIRED_FIELD.format(
            date_field_name=self.condition_start_date,
            year_threshold=self.year_threshold,
            cutoff_date=self.cutoff_date)

        condition_start_datetime_where_clause = WHERE_CLAUSE_REQUIRED_FIELD.format(
            date_field_name=self.condition_start_datetime,
            year_threshold=self.year_threshold,
            cutoff_date=self.cutoff_date)

        expected = remove_records_with_wrong_date.REMOVE_RECORDS_WITH_WRONG_DATE_FIELD_TEMPLATE.format(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.condition_occurrence,
            col_expr=self.col_expression,
            where_clause=condition_start_date_where_clause +
            remove_records_with_wrong_date.AND +
            condition_start_datetime_where_clause)

        self.assertEqual(expected, actual)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.DOMAIN_TABLES_EXCEPT_OBSERVATION'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.parse_remove_records_with_wrong_date_query'
    )
    def test_get_remove_records_with_wrong_date_queries(
        self, mock_parse_remove_records_with_wrong_date_query,
        mock_domain_tables):
        mock_parse_remove_records_with_wrong_date_query.side_effect = [
            self.observation_query, self.condition_query
        ]
        mock_domain_tables.__iter__.return_value = [self.condition_occurrence]

        actual = remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries(
            self.project_id, self.dataset_id, None, self.cutoff_date,
            DEFAULT_YEAR_THRESHOLD, OBSERVATION_DEFAULT_YEAR_THRESHOLD)

        expected_queries = []

        query = dict()
        query[cdr_consts.QUERY] = self.observation_query
        query[cdr_consts.DESTINATION_TABLE] = OBSERVATION_TABLE
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        query[cdr_consts.BATCH] = True
        expected_queries.append(query)

        query = dict()
        query[cdr_consts.QUERY] = self.condition_query
        query[cdr_consts.DESTINATION_TABLE] = self.condition_occurrence
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        query[cdr_consts.BATCH] = True
        expected_queries.append(query)

        self.assertCountEqual(expected_queries, actual)

        args, _ = mock_parse_remove_records_with_wrong_date_query.call_args_list[
            0]
        self.assertEqual((self.project_id, self.dataset_id, OBSERVATION_TABLE,
                          OBSERVATION_DEFAULT_YEAR_THRESHOLD, self.cutoff_date),
                         args)

        args, _ = mock_parse_remove_records_with_wrong_date_query.call_args_list[
            1]
        self.assertEqual(
            (self.project_id, self.dataset_id, self.condition_occurrence,
             DEFAULT_YEAR_THRESHOLD, self.cutoff_date), args)
