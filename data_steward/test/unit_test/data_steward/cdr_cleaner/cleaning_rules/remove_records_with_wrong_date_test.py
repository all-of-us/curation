import unittest
from mock import mock

from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import OBSERVATION_TABLE
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import DEFAULT_YEAR_THRESHOLD
from cdr_cleaner.cleaning_rules.remove_records_with_wrong_date import OBSERVATION_DEFAULT_YEAR_THRESHOLD
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
        self.condition_start_date = 'condition_start_date'
        self.condition_end_date = 'condition_end_date'
        self.year_threshold = 1980
        self.condition_start_date_where_clause = '(condition_start_date where clause)'
        self.condition_end_date_where_clause = '(condition_end_date where clause)'
        self.observation_query = 'SELECT * FROM observation'
        self.condition_query = 'SELECT * FROM condition'

    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.is_field_required')
    def test_generate_where_clause(self, mock_is_field_required):
        mock_is_field_required.side_effect = [True, False]

        actual_where_clause = remove_records_with_wrong_date.generate_where_clause(self.condition_occurrence,
                                                                                   self.condition_start_date,
                                                                                   self.year_threshold)
        expected_where_clause = remove_records_with_wrong_date.WHERE_CLAUSE_REQUIRED_FIELD.format(
            date_field_name=self.condition_start_date,
            year_threshold=self.year_threshold)

        self.assertEqual(expected_where_clause, actual_where_clause)

        actual_where_clause = remove_records_with_wrong_date.generate_where_clause(self.condition_occurrence,
                                                                                   self.condition_end_date,
                                                                                   self.year_threshold)

        expected_where_clause = remove_records_with_wrong_date.WHERE_CLAUSE_NULLABLE_FIELD.format(
            date_field_name=self.condition_end_date,
            year_threshold=self.year_threshold)

        self.assertEqual(expected_where_clause, actual_where_clause)

        args, _ = mock_is_field_required.call_args_list[0]
        self.assertEqual((self.condition_occurrence, self.condition_start_date), args)

        args, _ = mock_is_field_required.call_args_list[1]
        self.assertEqual((self.condition_occurrence, self.condition_end_date), args)

        self.assertEqual(2, mock_is_field_required.call_count)

    @mock.patch('cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.generate_where_clause')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain_fields')
    def test_parse_remove_records_with_wrong_date_query(self, mock_get_domain_fields, mock_generate_where_clause):
        mock_get_domain_fields.return_value = [self.condition_start_date, self.condition_end_date]
        mock_generate_where_clause.side_effect = [self.condition_start_date_where_clause,
                                                  self.condition_end_date_where_clause]

        actual = remove_records_with_wrong_date.parse_remove_records_with_wrong_date_query(self.project_id,
                                                                                           self.dataset_id,
                                                                                           self.condition_occurrence,
                                                                                           self.year_threshold)

        expected = remove_records_with_wrong_date.REMOVE_RECORDS_WITH_WRONG_DATE_FIELD_TEMPLATE.format(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.condition_occurrence,
            where_clause=self.condition_start_date_where_clause + remove_records_with_wrong_date.AND + self.condition_end_date_where_clause)

        self.assertEqual(expected, actual)

    @mock.patch('cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.DOMAIN_TABLES_EXCEPT_OBSERVATION')
    @mock.patch('cdr_cleaner.cleaning_rules.remove_records_with_wrong_date.parse_remove_records_with_wrong_date_query')
    def test_get_remove_records_with_wrong_date_queries(self, mock_parse_remove_records_with_wrong_date_query,
                                                        mock_domain_tables):
        mock_parse_remove_records_with_wrong_date_query.side_effect = [self.observation_query,
                                                                       self.condition_query]
        mock_domain_tables.__iter__.return_value = [self.condition_occurrence]

        actual = remove_records_with_wrong_date.get_remove_records_with_wrong_date_queries(self.project_id,
                                                                                           self.dataset_id,
                                                                                           DEFAULT_YEAR_THRESHOLD,
                                                                                           OBSERVATION_DEFAULT_YEAR_THRESHOLD)

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

        self.assertItemsEqual(expected_queries, actual)

        args, _ = mock_parse_remove_records_with_wrong_date_query.call_args_list[0]
        self.assertEqual((self.project_id, self.dataset_id, OBSERVATION_TABLE, OBSERVATION_DEFAULT_YEAR_THRESHOLD),
                         args)

        args, _ = mock_parse_remove_records_with_wrong_date_query.call_args_list[1]
        self.assertEqual((self.project_id, self.dataset_id, self.condition_occurrence, DEFAULT_YEAR_THRESHOLD),
                         args)
