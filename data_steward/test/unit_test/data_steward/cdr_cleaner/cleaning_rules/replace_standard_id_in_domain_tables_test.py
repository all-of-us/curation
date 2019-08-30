import unittest

from mock import mock, patch

import cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables as replace_standard_id
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables import SRC_CONCEPT_ID_TABLE_NAME, \
    SRC_CONCEPT_ID_MAPPING_QUERY, \
    SRC_CONCEPT_ID_UPDATE_QUERY, \
    UPDATE_MAPPING_TABLES_QUERY


class ReplaceStandardIdInDomainTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project_id'
        self.dataset_id = 'dataset_id'
        self.snapshot_dataset_id = 'snapshot_dataset_id'
        self.condition_table = 'condition_occurrence'
        self.domain_concept_id = 'condition_concept_id'
        self.domain_source_concept_id = 'condition_source_concept_id'
        self.mapping_table = '_mapping_condition_occurrence'
        self.src_concept_logging_table = '_logging_standard_concept_id_replacement'
        self.domain_tables = [self.condition_table]

        self.mock_domain_table_names_patcher = patch(
            'cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.DOMAIN_TABLE_NAMES')
        self.mock_domain_table_names = self.mock_domain_table_names_patcher.start()
        self.mock_domain_table_names.__iter__.return_value = self.domain_tables

    def tearDown(self):
        self.mock_domain_table_names_patcher.stop()

    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.get_mapping_table_update_queries')
    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.get_src_concept_id_update_queries')
    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.get_src_concept_id_logging_queries')
    def test_replace_standard_id_in_domain_tables(self,
                                                  mock_get_src_concept_id_logging_queries,
                                                  mock_get_src_concept_id_update_queries,
                                                  mock_get_mapping_table_update_queries):
        query = 'select this query'

        mock_get_src_concept_id_logging_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        mock_get_src_concept_id_update_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        mock_get_mapping_table_update_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        expected_query_list = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        actual_query_list = replace_standard_id.replace_standard_id_in_domain_tables(self.project_id, self.dataset_id)

        self.assertEqual(expected_query_list, actual_query_list)

        mock_get_src_concept_id_logging_queries.assert_any_call(self.project_id, self.dataset_id)
        mock_get_src_concept_id_update_queries.assert_any_call(self.project_id, self.dataset_id)
        mock_get_mapping_table_update_queries.assert_any_call(self.project_id, self.dataset_id)

        self.assertEqual(mock_get_src_concept_id_logging_queries.call_count, 1)
        self.assertEqual(mock_get_src_concept_id_update_queries.call_count, 1)
        self.assertEqual(mock_get_mapping_table_update_queries.call_count, 1)

    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.parse_duplicate_id_update_query')
    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.parse_src_concept_id_logging_query')
    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.bq_utils.create_standard_table')
    def test_get_src_concept_id_logging_queries(self,
                                                mock_create_standard_table,
                                                mock_parse_src_concept_id_logging_query,
                                                mock_parse_duplicate_id_update_query):
        mock_create_standard_table.return_value = {
            bq_consts.DATASET_REF: {bq_consts.DATASET_ID: self.dataset_id}
        }

        src_concept_id_mapping_query = 'SELECT DISTINCT \'condition_occurrence\' AS domain_table'
        duplicate_id_update_query = 'UPDATE `test_project_id.dataset_id._logging_standard_concept_id_replacement'

        mock_parse_src_concept_id_logging_query.return_value = src_concept_id_mapping_query
        mock_parse_duplicate_id_update_query.return_value = duplicate_id_update_query

        # Define the expected queries
        expected_queries = [{
            cdr_consts.QUERY: src_concept_id_mapping_query,
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: duplicate_id_update_query,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        actual_queries = replace_standard_id.get_src_concept_id_logging_queries(self.project_id,
                                                                                self.dataset_id)

        # Test the content of the expected and actual queries
        self.assertItemsEqual(expected_queries, actual_queries)

        mock_create_standard_table.assert_called_once_with(SRC_CONCEPT_ID_TABLE_NAME,
                                                           SRC_CONCEPT_ID_TABLE_NAME,
                                                           drop_existing=True,
                                                           dataset_id=self.dataset_id)

        mock_parse_src_concept_id_logging_query.assert_called_once_with(self.project_id,
                                                                        self.dataset_id,
                                                                        self.condition_table)
        mock_parse_duplicate_id_update_query.assert_called_once_with(self.project_id,
                                                                     self.dataset_id,
                                                                     self.condition_table)

    @mock.patch('resources.get_domain_source_concept_id')
    @mock.patch('resources.get_domain_concept_id')
    def test_parse_src_concept_id_logging_query(self,
                                                mock_get_domain_concept_id,
                                                mock_get_domain_source_concept_id):
        mock_get_domain_concept_id.return_value = self.domain_concept_id
        mock_get_domain_source_concept_id.return_value = self.domain_source_concept_id

        expected_query = SRC_CONCEPT_ID_MAPPING_QUERY.format(table_name=self.condition_table,
                                                             project=self.project_id,
                                                             dataset=self.dataset_id,
                                                             domain_concept_id=self.domain_concept_id,
                                                             domain_source=self.domain_source_concept_id)

        actual_query = replace_standard_id.parse_src_concept_id_logging_query(self.project_id,
                                                                              self.dataset_id,
                                                                              self.condition_table)

        self.assertItemsEqual(expected_query, actual_query)

    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.parse_src_concept_id_update_query')
    def test_get_src_concept_id_update_queries(self,
                                               mock_parse_src_concept_id_update_query):
        src_concept_update_query = 'select a random value'
        mock_parse_src_concept_id_update_query.return_value = src_concept_update_query

        expected_query = [{
            cdr_consts.QUERY: src_concept_update_query,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        actual_query = replace_standard_id.get_src_concept_id_update_queries(self.project_id, self.dataset_id)

        self.assertItemsEqual(actual_query, expected_query)

    @mock.patch('resources.get_domain_source_concept_id')
    @mock.patch('resources.get_domain_concept_id')
    @mock.patch('resources.get_domain_id_field')
    @mock.patch('resources.fields_for')
    def test_parse_src_concept_id_update_query(self,
                                               mock_fields_for,
                                               mock_get_domain_id_field,
                                               mock_get_domain_concept_id,
                                               mock_get_domain_source_concept_id):
        mock_fields_for.return_value = [{'name': 'condition_occurrence_id'}, {'name': 'condition_concept_id'},
                                        {'name': 'condition_source_concept_id'}, {'name': 'condition_source_value'}]
        mock_get_domain_id_field.return_value = 'condition_occurrence_id'
        mock_get_domain_concept_id.return_value = self.domain_concept_id
        mock_get_domain_source_concept_id.return_value = self.domain_source_concept_id
        cols = 'coalesce(dest_id, condition_occurrence_id) AS condition_occurrence_id, ' \
               'coalesce(new_concept_id, condition_concept_id) AS condition_concept_id, ' \
               'coalesce(new_src_concept_id, condition_source_concept_id) AS condition_source_concept_id, ' \
               'condition_source_value'

        expected_query = SRC_CONCEPT_ID_UPDATE_QUERY.format(cols=cols,
                                                            project=self.project_id,
                                                            dataset=self.dataset_id,
                                                            domain_table=self.condition_table,
                                                            logging_table=SRC_CONCEPT_ID_TABLE_NAME
                                                            )

        actual_query = replace_standard_id.parse_src_concept_id_update_query(self.project_id,
                                                                             self.dataset_id,
                                                                             self.condition_table)

        self.assertEqual(actual_query, expected_query)

    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.parse_mapping_table_update_query')
    @mock.patch('cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.mapping_table_for')
    def test_get_mapping_table_update_queries(self,
                                              mock_mapping_table_for,
                                              mock_parse_mapping_table_update_query):
        mock_mapping_table_for.return_value = self.mapping_table

        expected_table = self.mapping_table
        actual_table = replace_standard_id.mapping_table_for(self.condition_table)

        self.assertEqual(actual_table, expected_table)

        src_concept_update_query = 'select a random value'
        mock_parse_mapping_table_update_query.return_value = src_concept_update_query

        expected_query = [{
            cdr_consts.QUERY: src_concept_update_query,
            cdr_consts.DESTINATION_TABLE: self.mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        actual_query = replace_standard_id.get_mapping_table_update_queries(self.project_id, self.dataset_id)

        self.assertItemsEqual(actual_query, expected_query)

    @mock.patch('resources.fields_for')
    def test_parse_mapping_table_update_query(self,
                                              mock_fields_for):
        mock_fields_for.return_value = [{'name': 'condition_occurrence_id'}, {'name': 'src_condition_occurrence_id'}]
        cols = 'coalesce(dest_id, condition_occurrence_id) AS condition_occurrence_id, src_condition_occurrence_id'

        expected_query = UPDATE_MAPPING_TABLES_QUERY.format(cols=cols,
                                                            project=self.project_id,
                                                            dataset=self.dataset_id,
                                                            mapping_table=self.mapping_table,
                                                            logging_table=SRC_CONCEPT_ID_TABLE_NAME,
                                                            domain_table=self.condition_table
                                                            )

        actual_query = replace_standard_id.parse_mapping_table_update_query(self.project_id,
                                                                            self.dataset_id,
                                                                            self.condition_table,
                                                                            self.mapping_table, )
        self.assertEqual(actual_query, expected_query)
