from __future__ import print_function
import unittest

import mock
from mock import patch

from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables import (
    SRC_CONCEPT_ID_TABLE_NAME, SRC_CONCEPT_ID_UPDATE_QUERY,
    UPDATE_MAPPING_TABLES_QUERY, ReplaceWithStandardConceptId)


class ReplaceStandardIdInDomainTablesTest(unittest.TestCase):

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

        self.rule_instance = ReplaceWithStandardConceptId(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.condition_table = 'condition_occurrence'
        self.procedure_table = 'procedure_occurrence'
        self.sandbox_condition_table = 'sandbox_condition_table'
        self.sandbox_procedure_table = 'sandbox_procedure_table'
        self.domain_concept_id = 'condition_concept_id'
        self.domain_source_concept_id = 'condition_source_concept_id'
        self.condition_mapping_table = '_mapping_condition_occurrence'
        self.procedure_mapping_table = '_mapping_procedure_occurrence'
        self.src_concept_logging_table = '_logging_standard_concept_id_replacement'
        self.domain_tables = [self.condition_table, self.procedure_table]

        self.mock_domain_table_names_patcher = patch.object(
            ReplaceWithStandardConceptId, 'affected_tables')
        self.mock_domain_table_names = self.mock_domain_table_names_patcher.start(
        )
        self.mock_domain_table_names.__iter__.return_value = self.domain_tables

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def tearDown(self):
        self.mock_domain_table_names_patcher.stop()

    @mock.patch(
        'cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.get_delete_empty_sandbox_tables_queries'
    )
    @patch.object(ReplaceWithStandardConceptId,
                  'get_mapping_table_update_queries')
    @patch.object(ReplaceWithStandardConceptId,
                  'get_src_concept_id_update_queries')
    @patch.object(ReplaceWithStandardConceptId,
                  'get_sandbox_src_concept_id_update_queries')
    @patch.object(ReplaceWithStandardConceptId,
                  'get_src_concept_id_logging_queries')
    def test_replace_standard_id_in_domain_tables(
            self, mock_get_src_concept_id_logging_queries,
            mock_get_sandbox_src_concept_id_update_queries,
            mock_get_src_concept_id_update_queries,
            mock_get_mapping_table_update_queries,
            mock_get_delete_empty_sandbox_tables_queries):
        query = 'select this query'

        mock_get_src_concept_id_logging_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }]

        mock_get_sandbox_src_concept_id_update_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.sandbox_condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }]

        mock_get_src_concept_id_update_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        mock_get_mapping_table_update_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.condition_mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        mock_get_delete_empty_sandbox_tables_queries.return_value = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }]

        expected_query_list = [{
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.sandbox_condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_TABLE: self.condition_mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }]

        actual_query_list = self.rule_instance.get_query_specs()

        self.assertEqual(expected_query_list, actual_query_list)

        mock_get_src_concept_id_logging_queries.assert_any_call()
        mock_get_sandbox_src_concept_id_update_queries.assert_any_call()
        mock_get_src_concept_id_update_queries.assert_any_call()
        mock_get_mapping_table_update_queries.assert_any_call()

        self.assertEqual(mock_get_src_concept_id_logging_queries.call_count, 1)
        self.assertEqual(
            mock_get_sandbox_src_concept_id_update_queries.call_count, 1)
        self.assertEqual(mock_get_src_concept_id_update_queries.call_count, 1)
        self.assertEqual(mock_get_mapping_table_update_queries.call_count, 1)

    @patch.object(ReplaceWithStandardConceptId,
                  'parse_duplicate_id_update_query')
    @patch.object(ReplaceWithStandardConceptId,
                  'parse_src_concept_id_logging_query')
    def test_get_src_concept_id_logging_queries(
            self, mock_parse_src_concept_id_logging_query,
            mock_parse_duplicate_id_update_query):
        src_concept_id_logging_condition = 'condition UNION ALL procedure'
        duplicate_id_update_query = ('UPDATE `test_project_id.dataset_id'
                                     '._logging_standard_concept_id_replacement'
                                     'SELECT condition'
                                     'UNION ALL'
                                     'SELECT procedure')
        mock_parse_src_concept_id_logging_query.return_value = src_concept_id_logging_condition
        mock_parse_duplicate_id_update_query.return_value = duplicate_id_update_query

        # Define the expected queries
        expected_queries = [{
            cdr_consts.QUERY: src_concept_id_logging_condition,
            cdr_consts.DESTINATION_TABLE: SRC_CONCEPT_ID_TABLE_NAME,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }, {
            cdr_consts.QUERY: duplicate_id_update_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }]

        actual_queries = self.rule_instance.get_src_concept_id_logging_queries()

        # Test the content of the expected and actual queries
        self.assertCountEqual(expected_queries, actual_queries)

    @patch.object(ReplaceWithStandardConceptId, 'sandbox_table_for')
    @patch.object(ReplaceWithStandardConceptId,
                  'parse_sandbox_src_concept_id_update_query')
    def test_get_sandbox_src_concept_id_update_queries(
            self, mock_parse_sandbox_src_concept_id_update_query,
            mock_sandbox_table_for):
        sandbox_query_condition = 'condition sandbox query'
        sandbox_query_procedure = 'procedure sandbox query'
        condition_sandbox_table = 'condition_sandbox_table'
        procedure_sandbox_table = 'procedure_sandbox_table'

        mock_parse_sandbox_src_concept_id_update_query.side_effect = [
            sandbox_query_condition, sandbox_query_procedure
        ]
        mock_sandbox_table_for.side_effect = [
            condition_sandbox_table, procedure_sandbox_table
        ]

        expected_queries = [{
            cdr_consts.QUERY: sandbox_query_condition,
            cdr_consts.DESTINATION_TABLE: condition_sandbox_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }, {
            cdr_consts.QUERY: sandbox_query_procedure,
            cdr_consts.DESTINATION_TABLE: procedure_sandbox_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.sandbox_id
        }]

        actual_queries = self.rule_instance.get_sandbox_src_concept_id_update_queries(
        )

        # Test the content of the expected and actual queries
        self.assertCountEqual(expected_queries, actual_queries)

        self.assertEqual(mock_sandbox_table_for.call_count, 2)
        mock_sandbox_table_for.assert_any_call(self.condition_table)
        mock_sandbox_table_for.assert_called_with(self.procedure_table)

        self.assertEqual(
            mock_parse_sandbox_src_concept_id_update_query.call_count, 2)
        mock_parse_sandbox_src_concept_id_update_query.assert_any_call(
            self.condition_table)
        mock_parse_sandbox_src_concept_id_update_query.assert_called_with(
            self.procedure_table)

    @patch.object(ReplaceWithStandardConceptId,
                  'parse_src_concept_id_update_query')
    def test_get_src_concept_id_update_queries(
            self, mock_parse_src_concept_id_update_query):
        update_query_condition = 'select a random value'
        update_query_procedure = 'select a random value procedure'
        mock_parse_src_concept_id_update_query.side_effect = [
            update_query_condition, update_query_procedure
        ]

        expected_query = [{
            cdr_consts.QUERY: update_query_condition,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: update_query_procedure,
            cdr_consts.DESTINATION_TABLE: self.procedure_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        actual_query = self.rule_instance.get_src_concept_id_update_queries()

        self.assertCountEqual(actual_query, expected_query)

    @mock.patch('resources.get_domain_source_concept_id')
    @mock.patch('resources.get_domain_concept_id')
    @mock.patch('resources.get_domain_id_field')
    @mock.patch('resources.fields_for')
    def test_parse_src_concept_id_update_query(
            self, mock_fields_for, mock_get_domain_id_field,
            mock_get_domain_concept_id, mock_get_domain_source_concept_id):
        mock_fields_for.return_value = [{
            'name': 'condition_occurrence_id'
        }, {
            'name': 'condition_concept_id'
        }, {
            'name': 'condition_source_concept_id'
        }, {
            'name': 'condition_source_value'
        }]
        mock_get_domain_id_field.return_value = 'condition_occurrence_id'
        mock_get_domain_concept_id.return_value = self.domain_concept_id
        mock_get_domain_source_concept_id.return_value = self.domain_source_concept_id
        cols = 'coalesce(dest_id, condition_occurrence_id) AS condition_occurrence_id, ' \
               'coalesce(new_concept_id, condition_concept_id) AS condition_concept_id, ' \
               'coalesce(new_src_concept_id, condition_source_concept_id) AS condition_source_concept_id, ' \
               'condition_source_value'

        expected_query = SRC_CONCEPT_ID_UPDATE_QUERY.render(
            cols=cols,
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            domain_table=self.condition_table,
            logging_table=SRC_CONCEPT_ID_TABLE_NAME)

        actual_query = self.rule_instance.parse_src_concept_id_update_query(
            self.condition_table)

        self.assertEqual(actual_query, expected_query)

    @patch.object(ReplaceWithStandardConceptId,
                  'parse_mapping_table_update_query')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.replace_standard_id_in_domain_tables.mapping_table_for'
    )
    def test_get_mapping_table_update_queries(
            self, mock_mapping_table_for,
            mock_parse_mapping_table_update_query):
        mock_mapping_table_for.side_effect = [
            self.condition_mapping_table, self.procedure_mapping_table
        ]

        src_concept_update_query_condition = 'select a random value condition'
        src_concept_update_query_procedure = 'select a random value procedure'
        mock_parse_mapping_table_update_query.side_effect = [
            src_concept_update_query_condition,
            src_concept_update_query_procedure
        ]

        expected_query = [{
            cdr_consts.QUERY: src_concept_update_query_condition,
            cdr_consts.DESTINATION_TABLE: self.condition_mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }, {
            cdr_consts.QUERY: src_concept_update_query_procedure,
            cdr_consts.DESTINATION_TABLE: self.procedure_mapping_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id
        }]

        actual_query = self.rule_instance.get_mapping_table_update_queries()

        self.assertCountEqual(actual_query, expected_query)

        self.assertEqual(mock_mapping_table_for.call_count, 2)
        mock_mapping_table_for.assert_any_call(self.condition_table)
        mock_mapping_table_for.assert_called_with(self.procedure_table)

        self.assertEqual(mock_parse_mapping_table_update_query.call_count, 2)
        mock_parse_mapping_table_update_query.assert_any_call(
            self.condition_table, self.condition_mapping_table)
        mock_parse_mapping_table_update_query.assert_called_with(
            self.procedure_table, self.procedure_mapping_table)

    @mock.patch('resources.fields_for')
    def test_parse_mapping_table_update_query(self, mock_fields_for):
        mock_fields_for.return_value = [{
            'name': 'condition_occurrence_id'
        }, {
            'name': 'src_condition_occurrence_id'
        }]
        cols = (
            'coalesce(dest_id, condition_occurrence_id) AS condition_occurrence_id, '
            'src_condition_occurrence_id')

        expected_query = UPDATE_MAPPING_TABLES_QUERY.render(
            cols=cols,
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            mapping_table=self.condition_mapping_table,
            logging_table=SRC_CONCEPT_ID_TABLE_NAME,
            domain_table=self.condition_table)

        actual_query = self.rule_instance.parse_mapping_table_update_query(
            self.condition_table,
            self.condition_mapping_table,
        )
        self.assertEqual(actual_query, expected_query)
