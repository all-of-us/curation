import re
import unittest
from collections import OrderedDict

from mock import patch, MagicMock

import cdr_cleaner.cleaning_rules.domain_alignment as domain_alignment
from cdr_cleaner.cleaning_rules.domain_alignment import (
    WHEN_STATEMENT, DOMAIN_ALIGNMENT_TABLE_NAME, CASE_STATEMENT,
    SRC_FIELD_AS_DEST_FIELD)
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts


class DomainAlignmentTest(unittest.TestCase):

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
        self.procedure_table = 'procedure_occurrence'
        self.condition_occurrence_id = 'condition_occurrence_id'
        self.procedure_occurrence_id = 'procedure_occurrence_id'
        self.procedure = 'Procedure'
        self.condition = 'Condition'
        self.condition_concept_id = 'condition_concept_id'
        self.procedure_concept_id = 'procedure_concept_id'
        self.condition_type_concept_id = 'condition_type_concept_id'
        self.procedure_type_concept_id = 'procedure_type_concept_id'
        self.primary_procedure_concept_id = 44786630
        self.primary_condition_concept_id = 44786627
        self.rerouting_criteria = '(1 = 1)'
        self.domain_tables = [self.condition_table, self.procedure_table]
        self.condition_condition_alias = 'condition_concept_id AS condition_concept_id'
        self.condition_procedure_alias = 'condition_concept_id AS procedure_concept_id'
        self.chars_to_replace = '[\t\n\\s]+'
        self.single_space = ' '

        self.mock_domain_table_names_patcher = patch(
            'cdr_cleaner.cleaning_rules.domain_alignment.domain_mapping.DOMAIN_TABLE_NAMES'
        )
        self.mock_domain_table_names = self.mock_domain_table_names_patcher.start(
        )
        self.mock_domain_table_names.__iter__.return_value = self.domain_tables

    def tearDown(self):
        self.mock_domain_table_names_patcher.stop()

    @patch('cdr_cleaner.cleaning_rules.domain_alignment.resolve_field_mappings')
    @patch('resources.get_domain_id_field')
    @patch('cdr_cleaner.cleaning_rules.domain_mapping.exist_domain_mappings')
    def test_parse_reroute_domain_query(self, mock_exist_domain_mappings,
                                        mock_get_domain_id_field,
                                        mock_resolve_field_mappings):
        mock_exist_domain_mappings.side_effect = [True]
        mock_resolve_field_mappings.side_effect = [
            self.condition_condition_alias, self.condition_procedure_alias
        ]
        mock_get_domain_id_field.side_effect = [
            self.condition_occurrence_id, self.condition_occurrence_id,
            self.procedure_occurrence_id, self.condition_occurrence_id
        ]

        actual_query = domain_alignment.parse_reroute_domain_query(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            dest_table=self.condition_table)

        self.assertEqual(mock_resolve_field_mappings.call_count, 2)
        mock_resolve_field_mappings.assert_any_call(self.procedure_table,
                                                    self.condition_table)
        mock_resolve_field_mappings.assert_any_call(self.condition_table,
                                                    self.condition_table)

        expected_query = domain_alignment. \
            SELECT_DOMAIN_RECORD_QUERY. \
            render(project_id=self.project_id,
                   dataset_id=self.dataset_id,
                   dest_table=self.condition_table,
                   dest_domain_id_field=self.condition_occurrence_id,
                   field_mapping_expr=self.condition_condition_alias)
        expected_query += domain_alignment.UNION_ALL
        expected_query += domain_alignment.REROUTE_DOMAIN_RECORD_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            src_table=self.procedure_table,
            dest_table=self.condition_table,
            src_domain_id_field=self.procedure_occurrence_id,
            dest_domain_id_field=self.condition_occurrence_id,
            _logging_domain_alignment=domain_alignment.
            DOMAIN_ALIGNMENT_TABLE_NAME,
            field_mapping_expr=self.condition_procedure_alias)

        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, actual_query),
            re.sub(self.chars_to_replace, self.single_space, expected_query))

    @patch('resources.get_domain')
    @patch('resources.get_domain_id_field')
    @patch('resources.get_domain_concept_id')
    def test_parse_mapping_id_query_for_same_domains(self,
                                                     mock_get_domain_concept_id,
                                                     mock_get_domain_id_field,
                                                     mock_get_domain):
        mock_get_domain.side_effect = [self.condition, self.procedure]
        mock_get_domain_id_field.side_effect = [
            self.condition_occurrence_id, self.procedure_occurrence_id
        ]
        mock_get_domain_concept_id.side_effect = [
            self.condition_concept_id, self.procedure_concept_id
        ]

        actual_query = domain_alignment.parse_domain_mapping_query_for_same_domains(
            self.project_id, self.dataset_id)

        expected_query = domain_alignment.DOMAIN_REROUTE_INCLUDED_INNER_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            src_table=self.condition_table,
            dest_table=self.condition_table,
            src_id=self.condition_occurrence_id,
            dest_id=self.condition_occurrence_id,
            domain_concept_id=self.condition_concept_id,
            domain='\'{}\''.format('\',\''.join(
                [self.condition, domain_alignment.METADATA_DOMAIN])))

        expected_query += domain_alignment.UNION_ALL
        expected_query += domain_alignment.DOMAIN_REROUTE_INCLUDED_INNER_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            src_table=self.procedure_table,
            dest_table=self.procedure_table,
            src_id=self.procedure_occurrence_id,
            dest_id=self.procedure_occurrence_id,
            domain_concept_id=self.procedure_concept_id,
            domain='\'{}\''.format('\',\''.join(
                [self.procedure, domain_alignment.METADATA_DOMAIN])))

        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, actual_query),
            re.sub(self.chars_to_replace, self.single_space, expected_query))

    @patch('cdr_cleaner.cleaning_rules.domain_mapping.get_rerouting_criteria')
    @patch('cdr_cleaner.cleaning_rules.domain_mapping.exist_domain_mappings')
    @patch('resources.get_domain_concept_id')
    @patch('resources.get_domain_id_field')
    @patch('resources.get_domain')
    def test_parse_domain_mapping_query_cross_domain(
        self, mock_get_domain, mock_get_domain_id_field,
        mock_get_domain_concept_id, mock_exist_domain_mappings,
        mock_get_rerouting_criteria):
        mock_get_domain.side_effect = [self.condition, self.procedure]
        mock_get_domain_id_field.side_effect = [
            self.condition_occurrence_id, self.procedure_occurrence_id
        ]
        mock_get_domain_concept_id.side_effect = [self.procedure_concept_id]
        mock_exist_domain_mappings.side_effect = [True]
        mock_get_rerouting_criteria.side_effect = [self.rerouting_criteria]

        actual_query = domain_alignment.parse_domain_mapping_query_cross_domain(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            dest_table=self.condition_table)

        expected_inner_query = domain_alignment.DOMAIN_REROUTE_INCLUDED_INNER_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            src_table=self.procedure_table,
            dest_table=self.condition_table,
            src_id=self.procedure_occurrence_id,
            dest_id=domain_alignment.NULL_VALUE,
            domain_concept_id=self.procedure_concept_id,
            domain='\'{}\''.format(self.condition))

        expected_inner_query += domain_alignment.AND + self.rerouting_criteria

        expected_maximum_id_query = domain_alignment.MAXIMUM_DOMAIN_ID_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            domain_table=self.condition_table,
            domain_id_field=self.condition_occurrence_id)

        expected_query = domain_alignment.DOMAIN_MAPPING_OUTER_QUERY.render(
            union_query=expected_inner_query,
            domain_query=expected_maximum_id_query)

        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, actual_query),
            re.sub(self.chars_to_replace, self.single_space, expected_query))

    @patch('resources.get_domain_id_field')
    def test_parse_domain_mapping_query_for_excluded_records(
        self, mock_get_domain_id_field):
        mock_get_domain_id_field.side_effect = [
            self.condition_occurrence_id, self.procedure_occurrence_id
        ]

        expected_query = domain_alignment.DOMAIN_REROUTE_EXCLUDED_INNER_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            src_table=self.condition_table,
            src_id=self.condition_occurrence_id,
            src_domain_id_field=self.condition_occurrence_id)

        expected_query += domain_alignment.UNION_ALL

        expected_query += domain_alignment.DOMAIN_REROUTE_EXCLUDED_INNER_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            src_table=self.procedure_table,
            src_id=self.procedure_occurrence_id,
            src_domain_id_field=self.procedure_occurrence_id)

        actual_query = domain_alignment.parse_domain_mapping_query_for_excluded_records(
            self.project_id, self.dataset_id)

        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, actual_query),
            re.sub(self.chars_to_replace, self.single_space, expected_query))

    @patch('cdr_cleaner.cleaning_rules.field_mapping.is_field_required')
    @patch('cdr_cleaner.cleaning_rules.domain_mapping.get_value_mappings')
    @patch(
        'cdr_cleaner.cleaning_rules.domain_mapping.value_requires_translation')
    @patch('cdr_cleaner.cleaning_rules.domain_mapping.get_field_mappings')
    def test_resolve_field_mappings_value_requires_translation(
        self, mock_get_field_mappings, mock_value_requires_translation,
        mock_get_value_mappings, mock_is_field_required):
        get_field_mappings_return_value = OrderedDict()
        get_field_mappings_return_value[
            self.procedure_concept_id] = self.condition_concept_id
        get_field_mappings_return_value[
            self.procedure_type_concept_id] = self.condition_type_concept_id

        mock_get_field_mappings.return_value = get_field_mappings_return_value
        mock_value_requires_translation.side_effect = [True, True]
        mock_is_field_required.side_effect = [False, True]
        mock_get_value_mappings.return_value = dict()

        expected = ',\n\t'.join([
            domain_alignment.NULL_AS_DEST_FIELD.format(
                dest_field=self.procedure_concept_id),
            domain_alignment.ZERO_AS_DEST_FIELD.format(
                dest_field=self.procedure_type_concept_id)
        ])

        actual = domain_alignment.resolve_field_mappings(
            self.condition_table, self.procedure_table)

        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, expected),
            re.sub(self.chars_to_replace, self.single_space, actual))

    @patch('cdr_cleaner.cleaning_rules.domain_mapping.get_value_mappings')
    @patch(
        'cdr_cleaner.cleaning_rules.domain_mapping.value_requires_translation')
    @patch('cdr_cleaner.cleaning_rules.domain_mapping.get_field_mappings')
    def test_resolve_field_mappings(self, mock_get_field_mappings,
                                    mock_value_requires_translation,
                                    mock_get_value_mappings):
        get_field_mappings_return_value = OrderedDict()
        get_field_mappings_return_value[
            self.procedure_concept_id] = self.condition_concept_id
        get_field_mappings_return_value[
            self.procedure_type_concept_id] = self.condition_type_concept_id

        mock_get_field_mappings.return_value = get_field_mappings_return_value
        mock_value_requires_translation.side_effect = [False, True]
        mock_get_value_mappings.return_value = {
            self.primary_procedure_concept_id: self.primary_condition_concept_id
        }

        actual = domain_alignment.resolve_field_mappings(
            self.condition_table, self.procedure_table)

        select_field_1 = SRC_FIELD_AS_DEST_FIELD.format(
            src_field=self.condition_concept_id,
            dest_field=self.procedure_concept_id)

        select_field_2 = CASE_STATEMENT.format(
            src_field=self.condition_type_concept_id,
            statements=WHEN_STATEMENT.format(
                src_value=self.primary_condition_concept_id,
                dest_value=self.primary_procedure_concept_id),
            dest_field=self.procedure_type_concept_id)

        expected = ',\n\t'.join([select_field_1, select_field_2])

        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, expected),
            re.sub(self.chars_to_replace, self.single_space, actual))

        mock_get_field_mappings.assert_called_once_with(self.condition_table,
                                                        self.procedure_table)

        mock_value_requires_translation.assert_any_call(
            self.condition_table, self.procedure_table,
            self.condition_concept_id, self.procedure_concept_id)
        mock_value_requires_translation.assert_any_call(
            self.condition_table, self.procedure_table,
            self.condition_type_concept_id, self.procedure_type_concept_id)

        mock_get_value_mappings.assert_called_once_with(
            self.condition_table, self.procedure_table,
            self.condition_type_concept_id, self.procedure_type_concept_id)

        self.assertEqual(mock_value_requires_translation.call_count, 2)
        self.assertEqual(mock_get_value_mappings.call_count, 1)

    @patch(
        'cdr_cleaner.cleaning_rules.domain_alignment.parse_domain_mapping_query_for_excluded_records'
    )
    @patch(
        'cdr_cleaner.cleaning_rules.domain_alignment.parse_domain_mapping_query_for_same_domains'
    )
    @patch(
        'cdr_cleaner.cleaning_rules.domain_alignment.parse_domain_mapping_query_cross_domain'
    )
    @patch('cdr_cleaner.cleaning_rules.domain_alignment.bq.get_client')
    @patch('cdr_cleaner.cleaning_rules.domain_alignment.bq.create_tables')
    def test_get_domain_mapping_queries(
        self, mock_create_tables, mock_bq_client,
        mock_parse_domain_mapping_query_cross_domain,
        mock_parse_domain_mapping_query_for_same_domains,
        mock_parse_domain_mapping_query_for_excluded_records):
        bq_client = MagicMock()
        mock_bq_client.return_value = bq_client
        bq_client.delete_table = MagicMock()

        # Fake the queries returned by the other functions inside of get_domain_mapping_queries
        cross_domain_query_condition = 'SELECT cross_domain_query_condition'
        cross_domain_query_procedure = 'SELECT cross_domain_query_procedure'
        same_domain_query = 'SELECT same_domain_query_condition ' \
                            'UNION ALL ' \
                            'SELECT same_domain_query_procedure'

        excluded_records_query = 'SELECT excluded_record_query_condition ' \
                                 'UNION ALL ' \
                                 'SELECT excluded_record_query_procedure'

        # Mock the behaviors of the function calls inside of get_domain_mapping_queries
        mock_parse_domain_mapping_query_cross_domain.side_effect = [
            cross_domain_query_condition, cross_domain_query_procedure
        ]
        mock_parse_domain_mapping_query_for_same_domains.return_value = same_domain_query
        mock_parse_domain_mapping_query_for_excluded_records.return_value = excluded_records_query

        # Define the expected queries
        expected_query = {
            cdr_consts.QUERY:
                domain_alignment.UNION_ALL.join([
                    cross_domain_query_condition,
                    cross_domain_query_procedure,
                    same_domain_query,
                    excluded_records_query,
                ]),
            cdr_consts.DESTINATION_TABLE:
                DOMAIN_ALIGNMENT_TABLE_NAME,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_EMPTY,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id
        }

        actual_queries = domain_alignment.get_domain_mapping_queries(
            self.project_id, self.dataset_id)
        actual_query = actual_queries[0]

        # Test the content of the expected and actual queries
        self.assertDictEqual(expected_query, actual_query)

        mock_bq_client.assert_called_once_with(self.project_id)
        fake_table = f'{self.project_id}.{self.dataset_id}.{DOMAIN_ALIGNMENT_TABLE_NAME}'
        bq_client.delete_table.assert_called_once_with(fake_table,
                                                       not_found_ok=True)
        mock_create_tables.assert_called_once_with(bq_client,
                                                   self.project_id,
                                                   [fake_table],
                                                   exists_ok=False)

        # Test the function calls with the corresponding arguments
        mock_parse_domain_mapping_query_cross_domain.assert_any_call(
            self.project_id, self.dataset_id, self.condition_table)
        mock_parse_domain_mapping_query_cross_domain.assert_any_call(
            self.project_id, self.dataset_id, self.procedure_table)
        mock_parse_domain_mapping_query_for_same_domains.assert_called_once_with(
            self.project_id, self.dataset_id)
        mock_parse_domain_mapping_query_for_excluded_records.assert_called_once_with(
            self.project_id, self.dataset_id)

    @patch(
        'cdr_cleaner.cleaning_rules.domain_alignment.parse_reroute_domain_query'
    )
    def test_get_reroute_domain_queries(self, mock_parse_reroute_domain_query):
        reroute_domain_query_condition = 'SELECT reroute_domain_query_condition'
        reroute_domain_query_procedure = 'SELECT reroute_domain_query_procedure'

        mock_parse_reroute_domain_query.side_effect = [
            reroute_domain_query_condition, reroute_domain_query_procedure
        ]

        # Define the expected queries
        expected_queries = [{
            cdr_consts.QUERY: reroute_domain_query_condition,
            cdr_consts.DESTINATION_TABLE: self.condition_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id,
            cdr_consts.BATCH: True
        }, {
            cdr_consts.QUERY: reroute_domain_query_procedure,
            cdr_consts.DESTINATION_TABLE: self.procedure_table,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: self.dataset_id,
            cdr_consts.BATCH: True
        }]

        actual_queries = domain_alignment.get_reroute_domain_queries(
            self.project_id, self.dataset_id)

        self.assertCountEqual(expected_queries, actual_queries)
