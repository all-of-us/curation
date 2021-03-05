import re
import unittest

import mock

from cdr_cleaner.cleaning_rules.fill_source_value_text_fields import (
    FillSourceValueTextFields, get_fields_dict, get_modified_columns,
    get_full_join_expression)
from constants.cdr_cleaner import clean_cdr as cdr_consts


class FillSourceValueTextFieldsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project_id'
        self.dataset_id = 'dataset_id'
        self.sandbox_id = 'sandbox_id'
        self.client = None

        self.rule_instance = FillSourceValueTextFields(self.project_id,
                                                       self.dataset_id,
                                                       self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

        self.fields = [
            'visit_occurrence_id', 'visit_source_value',
            'visit_source_concept_id', 'admitting_source_concept_id',
            'admitting_source_value'
        ]
        self.fields_resource = [{
            'name': 'visit_occurrence_id'
        }, {
            'name': 'visit_source_value'
        }, {
            'name': 'visit_source_concept_id'
        }, {
            'name': 'admitting_source_concept_id'
        }, {
            'name': 'admitting_source_value'
        }]
        self.cdm_tables = ['visit_occurrence']
        self.table = 'visit_occurrence'
        self.fields_dict = {
            'admitting_source_value': {
                'prefix': 'adm_5',
                'name': 'admitting_source_value',
                'join_field': 'admitting_source_concept_id'
            },
            'visit_source_value': {
                'prefix': 'vis_2',
                'name': 'visit_source_value',
                'join_field': 'visit_source_concept_id'
            }
        }
        self.cols = 'visit_occurrence_id, vis_2.concept_code as visit_source_value, visit_source_concept_id, ' \
                    'admitting_source_concept_id, adm_5.concept_code as admitting_source_value'
        self.join_expression = 'LEFT JOIN `test_project_id.dataset_id.concept` as ' \
                               'adm_5 on m.admitting_source_concept_id = adm_5.concept_id  LEFT JOIN ' \
                               '`test_project_id.dataset_id.concept` as vis_2 on ' \
                               'm.visit_source_concept_id = vis_2.concept_id '
        self.expected_query = [{
            'query':
                'select visit_occurrence_id,'
                ' vis_2.concept_code as visit_source_value, visit_source_concept_id, '
                'admitting_source_concept_id, adm_5.concept_code as admitting_source_value '
                'from `test_project_id.dataset_id.visit_occurrence` as m '
                'LEFT JOIN `test_project_id.dataset_id.concept` as '
                'adm_5 on m.admitting_source_concept_id = adm_5.concept_id  LEFT JOIN '
                '`test_project_id.dataset_id.concept` as vis_2 on '
                'm.visit_source_concept_id = vis_2.concept_id ',
            'destination_table_id': 'visit_occurrence',
            'write_disposition': 'WRITE_TRUNCATE',
            'destination_dataset_id': 'dataset_id'
        }]

        self.chars_to_replace = '[\t\n\\s]+'
        self.single_space = ' '

    def test_get_fields_dict(self):
        expected = self.fields_dict
        actual = get_fields_dict(self.table, self.fields)
        self.assertDictEqual(actual, expected)

    def test_get_modified_columns(self):
        expected = self.cols
        actual = get_modified_columns(self.fields, self.fields_dict)
        self.assertEqual(actual, expected)

    def test_get_full_join_expression(self):
        expected = self.join_expression
        actual = get_full_join_expression(self.dataset_id, self.project_id,
                                          self.fields_dict)
        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space, actual),
            re.sub(self.chars_to_replace, self.single_space, expected))

    @mock.patch(
        'cdr_cleaner.cleaning_rules.fill_source_value_text_fields.get_full_join_expression'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.fill_source_value_text_fields.get_modified_columns'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.fill_source_value_text_fields.get_fields_dict'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.fill_source_value_text_fields.resources.fields_for'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.fill_source_value_text_fields.resources.CDM_TABLES'
    )
    def test_get_query_specs(self, mock_cdm_tables, mock_fields_for,
                             mock_get_fields_dict, mock_get_modified_columns,
                             mock_get_full_join_expression):
        mock_cdm_tables.__iter__.return_value = self.cdm_tables
        mock_fields_for.__iter__.return_value = self.fields_resource
        mock_get_fields_dict.return_value = self.fields_dict
        mock_get_modified_columns.return_value = self.cols
        mock_get_full_join_expression.return_value = self.join_expression

        mock_fields_for.call_any_args('visit_occurrence')

        self.assertEqual(
            self.rule_instance.affected_datasets,
            [cdr_consts.DEID_BASE, cdr_consts.CONTROLLED_TIER_DEID_BASE])

        expected = self.expected_query

        actual = FillSourceValueTextFields(self.project_id, self.dataset_id,
                                           self.sandbox_id).get_query_specs()
        self.assertEqual(len(actual), len(expected))
        self.assertEqual(
            re.sub(self.chars_to_replace, self.single_space,
                   actual[0]['query']),
            re.sub(self.chars_to_replace, self.single_space,
                   expected[0]['query']))
        self.assertEqual(actual[0]['destination_table_id'],
                         expected[0]['destination_table_id'])
        self.assertEqual(actual[0]['destination_dataset_id'],
                         expected[0]['destination_dataset_id'])
        self.assertEqual(actual[0]['write_disposition'],
                         expected[0]['write_disposition'])
