import unittest

import mock
from mock import patch

from cdr_cleaner.cleaning_rules.domain_alignment import NULL_VALUE
from cdr_cleaner.cleaning_rules.field_mapping import DOMAIN_DATE_FIELDS, DOMAIN_SPECIFIC_FIELDS, \
    DOMAIN_COMMON_FIELDS
from cdr_cleaner.cleaning_rules import field_mapping as field_mapping


class FieldMappingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.condition_table = 'condition_occurrence'
        self.procedure_table = 'procedure_occurrence'
        self.condition_occurrence_id = 'condition_occurrence_id'
        self.procedure_occurrence_id = 'procedure_occurrence_id'
        self.condition_end_date = 'condition_end_date'
        self.domain_tables = [self.condition_table, self.procedure_table]
        self.procedure = 'Procedure'
        self.condition = 'Condition'
        self.condition_schema = [
            {"type": "integer", "name": "condition_occurrence_id", "mode": "required"},
            {"type": "integer", "name": "person_id", "mode": "required"},
            {"type": "integer", "name": "condition_concept_id", "mode": "required"},
            {"type": "date", "name": "condition_start_date", "mode": "required"},
            {"type": "timestamp", "name": "condition_start_datetime", "mode": "required"},
            {"type": "date", "name": "condition_end_date", "mode": "nullable"},
            {"type": "timestamp", "name": "condition_end_datetime", "mode": "required"},
            {"type": "string", "name": "condition_source_value", "mode": "required"}
        ]
        self.procedure_schema = [
            {"type": "integer", "name": "procedure_occurrence_id", "mode": "required"},
            {"type": "integer", "name": "person_id", "mode": "required"},
            {"type": "integer", "name": "procedure_concept_id", "mode": "required"},
            {"type": "date", "name": "procedure_date", "mode": "required"},
            {"type": "timestamp", "name": "procedure_datetime", "mode": "required"},
            {"type": "integer", "name": "modifier_concept_id", "mode": "required"},
            {"type": "string", "name": "procedure_source_value", "mode": "required"}
        ]

        self.cdm_schemas = {
            self.condition_table: self.condition_schema,
            self.procedure_table: self.procedure_schema
        }

        self.condition_occurrence_fields = ['person_id',
                                            'condition_concept_id',
                                            'condition_start_date',
                                            'condition_start_datetime',
                                            'condition_end_date',
                                            'condition_end_datetime',
                                            'condition_source_value']
        self.procedure_occurrence_fields = ['person_id',
                                            'procedure_concept_id',
                                            'procedure_date',
                                            'procedure_datetime',
                                            'modifier_concept_id',
                                            'procedure_source_value']

        self.condition_date_fields = {'_start_datetime': 'condition_start_datetime',
                                      '_start_date': 'condition_start_date',
                                      '_end_datetime': 'condition_end_datetime',
                                      '_end_date': 'condition_end_date'}
        self.procedure_date_fields = {'_datetime': 'procedure_datetime',
                                      '_date': 'procedure_date'}
        self.drug_date_fields = {'_start_datetime': 'drug_exposure_start_datetime',
                                 '_start_date': 'drug_exposure_start_date',
                                 '_end_datetime': 'drug_exposure_end_datetime',
                                 '_end_date': 'drug_exposure_end_date'}
        self.condition_common_fields = {'person_id': 'person_id',
                                        '_concept_id': 'condition_concept_id',
                                        '_source_value': 'condition_source_value'}
        self.procedure_common_fields = {'person_id': 'person_id',
                                        '_concept_id': 'procedure_concept_id',
                                        '_source_value': 'procedure_source_value'}
        self.condition_specific_fields = []
        self.procedure_specific_fields = ['modifier_concept_id']

        self.condition_fields = {
            DOMAIN_COMMON_FIELDS: self.condition_common_fields,
            DOMAIN_DATE_FIELDS: self.condition_date_fields,
            DOMAIN_SPECIFIC_FIELDS: self.condition_specific_fields
        }

        self.procedure_fields = {
            DOMAIN_COMMON_FIELDS: self.procedure_common_fields,
            DOMAIN_DATE_FIELDS: self.procedure_date_fields,
            DOMAIN_SPECIFIC_FIELDS: self.procedure_specific_fields
        }

    @mock.patch('cdr_cleaner.cleaning_rules.domain_mapping.DOMAIN_TABLE_NAMES')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain_fields')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain_id_field')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain')
    def test_create_domain_field_dict(self,
                                      mock_get_domain,
                                      mock_get_domain_id_field,
                                      mock_get_domain_fields,
                                      mock_domain_table_names):
        mock_get_domain.side_effect = [self.condition, self.procedure]
        mock_get_domain_id_field.side_effect = [self.condition_occurrence_id, self.procedure_occurrence_id]
        mock_get_domain_fields.side_effect = [
            self.condition_occurrence_fields,
            self.procedure_occurrence_fields
        ]
        mock_domain_table_names.__iter__.return_value = self.domain_tables

        domain_field_dict = field_mapping.create_domain_field_dict()

        self.assertEqual(len(domain_field_dict), 2)

        self.assertIn(self.condition_table, domain_field_dict.keys())
        self.assertIn(self.procedure_table, domain_field_dict.keys())

        # Testing for the condition_occurrence field dictionary
        condition_field_mappings = domain_field_dict[self.condition_table]
        self.assertEqual(len(condition_field_mappings), 3)
        self.assertDictContainsSubset(self.condition_common_fields, condition_field_mappings[DOMAIN_COMMON_FIELDS])
        self.assertItemsEqual(condition_field_mappings[DOMAIN_DATE_FIELDS],
                              self.condition_date_fields)
        self.assertTrue(len(condition_field_mappings[DOMAIN_SPECIFIC_FIELDS]) == 0)

        # Testing for the procedure_occurrence field dictionary
        procedure_field_mappings = domain_field_dict[self.procedure_table]
        self.assertEqual(len(procedure_field_mappings), 3)
        self.assertDictContainsSubset(self.procedure_common_fields, procedure_field_mappings[DOMAIN_COMMON_FIELDS])
        self.assertItemsEqual(procedure_field_mappings[DOMAIN_DATE_FIELDS],
                              self.procedure_date_fields)
        self.assertTrue(len(procedure_field_mappings[DOMAIN_SPECIFIC_FIELDS]) == 1)

    def test_resolve_common_field_mappings(self):
        actual = field_mapping.resolve_date_field_mappings(self.condition_common_fields,
                                                           self.procedure_common_fields)
        expected = {'person_id': 'person_id',
                    'procedure_concept_id': 'condition_concept_id',
                    'procedure_source_value': 'condition_source_value'}
        self.assertDictEqual(actual, expected)

    def test_resolve_date_field_mappings(self):
        actual = field_mapping.resolve_date_field_mappings(self.condition_date_fields, self.procedure_date_fields)
        expected = {'procedure_datetime': 'condition_start_datetime', 'procedure_date': 'condition_start_date'}
        self.assertDictEqual(actual, expected)

        actual = field_mapping.resolve_date_field_mappings(self.procedure_date_fields, self.condition_date_fields)
        expected = {'condition_start_datetime': 'procedure_datetime',
                    'condition_start_date': 'procedure_date',
                    'condition_end_datetime': NULL_VALUE,
                    'condition_end_date': NULL_VALUE}
        self.assertDictEqual(actual, expected)

        actual = field_mapping.resolve_date_field_mappings(self.drug_date_fields, self.condition_date_fields)
        expected = {'condition_start_datetime': 'drug_exposure_start_datetime',
                    'condition_start_date': 'drug_exposure_start_date',
                    'condition_end_datetime': 'drug_exposure_end_datetime',
                    'condition_end_date': 'drug_exposure_end_date'}
        self.assertDictEqual(actual, expected)

    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_field_mappings')
    def test_resolve_specific_field_mappings(self, mock_get_field_mappings):
        mock_get_field_mappings.return_value = {'condition_test_field': 'procedure_test_field'}

        actual = field_mapping.resolve_specific_field_mappings(self.condition_table, self.procedure_table,
                                                               ['test_1', 'test_2'])
        expected = {'condition_test_field': 'procedure_test_field', 'test_1': NULL_VALUE, 'test_2': NULL_VALUE}
        self.assertDictEqual(actual, expected)

        actual = field_mapping.resolve_specific_field_mappings(self.procedure_table, self.procedure_table,
                                                               ['test_1', 'test_2'])
        expected = {'test_1': 'test_1', 'test_2': 'test_2'}
        self.assertDictEqual(actual, expected)

    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.resolve_date_field_mappings')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.resolve_specific_field_mappings')
    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.resolve_common_field_mappings')
    def test_generate_field_mappings(self,
                                     mock_resolve_common_field_mappings,
                                     mock_resolve_specific_field_mappings,
                                     mock_resolve_date_field_mappings):
        mock_resolve_common_field_mappings.return_value = {'person_id': 'person_id',
                                                           'procedure_concept_id': 'condition_concept_id',
                                                           'procedure_source_value': 'condition_source_value', }
        mock_resolve_date_field_mappings.return_value = {'dest_date': 'src_date', 'dest_datetime': 'src_datetime'}
        mock_resolve_specific_field_mappings.return_value = {'dest_field': 'src_field'}

        actual = field_mapping.generate_field_mappings(self.condition_table, self.procedure_table,
                                                       self.condition_fields, self.procedure_fields)
        expected = {'person_id': 'person_id',
                    'procedure_concept_id': 'condition_concept_id',
                    'procedure_source_value': 'condition_source_value',
                    'dest_date': 'src_date',
                    'dest_datetime': 'src_datetime',
                    'dest_field': 'src_field'}

        self.assertDictEqual(actual, expected)

    @mock.patch('cdr_cleaner.cleaning_rules.field_mapping.get_domain_id_field')
    def test_get_domain_fields(self, mock_get_domain_id_field):
        with patch.dict('cdr_cleaner.cleaning_rules.field_mapping.CDM_TABLE_SCHEMAS', self.cdm_schemas):
            mock_get_domain_id_field.side_effect = [self.condition_occurrence_id, self.procedure_occurrence_id]
            fields = field_mapping.get_domain_fields(self.condition_table)
            self.assertEqual(fields, self.condition_occurrence_fields)
            self.assertEqual(field_mapping.get_domain_fields(self.procedure_table),
                             self.procedure_occurrence_fields)

    def test_is_field_nullable(self):
        with patch.dict('cdr_cleaner.cleaning_rules.field_mapping.CDM_TABLE_SCHEMAS', self.cdm_schemas):
            self.assertTrue(field_mapping.is_field_required(self.condition_table, self.condition_occurrence_id))
            self.assertFalse(field_mapping.is_field_required(self.condition_table, self.condition_end_date))
