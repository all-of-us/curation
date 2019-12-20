import unittest

from mock import patch

import resources
from cdr_cleaner.cleaning_rules.domain_mapping import (
    SRC_TABLE, DEST_TABLE, SRC_FIELD, DEST_FIELD, SRC_VALUE,
    DEST_VALUE, IS_REROUTED, REROUTING_CRITERIA, EMPTY_STRING, TRANSLATION
)
from cdr_cleaner.cleaning_rules import domain_mapping as domain_mapping


class DomainMappingTest(unittest.TestCase):

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
        self.measurement_table = 'measurement'
        self.rerouted_criteria = '(1 = 1)'
        self.procedure_occurrence_id = 'procedure_occurrence_id'
        self.condition_occurrence_id = 'condition_occurrence_id'
        self.condition_concept_id = 'condition_concept_id'
        self.procedure_concept_id = 'procedure_concept_id'
        self.condition_type_concept_id = 'condition_type_concept_id'
        self.procedure_type_concept_id = 'procedure_type_concept_id'
        self.condition_source_concept_id = 'condition_source_concept_id'
        self.procedure_source_concept_id = 'procedure_source_concept_id'
        self.procedure = 'Procedure'
        self.condition = 'Condition'
        self.primary_procedure_concept_id = 44786630
        self.primary_condition_concept_id = 44786627

        self.mock_table_mappings_csv_patcher = patch('cdr_cleaner.cleaning_rules.domain_mapping.table_mappings_csv')
        self.mock_table_mappings_csv = self.mock_table_mappings_csv_patcher.start()
        self.mock_table_mappings_csv.__iter__.return_value = [
            {
                SRC_TABLE: self.condition_table,
                DEST_TABLE: self.procedure_table,
                IS_REROUTED: 1,
                REROUTING_CRITERIA: self.rerouted_criteria
            },
            {
                SRC_TABLE: self.procedure_table,
                DEST_TABLE: self.condition_table,
                IS_REROUTED: 1,
                REROUTING_CRITERIA: EMPTY_STRING
            },
            {
                SRC_TABLE: self.procedure_table,
                DEST_TABLE: self.measurement_table,
                IS_REROUTED: 0,
                REROUTING_CRITERIA: EMPTY_STRING
            }
        ]

        self.mock_field_mappings_csv_patcher = patch('cdr_cleaner.cleaning_rules.domain_mapping.field_mappings_csv')
        self.mock_field_mappings_csv = self.mock_field_mappings_csv_patcher.start()
        self.mock_field_mappings_csv.__iter__.return_value = [
            {
                SRC_TABLE: self.condition_table,
                DEST_TABLE: self.procedure_table,
                SRC_FIELD: self.condition_concept_id,
                DEST_FIELD: self.procedure_concept_id,
                TRANSLATION: 0
            },
            {
                SRC_TABLE: self.condition_table,
                DEST_TABLE: self.procedure_table,
                SRC_FIELD: self.condition_type_concept_id,
                DEST_FIELD: self.procedure_type_concept_id,
                TRANSLATION: 1
            },
            {
                SRC_TABLE: self.condition_table,
                DEST_TABLE: self.procedure_table,
                SRC_FIELD: self.condition_source_concept_id,
                DEST_FIELD: self.procedure_source_concept_id,
                TRANSLATION: 0
            }
        ]

        self.mock_value_mappings_csv_patcher = patch('cdr_cleaner.cleaning_rules.domain_mapping.value_mappings_csv')
        self.mock_value_mappings_csv = self.mock_value_mappings_csv_patcher.start()
        self.mock_value_mappings_csv.__iter__.return_value = [
            {
                SRC_TABLE: self.condition_table,
                DEST_TABLE: self.procedure_table,
                SRC_FIELD: self.condition_type_concept_id,
                DEST_FIELD: self.procedure_type_concept_id,
                SRC_VALUE: self.primary_condition_concept_id,
                DEST_VALUE: self.primary_procedure_concept_id
            },
            {
                SRC_TABLE: self.procedure_table,
                DEST_TABLE: self.condition_table,
                SRC_FIELD: self.procedure_type_concept_id,
                DEST_FIELD: self.condition_type_concept_id,
                SRC_VALUE: self.primary_procedure_concept_id,
                DEST_VALUE: self.primary_condition_concept_id
            }
        ]

    def tearDown(self):
        self.mock_table_mappings_csv_patcher.stop()
        self.mock_field_mappings_csv_patcher.stop()
        self.mock_value_mappings_csv_patcher.stop()

    def test_get_domain_id_field(self):
        self.assertEqual(resources.get_domain_id_field(self.condition_table), self.condition_occurrence_id)
        self.assertEqual(resources.get_domain_id_field(self.procedure_table), self.procedure_occurrence_id)

    def test_get_domain_concept_id(self):
        self.assertEqual(resources.get_domain_concept_id(self.condition_table), self.condition_concept_id)
        self.assertEqual(resources.get_domain_concept_id(self.procedure_table), self.procedure_concept_id)

    def test_get_domain(self):
        self.assertEqual(resources.get_domain(self.condition_table), self.condition)
        self.assertEqual(resources.get_domain(self.procedure_table), self.procedure)

    def test_exist_domain_mappings(self):
        self.assertTrue(domain_mapping.exist_domain_mappings(self.condition_table, self.procedure_table))
        self.assertTrue(domain_mapping.exist_domain_mappings(self.procedure_table, self.condition_table))
        self.assertFalse(domain_mapping.exist_domain_mappings(self.procedure_table, self.measurement_table))
        self.assertFalse(domain_mapping.exist_domain_mappings(self.condition_table, self.measurement_table))

    def test_get_rerouted_criteria(self):
        self.assertEqual(self.rerouted_criteria,
                         domain_mapping.get_rerouting_criteria(self.condition_table, self.procedure_table))
        self.assertEqual(EMPTY_STRING,
                         domain_mapping.get_rerouting_criteria(self.condition_table, self.measurement_table))
        self.assertEqual(EMPTY_STRING,
                         domain_mapping.get_rerouting_criteria(self.procedure_table, self.measurement_table))
        self.assertEqual(EMPTY_STRING,
                         domain_mapping.get_rerouting_criteria(self.procedure_table, self.condition_table))

    def test_get_field_mappings(self):
        expected = {
            self.procedure_concept_id: self.condition_concept_id,
            self.procedure_type_concept_id: self.condition_type_concept_id,
            self.procedure_source_concept_id: self.condition_source_concept_id
        }
        self.assertDictEqual(domain_mapping.get_field_mappings(self.condition_table, self.procedure_table), expected)
        self.assertDictEqual(domain_mapping.get_field_mappings(self.procedure_table, self.condition_table), {})

    def test_value_requires_translation(self):
        self.assertTrue(domain_mapping.value_requires_translation(src_table=self.condition_table,
                                                                  dest_table=self.procedure_table,
                                                                  src_field=self.condition_type_concept_id,
                                                                  dest_field=self.procedure_type_concept_id))

        self.assertFalse(domain_mapping.value_requires_translation(src_table=self.condition_table,
                                                                   dest_table=self.procedure_table,
                                                                   src_field=self.condition_concept_id,
                                                                   dest_field=self.procedure_concept_id))

        self.assertFalse(domain_mapping.value_requires_translation(src_table=self.condition_table,
                                                                   dest_table=self.procedure_table,
                                                                   src_field=self.condition_source_concept_id,
                                                                   dest_field=self.procedure_source_concept_id))

        self.assertFalse(domain_mapping.value_requires_translation(src_table=self.condition_table,
                                                                   dest_table=self.procedure_table,
                                                                   src_field=self.condition_concept_id,
                                                                   dest_field=self.procedure_source_concept_id))

    def test_get_value_mappings(self):
        actual = domain_mapping.get_value_mappings(src_table=self.condition_table, dest_table=self.procedure_table,
                                                   src_field=self.condition_type_concept_id,
                                                   dest_field=self.procedure_type_concept_id)

        self.assertDictEqual(actual, {self.primary_procedure_concept_id: self.primary_condition_concept_id})

        actual = domain_mapping.get_value_mappings(src_table=self.procedure_table, dest_table=self.condition_table,
                                                   src_field=self.procedure_type_concept_id,
                                                   dest_field=self.condition_type_concept_id)

        self.assertDictEqual(actual, {self.primary_condition_concept_id: self.primary_procedure_concept_id})