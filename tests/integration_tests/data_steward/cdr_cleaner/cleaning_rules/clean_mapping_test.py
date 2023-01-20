"""
Integration test for clean_mapping module

DC-1528
"""

# Python imports
import os
from unittest import mock

# Project imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION, UNIONED_EHR
from cdr_cleaner.cleaning_rules.clean_mapping import CleanMappingExtTables
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

MAPPING_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.{{mapping_table}}` (
        observation_id,
        src_dataset_id,
        src_observation_id,
        src_hpo_id,
        src_table_id
    )
    VALUES
        (100, '2022_id', 400, 'pitt', 'pitt_observation'),
        (900, '2021_id', 800, 'chs', 'chs_observation')
""")

EXT_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.{{ext_table}}` (
        observation_id,
        src_id,
        survey_version_concept_id
    )
    VALUES
        (100, 'pitt', 1000),
        (150, 'chs', 2000)
""")

OBSERVATION_TABLE_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
    observation_id, person_id, observation_concept_id, observation_date, observation_datetime, 
    observation_type_concept_id, value_as_number, value_as_string, value_as_concept_id, qualifier_concept_id, 
    unit_concept_id, provider_id, visit_occurrence_id, visit_detail_id, observation_source_value,
    observation_source_concept_id, unit_source_value, qualifier_source_value, value_source_concept_id, value_source_value, 
    questionnaire_response_id
    )

VALUES
    (100, 1, 1585838, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585838, NULL, NULL, 1585840, NULL, NULL),

    (200, 1, 1585845, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585845, NULL, NULL, 1585846, NULL, NULL),

    (300, 2, 1585845, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585845, NULL, NULL, 1585846, NULL, NULL),

    (400, 3, 1585838, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585838, NULL, NULL, 1585839, NULL, NULL)
""")

OBSERVATION_TABLE_EHR_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.unioned_ehr_observation` (
    observation_id, person_id, observation_concept_id, observation_date, observation_datetime, 
    observation_type_concept_id, value_as_number, value_as_string, value_as_concept_id, qualifier_concept_id, 
    unit_concept_id, provider_id, visit_occurrence_id, visit_detail_id, observation_source_value,
    observation_source_concept_id, unit_source_value, qualifier_source_value, value_source_concept_id, value_source_value, 
    questionnaire_response_id
    )

VALUES
    (900, 1, 1585838, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585838, NULL, NULL, 1585840, NULL, NULL),

    (300, 2, 1585845, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585845, NULL, NULL, 1585846, NULL, NULL)
""")


class CleanMappingExtTablesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    @mock.patch('cdr_cleaner.cleaning_rules.clean_mapping.get_mapping_tables')
    def setUpClass(cls, mock_mapping_tables):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # mock mapping_tables
        cls.tables = [
            f'_mapping_{OBSERVATION}', f'{OBSERVATION}_ext', OBSERVATION
        ]
        mock_mapping_tables.return_value = cls.tables

        # Instantiate class
        cls.rule_instance = CleanMappingExtTables(cls.project_id,
                                                  cls.dataset_id,
                                                  cls.sandbox_id)

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store affected table names
        affected_tables = cls.rule_instance.affected_tables
        for table_name in affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{UNIONED_EHR}_{OBSERVATION}')

        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """

        super().setUp()

        mapping_query = MAPPING_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            mapping_table=f'_mapping_{OBSERVATION}')

        ext_query = EXT_TABLE_TEMPLATE.render(project_id=self.project_id,
                                              dataset_id=self.dataset_id,
                                              ext_table=f'{OBSERVATION}_ext')

        observation_query = OBSERVATION_TABLE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        observation_ehr_unioned_query = OBSERVATION_TABLE_EHR_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([
            mapping_query, ext_query, observation_query,
            observation_ehr_unioned_query
        ])

    @mock.patch('cdr_cleaner.cleaning_rules.clean_mapping.get_mapping_tables')
    @mock.patch.object(CleanMappingExtTables, 'setup_rule')
    def test_field_cleaning(self, mock_setup, mock_mapping_tables):
        """
        Records with observation_ids 900 and 150 are sandboxed
        """
        # mock mapping_tables
        mock_mapping_tables.return_value = self.tables

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [100, 900],
            'sandboxed_ids': [900],
            'fields': [
                'observation_id', 'src_dataset_id', 'src_observation_id',
                'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [(100, '2022_id', 400, 'pitt', 'pitt_observation')
                              ]
        }, {
            'fq_table_name': self.fq_table_names[1],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[1],
            'loaded_ids': [100, 150],
            'sandboxed_ids': [150],
            'fields': ['observation_id', 'src_id', 'survey_version_concept_id'],
            'cleaned_values': [(100, 'pitt', 1000)]
        }]
        self.default_test(tables_and_counts)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.clean_mapping.CleanMappingExtTables.is_ehr_dataset'
    )
    @mock.patch('cdr_cleaner.cleaning_rules.clean_mapping.get_mapping_tables')
    def test_field_cleaning_ehr(self, mock_mapping_tables, mock_is_ehr_dataset):
        """
        Test for clean_mapping when the dataset is an EHR dataset.
        _mapping_xyz is cleaned based on unioned_ehr_xyz tables.
        """
        mock_mapping_tables.return_value = f'_mapping_{OBSERVATION}'
        mock_is_ehr_dataset.return_value = True

        tables_and_counts = [{
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': [100, 900],
            'sandboxed_ids': [100],
            'fields': [
                'observation_id', 'src_dataset_id', 'src_observation_id',
                'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [(900, '2021_id', 800, 'chs', 'chs_observation')]
        }]
        self.default_test(tables_and_counts)
