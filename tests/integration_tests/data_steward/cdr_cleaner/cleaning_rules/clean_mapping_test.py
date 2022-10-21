"""
Integration test for clean_mapping module

DC-1528
"""

# Python imports
import os
from unittest import mock

# Third party imports
from google.cloud import bigquery

# Project imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION
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
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`

(observation_id, person_id, observation_concept_id, observation_date, observation_datetime, 
observation_type_concept_id, value_as_number, value_as_string, value_as_concept_id, qualifier_concept_id, 
unit_concept_id, provider_id, visit_occurrence_id, visit_detail_id, observation_source_value,
observation_source_concept_id, unit_source_value, qualifier_source_value, value_source_concept_id, value_source_value, 
questionnaire_response_id)

VALUES
    (100, 1, 1585838, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585838, NULL, NULL, 1585840, NULL, NULL
    ),

    (200, 1, 1585845, '2009-04-29', TIMESTAMP('2009-04-29'),
     8201211115, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99051',
     1585845, NULL, NULL, 1585846, NULL, NULL
    ),

    (300, 2, 1585845, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585845, NULL, NULL, 1585846, NULL, NULL
    ),

    (400, 3, 1585838, '2009-05-15', TIMESTAMP('2009-05-15'),
     8201211116, 1.0, NULL, 0, 0,
     0, 0, NULL, NULL, '99054',
     1585838, NULL, NULL, 1585839, NULL, NULL
    )
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
        cls.tables = ['_mapping_observation', 'observation_ext']
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

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """

        table_schemas = [
            self.client.get_table_schema('_mapping_observation'),
            self.client.get_table_schema('observation_ext'),
            self.client.get_table_schema(OBSERVATION)
        ]

        # create tables
        observation_table = f'{self.project_id}.{self.dataset_id}.{OBSERVATION}'
        for table, schema in zip(self.fq_table_names + [observation_table],
                                 table_schemas):
            table_obj = bigquery.Table(table, schema)
            self.client.create_table(table_obj, exists_ok=True)

        mapping_query = MAPPING_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            mapping_table='_mapping_observation')
        ext_query = EXT_TABLE_TEMPLATE.render(project_id=self.project_id,
                                              dataset_id=self.dataset_id,
                                              ext_table='observation_ext')
        observation_query = OBSERVATION_TABLE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([mapping_query, ext_query, observation_query])

    @mock.patch('cdr_cleaner.cleaning_rules.clean_mapping.get_mapping_tables')
    @mock.patch.object(CleanMappingExtTables, 'setup_rule')
    def test_field_cleaning(self, mock_setup, mock_mapping_tables):
        """
        test
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
        self.client.delete_table(
            f'{self.project_id}.{self.dataset_id}.{OBSERVATION}')
