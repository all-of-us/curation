"""
Integration test for birth_information_suppression module

Original Issues: DC-1358

The intent is to suppress the birth information concepts across all *concept_id fields in 
observation """

# Python Imports
import os

from google.cloud.bigquery import Table

# Project Imports
from common import OBSERVATION, VOCABULARY_TABLES
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.birth_information_suppression import \
    BirthInformationSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class BirthInformationSuppressionTestBase(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = BirthInformationSuppression(cls.project_id,
                                                        cls.dataset_id,
                                                        cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [OBSERVATION]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create domain tables required for the test
        super().setUp()

        # Load the test data
        observation_data_template = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (
                observation_id, 
                person_id, 
                observation_concept_id, 
                observation_type_concept_id,
                value_as_concept_id,
                observation_source_concept_id,
                value_source_concept_id,
                qualifier_concept_id,
                unit_concept_id,
                observation_date
            )
            VALUES
              -- Concepts to suppress --
              -- 1585259: PII Birth Information: Birth Date --
              -- 4083587: Date of birth --
             
              -- Concepts to keep --
              -- 42628400: Follow-up service --
              -- 1332785: Smoking more cigarettes or vaping more --
              -- 43533330: Left main coronary artery --
              (1, 1, 1585259, 0, 0, 0, 0, 0, 0, '2020-01-01'),
              (2, 1, 0, 1585259, 0, 0, 0, 0, 0, '2020-01-01'),
              (3, 1, 0, 0, 4083587, 0, 0, 0, 0, '2020-01-01'),
              (4, 1, 0, 0, 0, 4083587, 0, 0, 0, '2020-01-01'),
              (5, 1, 1585259, 0, 0, 0, 44803087, 0, 0, '2020-01-01'),
              (6, 1, 42628400, 0, 0, 0, 0, 0, 0, '2020-01-01'),
              (7, 1, 0, 1332785, 0, 0, 0, 0, 0, '2020-01-01'),
              (8, 1, 0, 0, 43533330, 0, 0, 0, 0, '2020-01-01'),
              (9, 1, 0, 0, 0, 43533330, 0, 0, 0, '2020-01-01'),
              (10, 1, 42628400, 1332785, 43533330, 43533330, 0, 0, 0, '2020-01-01')
            """)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'''{insert_observation_query};'''])

    def test_birth_information_accident(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 3, 4, 5],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'qualifier_concept_id', 'unit_concept_id'
            ],
            'cleaned_values': [(6, 1, 42628400, 0, 0, 0, 0, 0, 0),
                               (7, 1, 0, 1332785, 0, 0, 0, 0, 0),
                               (8, 1, 0, 0, 43533330, 0, 0, 0, 0),
                               (9, 1, 0, 0, 0, 43533330, 0, 0, 0),
                               (10, 1, 42628400, 1332785, 43533330, 43533330, 0,
                                0, 0)]
        }]

        self.default_test(tables_and_counts)
