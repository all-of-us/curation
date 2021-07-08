"""
Integration test to ensure records are properly sandboxed and dropped in the cope_survey_response_suppression.py module.

Removes any records that have an observation_source_concept_id as any of these values: 1333234, 1310066, 715725,
    1310147, 702686, 1310054, 715726, 715724, 715714, 1310146, 1310058.

Original Issue: DC-1492

The intent is to ensure that no records exists that have any of the observation_source_concept_id above by sandboxing
    sandboxing the rows and removing them from the observation table.
"""

# Python Imports
import os

# Project Imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.cope_survey_response_suppression import CopeSurveyResponseSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CopeSurveyResponseSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = CopeSurveyResponseSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

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
              -- 1333234: What breathing treatments did you receive? --
              -- 1310066: How were you tested? --
              -- 715725: What do you think is the main reason(s) for these experiences? --
              -- 1310147: How has your prenatal care changed since COVID-19? --
              -- 702686: Who do you know who has died? --
              -- 1310054: Were you tested for COVID-19? --
              -- 715726: Are you currently covered by any of the following types of health insurance...? --
              -- 715724: Other substance? --
              -- 715714: What type of household do you live in? --
              -- 1310146: What factors might make you less likely to get the vaccine? --
              -- 1310058: Thinking about your current social habits, in the last 5 days: I have...? --

              (1, 101, 0, 0, 0, 1333234, 0, 0, 0, '2020-01-01'),
              (2, 102, 0, 0, 0, 1310066, 0, 0, 0, '2020-01-01'),
              (3, 103, 0, 0, 0, 715725, 0, 0, 0, '2020-01-01'),
              (4, 104, 0, 0, 0, 1310147, 0, 0, 0, '2020-01-01'),
              (5, 105, 0, 0, 0, 702686, 0, 0, 0, '2020-01-01'),
              (6, 106, 0, 0, 0, 1310054, 0, 0, 0, '2020-01-01'),
              (7, 107, 0, 0, 0, 715726, 0, 0, 0, '2020-01-01'),
              (8, 108, 0, 0, 0, 715724, 0, 0, 0, '2020-01-01'),
              (9, 109, 0, 0, 0, 715714, 0, 0, 0, '2020-01-01'),
              (10, 110, 0, 0, 0, 1310146, 0, 0, 0, '2020-01-01'),
              (11, 111, 0, 0, 0, 1310058, 0, 0, 0, '2020-01-01'),
              (12, 112, 0, 0, 0, 1310065, 0, 0, 0, '2020-01-01'),
              -- not concepts to be suppressed --
              (13, 113, 0, 0, 0, 1111111, 0, 0, 0, '2020-01-01'),
              (14, 114, 0, 0, 0, 2222222, 0, 0, 0, '2020-01-01')
            """)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'''{insert_observation_query};'''])

    def test_cope_survey_response_suppression(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'qualifier_concept_id', 'unit_concept_id'
            ],
            'cleaned_values': [(13, 113, 0, 0, 0, 1111111, 0, 0, 0),
                               (14, 114, 0, 0, 0, 2222222, 0, 0, 0)]
        }]

        self.default_test(tables_and_counts)
