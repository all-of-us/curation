"""
Integration test to ensure records are properly sandboxed and dropped in the registered_cope_survey_suppression.py module.

Removes any records that have an observation_source_concept_id as any of these values: 1310058, 1310065, 1333012,
 1333234, 702686, 1333327, 1333118, 1310054, 1333326, 1310066, 596884, 596885, 596886, 596887, 596888, 596889, 1310137,
 1310146, 1333015, 1333023, 1333016, 715714, 1310147, 715726.

Original Issue: DC-1666, DC-1740

The intent is to ensure that no records exists that have any of the observation_source_concept_id above by sandboxing
the rows and removing them from the observation table.
"""

# Python Imports
import os

# Project Imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.registered_cope_survey_suppression import RegisteredCopeSurveyQuestionsSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RegisteredCopeSurveyQuestionsSuppressionTest(
        BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = RegisteredCopeSurveyQuestionsSuppression(
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
            -- subset of Concepts to suppress --
            -- 1310058 Thinking about your current social habits, in the last 5 days: -- 
                    -- I have attended social gatherings of MORE than 50 people. Please specify: --
            -- 1310065 Which of the following symptoms did you have? Select all that apply. Please specify: --
            -- 1333012 What breathing treatment did you receive? Please select all that apply. --
            -- 1333234 What other breathing treatment did you receive? Please specify. --
            -- 702686  Who do you know who has died? Please specify. --
              (1, 101, 0, 0, 0, 1310058, 0, 0, 0, '2020-01-01'),
              (2, 102, 0, 0, 0, 1310065, 0, 0, 0, '2020-01-01'),
              (3, 103, 0, 0, 0, 1333012, 0, 0, 0, '2020-01-01'),
              (4, 104, 0, 0, 0, 1333234, 0, 0, 0, '2020-01-01'),
              (5, 105, 0, 0, 0, 702686, 0, 0, 0, '2020-01-01'),
              (6, 106, 0, 0, 0, 715714, 0, 0, 0, '2020-01-01'),
              -- concepts not to be suppressed --
               -- 1333325 In the past month, have you been sick for more than one day with a new illness related to -- 
                    -- COVID-19 or flu-like symptoms? --
               -- 1333235 Which of the following symptoms did you have? (select all that apply). --
               -- 1332769 Who do you know who has died? Check all that apply. --
               -- 1333156 Do you think you have had COVID-19? --
               -- 713888  When did your COVID-19 symptoms begin? --
              (7, 107, 0, 0, 0, 1333325, 0, 0, 0, '2020-01-01'),
              (8, 108, 0, 0, 0, 1333235, 0, 0, 0, '2020-01-01'),
              (9, 109, 0, 0, 0, 1332769, 0, 0, 0, '2020-01-01'),
              (10, 110, 0, 0, 0, 1333156, 0, 0, 0, '2020-01-01'),
              (11, 111, 0, 0, 0, 713888, 0, 0, 0, '2020-01-01'),
              (12, 112, 0, 0, 0, 1111111, 0, 0, 0, '2020-01-01'),
              (13, 113, 0, 0, 0, 2222222, 0, 0, 0, '2020-01-01'),
              (14, 114, 0, 0, 0, null, 0, 0, 0, '2020-01-01')
            """)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{insert_observation_query};'])

    def test_registered_cope_survey_suppression(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'qualifier_concept_id', 'unit_concept_id'
            ],
            'cleaned_values': [(7, 107, 0, 0, 0, 1333325, 0, 0, 0),
                               (8, 108, 0, 0, 0, 1333235, 0, 0, 0),
                               (9, 109, 0, 0, 0, 1332769, 0, 0, 0),
                               (10, 110, 0, 0, 0, 1333156, 0, 0, 0),
                               (11, 111, 0, 0, 0, 713888, 0, 0, 0),
                               (12, 112, 0, 0, 0, 1111111, 0, 0, 0),
                               (13, 113, 0, 0, 0, 2222222, 0, 0, 0),
                               (14, 114, 0, 0, 0, None, 0, 0, 0)]
        }]

        self.default_test(tables_and_counts)
