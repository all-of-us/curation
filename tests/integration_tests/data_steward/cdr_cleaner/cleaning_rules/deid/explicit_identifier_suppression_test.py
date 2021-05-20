"""
Integration test for explicit_identifier_suppression.py

Original Issue: DC-1347

suppress all records associated with a PPI vocabulary explicit identifier associated PPI vocabulary
The concept_ids to suppress can be determined from the vocabulary with the following regular expressions.

        REGEXP_CONTAINS(concept_code, 'Signature')
        (REGEXP_CONTAINS(concept_code, 'Name') AND REGEXP_CONTAINS(concept_code, r'(First)|(Last)|(Middle)|(Help)'))
        (REGEXP_CONTAINS(concept_code, '(Address)|(PhoneNumber)') and 
            NOT REGEXP_CONTAINS(concept_code, '(PIIState)|(State_)|(ZIP)') ) and concept_class_id != 'Topic'
        REGEXP_CONTAINS(concept_code, 'SecondaryContactInfo') and concept_class_id = 'Question'
        (REGEXP_CONTAINS(concept_code, 'SocialSecurity') and concept_class_id = 'Question')
        and also covers all the mapped standard concepts for non standard concepts that the regex filters.

"""
# Python Imports
import os

# Third Party Imports
from google.cloud.bigquery import Table

# Project Imports
from common import OBSERVATION, VOCABULARY_TABLES
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.explicit_identifier_suppression import \
    ExplicitIdentifierSuppression, EXPLICIT_IDENTIFIER_CONCEPTS
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class ExplicitIdentifierSuppressionTestBase(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = ExplicitIdentifierSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [OBSERVATION] + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        sandbox_table_name = cls.rule_instance.sandbox_table_for(OBSERVATION)
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')
        # Add EXPLICIT_IDENTIFIER_CONCEPTS table to fq_sandbox_table_names so it gets deleted after
        # the test
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{EXPLICIT_IDENTIFIER_CONCEPTS}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

        # Copy vocab tables over to the test dataset
        for src_table in cls.client.list_tables(cls.vocabulary_id):
            destination = f'{cls.project_id}.{cls.dataset_id}.{src_table.table_id}'
            cls.client.copy_table(src_table, destination)

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
              -- Subset of Concepts to suppress --
              -- 43529731: Social Security: Prefer Not To Answer --
              -- 43529730: Secondary Contact Info: Person Two Prefer Not To Answer --
              -- 3042942: First name --
              -- 1585907: Secondary Contact Info: Person One First Name --
              -- 1585246: PII Address: Street Address --

              -- Concepts to keep --
              -- 1384550: Insurance Type: Tricare Or Military --
              -- 903152: Hair style or head gear --
              -- 903574: Disability: Blind --
              -- 903155: Manual heart rate --
              (1, 1, 43529731, 0, 0, 0, 0, 0, 0, '2020-01-01'),
              (2, 1, 0, 43529730, 0, 0, 0, 0, 0, '2020-01-01'),
              (3, 1, 0, 0, 3042942, 0, 0, 0, 0, '2020-01-01'),
              (4, 1, 0, 0, 0, 1585907, 0, 0, 0, '2020-01-01'),
              (5, 1, 0, 0, 0, 0, 1585246, 0, 0, '2020-01-01'),
              (6, 1, 43529731, 43529730, 3042942, 1585907, 1585246, 0, 0, '2020-01-01'),
              (7, 1, 1384550, 0, 0, 0, 0, 0, 0, '2020-01-01'),
              (8, 1, 0, 903152, 0, 0, 0, 0, 0, '2020-01-01'),
              (9, 1, 0, 0, 903574, 0, 0, 0, 0, '2020-01-01'),
              (10, 1, 0, 0, 0, 903155, 0, 0, 0, '2020-01-01'),
              (11, 1, 1384550, 903152, 903574, 903155, 0, 0, 0, '2020-01-01')
            """)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'''{insert_observation_query};'''])

    def test_explicit_identifier_suppression(self):

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'qualifier_concept_id', 'unit_concept_id'
            ],
            'cleaned_values': [(7, 1, 1384550, 0, 0, 0, 0, 0, 0),
                               (8, 1, 0, 903152, 0, 0, 0, 0, 0),
                               (9, 1, 0, 0, 903574, 0, 0, 0, 0),
                               (10, 1, 0, 0, 0, 903155, 0, 0, 0),
                               (11, 1, 1384550, 903152, 903574, 903155, 0, 0, 0)
                              ]
        }]

        self.default_test(tables_and_counts)
