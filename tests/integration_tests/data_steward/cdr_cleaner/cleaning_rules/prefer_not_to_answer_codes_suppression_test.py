"""
Integration test for the prefer_not_to_answer_codes_suppression.py module

Original Issue: DC-524

The intent of this integration test is to ensure that any records that have an observation_source_concept_id of 903079
    are sandboxed and dropped.
"""

# Python Imports
import os

# Third party imports
from dateutil import parser

# Project Imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.prefer_not_to_answer_codes_suppression import PreferNotToAnswerSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class PreferNotToAnswerSuppressionTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = PreferNotToAnswerSuppression(
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
        Create empty tables on which the rule will run
        """

        # Create domain tables required for the test
        super().setUp()

        # Load the test data
        observation_tmpl = self.jinja_env.from_string("""
        DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.observation`;
        CREATE TABLE `{{project_id}}.{{dataset_id}}.observation`
        AS (
        WITH w AS (
            SELECT ARRAY<STRUCT<
                observation_id INT64,
                person_id INT64,
                observation_concept_id INT64,
                observation_date DATE,
                observation_type_concept_id INT64,
                value_as_concept_id INT64,
                qualifier_concept_id INT64,
                unit_concept_id INT64,
                observation_source_concept_id INT64,
                value_source_concept_id INT64
                >>
              -- Concepts to suppress --
              -- 903079: PMI Prefer Not To Answer --
              [(1, 2, 111111, date('2017-05-02'), 111111, 111111, 111111, 111111, 111111, 111111),
               (2, 3, 222222, date('2017-05-02'), 222222, 222222, 222222, 222222, 903079, 222222),
               (3, 4, 333333, date('2017-05-02'), 333333, 333333, 333333, 333333, 333333, 333333)] col
            )
            SELECT
                observation_id,
                person_id,
                observation_concept_id,
                observation_date,
                observation_type_concept_id,
                value_as_concept_id,
                qualifier_concept_id,
                unit_concept_id,
                observation_source_concept_id,
                value_source_concept_id
            FROM w, UNNEST(w.col))
            """)

        insert_observation_query = observation_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load the test data
        self.load_test_data([f'''{insert_observation_query};'''])

    def test_prefer_not_to_answer_code_suppression(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 2, 3],
            'sandboxed_ids': [2],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'value_as_concept_id', 'qualifier_concept_id',
                'unit_concept_id', 'observation_source_concept_id',
                'value_source_concept_id'
            ],
            'cleaned_values': [(1, 2, 111111, parser.parse('2017-05-02').date(),
                                111111, 111111, 111111, 111111, 111111, 111111),
                               (3, 4, 333333, parser.parse('2017-05-02').date(),
                                333333, 333333, 333333, 333333, 333333, 333333)]
        }]

        self.default_test(tables_and_counts)
