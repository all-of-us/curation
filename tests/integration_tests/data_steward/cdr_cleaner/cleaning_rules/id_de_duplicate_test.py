"""
Integration test for id_deduplicate module

Original Issues: DC-392

The intent is remove duplicate primary keys all domain tables excluding person table"""

# Python Imports
import os

# Project Imports
from common import CONDITION_OCCURRENCE, OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.id_deduplicate import \
    DeduplicateIdColumn
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class DeduplicateIdColumnTestBase(BaseTest.CleaningRulesTestBase):

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
        cls.affected_tables = [CONDITION_OCCURRENCE, OBSERVATION]

        cls.rule_instance = DeduplicateIdColumn(cls.project_id, cls.dataset_id,
                                                cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [CONDITION_OCCURRENCE, OBSERVATION]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in [CONDITION_OCCURRENCE, OBSERVATION]:
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
        condition_occurrence_data_template = self.jinja_env.from_string("""
            DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.condition_occurrence`;
            CREATE TABLE `{{project_id}}.{{dataset_id}}.condition_occurrence`
            AS (
            WITH w AS (
              SELECT ARRAY<STRUCT<
                    condition_occurrence_id int64, 
                    person_id int64, 
                    condition_concept_id int64
                    >>
                  [(1, 1, 0),
                   (1, 1, 0),
                   (2, 1, 0),
                   (3, 1, 0),
                   (3, 1, 0),
                   (4, 1, 0),
                   (5, 1, 0),
                   (6, 1, 0),
                   (6, 1, 0),
                   (7, 1, 0)] col
            )
            SELECT 
                condition_occurrence_id, 
                person_id, 
                condition_concept_id
            FROM w, UNNEST(w.col))
            """)

        # Load the test data
        observation_data_template = self.jinja_env.from_string("""
            DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.observation`;
            CREATE TABLE `{{project_id}}.{{dataset_id}}.observation`
            AS (
            WITH w AS (
              SELECT ARRAY<STRUCT<
                    observation_id int64, 
                    person_id int64, 
                    observation_concept_id int64
                    >>
                  [(1, 1, 0),
                   (1, 1, 0),
                   (2, 1, 0),
                   (3, 1, 0),
                   (3, 1, 0),
                   (4, 1, 0),
                   (5, 1, 0),
                   (6, 1, 0),
                   (6, 1, 0),
                   (7, 1, 0)] col
            )
            SELECT 
                observation_id, 
                person_id, 
                observation_concept_id
            FROM w, UNNEST(w.col))
            """)

        insert_condition_query = condition_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{insert_condition_query};
                {insert_observation_query};'''
        ])

    def test_id_deduplicate(self):

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.condition_occurrence',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("condition_occurrence")}',
            'loaded_ids': [1, 1, 2, 3, 3, 4, 5, 6, 6, 7],
            'sandboxed_ids': [1, 1, 3, 3, 6, 6],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id'
            ],
            'cleaned_values': [(1, 1, 0), (2, 1, 0), (3, 1, 0), (4, 1, 0),
                               (5, 1, 0), (6, 1, 0), (7, 1, 0)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 1, 2, 3, 3, 4, 5, 6, 6, 7],
            'sandboxed_ids': [1, 1, 3, 3, 6, 6],
            'fields': ['observation_id', 'person_id', 'observation_concept_id'],
            'cleaned_values': [(1, 1, 0), (2, 1, 0), (3, 1, 0), (4, 1, 0),
                               (5, 1, 0), (6, 1, 0), (7, 1, 0)]
        }]

        self.default_test(tables_and_counts)
