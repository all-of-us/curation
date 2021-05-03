"""
Integration test for id_deduplicate module

Original Issues: DC-392

The intent is remove duplicate primary keys all domain tables excluding person table"""

# Python Imports
import os
from datetime import date, datetime

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
        for table_name in cls.affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in cls.affected_tables:
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
        insert_condition_occurrence_query = self.jinja_env.from_string(
            """
            INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
                (condition_occurrence_id, person_id, condition_concept_id,
                condition_start_date, condition_start_datetime, condition_type_concept_id)
            VALUES (1, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (1, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (2, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (3, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (3, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (4, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (5, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (6, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (6, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1),
                   (7, 1, 0, '2010-01-01', timestamp('2010-01-01'), 1)"""
        ).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # Load the test data
        insert_observation_query = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation` 
                (observation_id, person_id, observation_concept_id, 
                observation_date, observation_type_concept_id)
            VALUES (1, 1, 0, '2010-01-01', 1),
                   (1, 1, 0, '2010-01-01', 1),
                   (2, 1, 0, '2010-01-01', 1),
                   (3, 1, 0, '2010-01-01', 1),
                   (3, 1, 0, '2010-01-01', 1),
                   (4, 1, 0, '2010-01-01', 1),
                   (5, 1, 0, '2010-01-01', 1),
                   (6, 1, 0, '2010-01-01', 1),
                   (6, 1, 0, '2010-01-01', 1),
                   (7, 1, 0, '2010-01-01', 1)""").render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data(
            [insert_condition_occurrence_query, insert_observation_query])

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
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_type_concept_id'
            ],
            'cleaned_values': [
                (1, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1),
                (2, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1),
                (3, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1),
                (4, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1),
                (5, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1),
                (6, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1),
                (7, 1, 0, date.fromisoformat('2010-01-01'),
                 datetime.fromisoformat('2010-01-01 00:00:00+00:00'), 1)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("observation")}',
            'loaded_ids': [1, 1, 2, 3, 3, 4, 5, 6, 6, 7],
            'sandboxed_ids': [1, 1, 3, 3, 6, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'cleaned_values': [(1, 1, 0, date.fromisoformat('2010-01-01'), 1),
                               (2, 1, 0, date.fromisoformat('2010-01-01'), 1),
                               (3, 1, 0, date.fromisoformat('2010-01-01'), 1),
                               (4, 1, 0, date.fromisoformat('2010-01-01'), 1),
                               (5, 1, 0, date.fromisoformat('2010-01-01'), 1),
                               (6, 1, 0, date.fromisoformat('2010-01-01'), 1),
                               (7, 1, 0, date.fromisoformat('2010-01-01'), 1)]
        }]

        self.default_test(tables_and_counts)
