"""
Integration test for StringFieldsSuppression module

Original Issues: DC-1369

The intent is to null all STRING type fields in all OMOP common data model tables """

# Python Imports
import os

# Project Imports
from common import AOU_DEATH, CONDITION_OCCURRENCE, OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.string_fields_suppression import StringFieldsSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class StringFieldsSuppressionTestBase(BaseTest.CleaningRulesTestBase):

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
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"

        cls.rule_instance = StringFieldsSuppression(cls.project_id,
                                                    cls.dataset_id,
                                                    cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [CONDITION_OCCURRENCE, OBSERVATION, AOU_DEATH]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for sandbox_table_name in cls.rule_instance.get_sandbox_tablenames():
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
        insert_condition_occurrence = self.jinja_env.from_string("""
            CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.condition_occurrence`
            (
              condition_occurrence_id int64, 
              person_id int64, 
              condition_concept_id int64, 
              stop_reason STRING,
              condition_source_value STRING,
              condition_status_source_value STRING)
            AS (
            WITH w AS (
              SELECT ARRAY<STRUCT<
                    condition_occurrence_id int64, 
                    person_id int64, 
                    condition_concept_id int64, 
                    stop_reason STRING,
                    condition_source_value STRING,
                    condition_status_source_value STRING
                    >>
                  [(1, 1, 0, 'stop reason', 'source value', 'status'),
                   (2, 1, 0, 'stop reason', 'source value', 'status'),
                   (3, 1, 0, 'stop reason', 'source value', 'status'),
                   (4, 1, 0, 'stop reason', 'source value', 'status')] col
            )
            SELECT 
                condition_occurrence_id, 
                person_id, 
                condition_concept_id, 
                stop_reason,
                condition_source_value,
                condition_status_source_value 
            FROM w, UNNEST(w.col))
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        insert_observation = self.jinja_env.from_string("""
            CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.observation`
            (
                observation_id int64,
                person_id int64,
                observation_concept_id int64,
                observation_source_concept_id int64,
                value_as_string STRING,
                observation_source_value STRING,
                unit_source_value STRING,
                qualifier_source_value STRING,
                value_source_value STRING
            )
            AS (
            -- 1585250 corresponds to the zipcode concept that is not subject to string suppression, value_as_string for this record should be kept --
            WITH w AS (
              SELECT ARRAY<STRUCT<
                    observation_id int64, 
                    person_id int64, 
                    observation_concept_id int64,
                    observation_source_concept_id int64,
                    value_as_string STRING, 
                    observation_source_value STRING,
                    unit_source_value STRING,
                    qualifier_source_value STRING,
                    value_source_value STRING
                    >>
                  [(1, 1, 0, 1585250, '111111', 'observation_source_value', 'unit_source_value', 'qualifier_source_value', 'value_source_value'),
                   (2, 1, 0, 0, 'value_as_string', 'observation_source_value', 'unit_source_value', 'qualifier_source_value', 'value_source_value'),
                   (3, 1, 0, 0, 'value_as_string', 'observation_source_value', 'unit_source_value', 'qualifier_source_value', 'value_source_value'),
                   (4, 1, 0, 0, 'value_as_string', 'observation_source_value', 'unit_source_value', 'qualifier_source_value', 'value_source_value'),
                   (5, 1, 0, 0, 'value_as_string', 'observation_source_value', 'unit_source_value', 'qualifier_source_value', 'value_source_value'),
                   (6, 1, 0, 715711, 'foo_date', 'observation_source_value', 'unit_source_value', 'qualifier_source_value', 'value_source_value')] col
            )
            SELECT 
                observation_id,
                person_id,
                observation_concept_id,
                observation_source_concept_id,
                value_as_string, 
                observation_source_value,
                unit_source_value,
                qualifier_source_value,
                value_source_value 
            FROM w, UNNEST(w.col))
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        insert_aou_death = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
            (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id, src_id, primary_death_record)
        VALUES
            ('a1', 1, date('2020-05-05'), 0, 0, 'dummy concept value', 0, 'rdr', False),
            ('a2', 1, date('2021-05-05'), 0, 0, NULL, 0, 'hpo_a', True)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data(
            [insert_condition_occurrence, insert_observation, insert_aou_death])

    def test_string_suppression(self):

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'stop_reason', 'condition_source_value',
                'condition_status_source_value'
            ],
            'cleaned_values': [(1, 1, 0, None, None, None),
                               (2, 1, 0, None, None, None),
                               (3, 1, 0, None, None, None),
                               (4, 1, 0, None, None, None)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [1, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'value_as_string',
                'observation_source_value', 'unit_source_value',
                'qualifier_source_value', 'value_source_value'
            ],
            'cleaned_values': [
                (1, 1, 0, 1585250, '111111', None, None, None, None),
                (2, 1, 0, 0, None, None, None, None, None),
                (3, 1, 0, 0, None, None, None, None, None),
                (4, 1, 0, 0, None, None, None, None, None),
                (5, 1, 0, 0, None, None, None, None, None),
                (6, 1, 0, 715711, 'foo_date', None, None, None, None)
            ]
        }, {
            'fq_table_name': f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'loaded_ids': ['a1', 'a2'],
            'sandboxed_ids': [],
            'fields': ['aou_death_id', 'cause_source_value', 'src_id'],
            'cleaned_values': [('a1', None, 'rdr'), ('a2', None, 'hpo_a')]
        }]

        self.default_test(tables_and_counts)
