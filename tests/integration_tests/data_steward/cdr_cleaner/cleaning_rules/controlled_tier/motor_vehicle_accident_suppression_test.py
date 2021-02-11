"""
Integration test for fix_unmapped_survey_answers module

Original Issues: DC-1043, DC-1053

The intent is to map the unmapped survey answers (value_as_concept_ids=0) using 
value_source_concept_id through 'Maps to' relationship """

# Python Imports
import os

from google.cloud.bigquery import Table

# Project Imports
from common import CONDITION_OCCURRENCE, OBSERVATION, VOCABULARY_TABLES
from utils import bq
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.controlled_tier.motor_vehicle_accident_suppression import \
    MotorVehicleAccidentSuppression, SUPPRESSION_RULE_CONCEPT_TABLE
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class MotorVehicleAccidentSuppressionTestBase(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = MotorVehicleAccidentSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [CONDITION_OCCURRENCE, OBSERVATION
                          ] + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in [
                CONDITION_OCCURRENCE, OBSERVATION,
                SUPPRESSION_RULE_CONCEPT_TABLE
        ]:
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

        # Copy vocab tables over to the test dataset
        cls.copy_vocab_tables(cls.vocabulary_id)

        # Build the lookup table manually here
        cls.rule_instance.create_suppression_lookup_table(cls.client)

    @classmethod
    def copy_vocab_tables(cls, vocabulary_id):
        """
        A function for copying the vocab tables to the test dataset_id
        :param vocabulary_id: 
        :return: 
        """
        # Copy vocab tables over to the test dataset
        vocabulary_dataset = bq.get_dataset(cls.project_id, vocabulary_id)
        for src_table in bq.list_tables(cls.client, vocabulary_dataset):
            schema = bq.get_table_schema(src_table.table_id)
            destination = f'{cls.project_id}.{cls.dataset_id}.{src_table.table_id}'
            dst_table = cls.client.create_table(Table(destination,
                                                      schema=schema),
                                                exists_ok=True)
            cls.client.copy_table(src_table, dst_table)

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
                    condition_concept_id int64, 
                    condition_type_concept_id int64,
                    condition_source_concept_id int64,
                    condition_status_concept_id int64
                    >>
                    # Concepts to suppress
                    # 443645: Injury in water transport caused by loading machinery, swimmer injured
                    # 4106309: Driver in watercraft accident
                    
                    # Concepts to keep
                    # 201826: Type 2 Diabetes Mellitus
                    # 320128: Essential hypertension
                  [(1, 1, 443645, 0, 0, 0),
                   (2, 1, 0, 4106309, 0, 0),
                   (3, 1, 0, 0, 443645, 0),
                   (4, 1, 0, 0, 0, 4106309),
                   (5, 1, 443645, 443645, 443645, 443645),
                   (6, 1, 201826, 0, 0, 0),
                   (7, 1, 0, 201826, 0, 0),
                   (8, 1, 0, 0, 320128, 0),
                   (9, 1, 0, 0, 0, 320128),
                   (10, 1, 201826, 201826, 320128, 320128)] col
            )
            SELECT 
                condition_occurrence_id, 
                person_id, 
                condition_concept_id, 
                condition_type_concept_id,
                condition_source_concept_id,
                condition_status_concept_id 
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
                    observation_concept_id int64, 
                    observation_type_concept_id int64,
                    value_as_concept_id int64,
                    observation_source_concept_id int64,
                    value_source_concept_id int64,
                    qualifier_concept_id int64,
                    unit_concept_id int64
                    >>
                  # Concepts to suppress
                  # 46271039: Motor vehicle accident with ejection of person from vehicle
                  # 4305858: Voluntary parachute descent accident
                  # 44791617: Pedal cycle accident involving collision between pedal cycle and motor vehicle
                  # 4145511: Automobile accident
                  # 44803087: Collision of spacecraft with other spacecraft
                  
                  # Concepts to keep
                  # 42628400: Follow-up service
                  # 1332785: Smoking more cigarettes or vaping more
                  # 43533330: Left main coronary artery
                  [(1, 1, 46271039, 0, 0, 0, 0, 0, 0),
                   (2, 1, 0, 4305858, 0, 0, 0, 0, 0),
                   (3, 1, 0, 0, 44791617, 0, 0, 0, 0),
                   (4, 1, 0, 0, 0, 4145511, 0, 0, 0),
                   (5, 1, 0, 0, 0, 0, 44803087, 0, 0),
                   (6, 1, 42628400, 4305858, 44791617, 4145511, 44803087, 0, 0),
                   (7, 1, 42628400, 0, 0, 0, 0, 0, 0),
                   (8, 1, 0, 1332785, 0, 0, 0, 0, 0),
                   (9, 1, 0, 0, 43533330, 0, 0, 0, 0),
                   (10, 1, 0, 0, 0, 43533330, 0, 0, 0),
                   (11, 1, 42628400, 1332785, 43533330, 43533330, 0, 0, 0)] col
            )
            SELECT 
                observation_id, 
                person_id, 
                observation_concept_id, 
                observation_type_concept_id,
                value_as_concept_id,
                observation_source_concept_id,
                value_source_concept_id,
                qualifier_concept_id,
                unit_concept_id
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

    def test_motor_vehicle_accident(self):

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.condition_occurrence',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for("condition_occurrence")}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 3, 4, 5],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_type_concept_id', 'condition_source_concept_id',
                'condition_status_concept_id'
            ],
            'cleaned_values': [(6, 1, 201826, 0, 0, 0), (7, 1, 0, 201826, 0, 0),
                               (8, 1, 0, 0, 320128, 0), (9, 1, 0, 0, 0, 320128),
                               (10, 1, 201826, 201826, 320128, 320128)]
        }, {
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
            'cleaned_values': [(7, 1, 42628400, 0, 0, 0, 0, 0, 0),
                               (8, 1, 0, 1332785, 0, 0, 0, 0, 0),
                               (9, 1, 0, 0, 43533330, 0, 0, 0, 0),
                               (10, 1, 0, 0, 0, 43533330, 0, 0, 0),
                               (11, 1, 42628400, 1332785, 43533330, 43533330, 0,
                                0, 0)]
        }]

        self.default_test(tables_and_counts)
