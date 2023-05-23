"""
Integration test for motor_vehicle_accident_suppression module

Original Issues: DC-1367

The intent is to suppress the vehicle accident related concepts across all *concept_id fields in 
all domain tables 
"""

# Python Imports
import os

# Project Imports
from common import AOU_DEATH, CONDITION_OCCURRENCE, DEATH, OBSERVATION, VOCABULARY_TABLES
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.motor_vehicle_accident_suppression import (
    MotorVehicleAccidentSuppression, SUPPRESSION_RULE_CONCEPT_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class MotorVehicleAccidentSuppressionTestBase(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = MotorVehicleAccidentSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        for table_name in [AOU_DEATH, CONDITION_OCCURRENCE, DEATH, OBSERVATION
                          ] + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in [AOU_DEATH, CONDITION_OCCURRENCE, DEATH, OBSERVATION]:
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')
        # Add SUPPRESSION_RULE_CONCEPT_TABLE to fq_sandbox_table_names so it gets deleted after
        # the test
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}'
        )

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

        insert_condition_occurrence = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
            (
                condition_occurrence_id, 
                person_id, 
                condition_concept_id, 
                condition_type_concept_id,
                condition_source_concept_id,
                condition_status_concept_id,
                condition_start_date,
                condition_start_datetime
            )
            VALUES
              -- Concepts to suppress --
              -- 443645: Injury in water transport caused by loading machinery, swimmer injured --
              -- 4106309: Driver in watercraft accident --
                
              -- Concepts to keep --
              -- 201826: Type 2 Diabetes Mellitus --
              -- 320128: Essential hypertension --
              (1, 1, 443645, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (2, 1, 0, 4106309, 0, 0, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (3, 1, 0, 0, 443645, 0, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (4, 1, 0, 0, 0, 4106309, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (5, 1, 443645, 443645, 443645, 443645, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (6, 1, 201826, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (7, 1, 0, 201826, 0, 0, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (8, 1, 0, 0, 320128, 0, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (9, 1, 0, 0, 0, 320128, '2020-01-01', '2020-01-01 00:00:00 UTC'),
              (10, 1, 201826, 201826, 320128, 320128, '2020-01-01', '2020-01-01 00:00:00 UTC')
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        insert_observation = self.jinja_env.from_string("""
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
              -- 46271039: Motor vehicle accident with ejection of person from vehicle --
              -- 4305858: Voluntary parachute descent accident --
              -- 44791617: Pedal cycle accident involving collision between pedal cycle and motor vehicle --
              -- 4145511: Automobile accident --
              -- 44803087: Collision of spacecraft with other spacecraft --
              
              -- Concepts to keep --
              -- 42628400: Follow-up service --
              -- 1332785: Smoking more cigarettes or vaping more --
              -- 43533330: Left main coronary artery --
              (1, 1, 46271039, 0, 0, null, null, 0, 0, '2020-01-01'),
              (2, 1, 0, 4305858, 0, 0, 0, 0, 0, '2020-01-01'),
              (3, 1, 0, 0, 44791617, 0, 0, 0, 0, '2020-01-01'),
              (4, 1, 0, 0, 0, 4145511, 0, 0, 0, '2020-01-01'),
              (5, 1, 0, 0, 0, 0, 44803087, 0, 0, '2020-01-01'),
              (6, 1, 42628400, 4305858, 44791617, 4145511, 44803087, 0, 0, '2020-01-01'),
              (7, 1, 42628400, 0, 0, null, null, 0, 0, '2020-01-01'),
              (8, 1, 0, 1332785, 0, 0, 0, 0, 0, '2020-01-01'),
              (9, 1, 0, 0, 43533330, 0, 0, 0, 0, '2020-01-01'),
              (10, 1, 0, 0, 0, 43533330, 0, 0, 0, '2020-01-01'),
              (11, 1, 42628400, 1332785, 43533330, 43533330, 0, 0, 0, '2020-01-01')
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        insert_death = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.death`
                (person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id)
            VALUES
                -- NOT dropped. No motor vehicle accident concept --
                (1, date('2020-05-05'), 38003569, 321042, 1569168),
                -- Dropped. One or more columns have motor vehicle accident concept --
                (2, date('2020-05-05'), 4145511, 321042, 1569168),
                (3, date('2020-05-05'), 38003569, 4145511, 1569168),
                (4, date('2020-05-05'), 38003569, 321042, 4145511),
                (5, date('2020-05-05'), 4145511, 4145511, 4145511)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        insert_aou_death = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
                (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
            VALUES
                -- NOT dropped. No motor vehicle accident concept --
                ('a', 1, date('2020-05-05'), 38003569, 321042, 1569168, 'rdr', False),
                -- Dropped. One or more columns have motor vehicle accident concept --
                ('b', 1, date('2020-05-05'), 4145511, 321042, 1569168, 'hpo_b', True),
                ('c', 1, date('2020-05-05'), 38003569, 4145511, 1569168, 'hpo_c', False),
                ('d', 1, date('2020-05-05'), 38003569, 321042, 4145511, 'hpo_d', False),
                ('e', 1, date('2020-05-05'), 4145511, 4145511, 4145511, 'hpo_e', False)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([
            insert_condition_occurrence, insert_observation, insert_death,
            insert_aou_death
        ])

    def test_motor_vehicle_accident(self):

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
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
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_type_concept_id', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'qualifier_concept_id', 'unit_concept_id'
            ],
            'cleaned_values': [(7, 1, 42628400, 0, 0, None, None, 0, 0),
                               (8, 1, 0, 1332785, 0, 0, 0, 0, 0),
                               (9, 1, 0, 0, 43533330, 0, 0, 0, 0),
                               (10, 1, 0, 0, 0, 43533330, 0, 0, 0),
                               (11, 1, 42628400, 1332785, 43533330, 43533330, 0,
                                0, 0)]
        }, {
            'fq_table_name': f'{self.project_id}.{self.dataset_id}.{DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(DEATH)}',
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [2, 3, 4, 5],
            'fields': ['person_id'],
            'cleaned_values': [(1,)]
        }, {
            'fq_table_name': f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(AOU_DEATH)}',
            'loaded_ids': ['a', 'b', 'c', 'd', 'e'],
            'sandboxed_ids': ['b', 'c', 'd', 'e'],
            'fields': ['aou_death_id'],
            'cleaned_values': [('a',)]
        }]

        self.default_test(tables_and_counts)
