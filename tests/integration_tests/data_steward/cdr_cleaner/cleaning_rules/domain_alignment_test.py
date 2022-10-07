"""
Integration test for domain_alignment module
"""

# Python Imports
import os
from datetime import date
from dateutil.parser import parse
import pytz

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.domain_alignment import DomainAlignment, LOOKUP_TABLE
from cdr_cleaner.cleaning_rules.domain_mapping import DOMAIN_TABLE_NAMES
from common import CONDITION_OCCURRENCE, OBSERVATION, PROCEDURE_OCCURRENCE, VOCABULARY_TABLES
from resources import mapping_table_for
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DomainAlignmentTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = DomainAlignment(cls.project_id, cls.dataset_id,
                                            cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in DOMAIN_TABLE_NAMES + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # Generates list of fully qualified mapping table names
        for table_name in DOMAIN_TABLE_NAMES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{mapping_table_for(table_name)}'
            )
            sandbox_table = f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_name)}'
            cls.fq_sandbox_table_names.append(sandbox_table)

        sandbox_table = f'{cls.project_id}.{cls.sandbox_id}.{LOOKUP_TABLE}'
        cls.fq_sandbox_table_names.append(sandbox_table)

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create domain tables required for the test
        super().setUp()

        # Copy vocab tables over to the test dataset
        self.copy_vocab_tables(self.vocabulary_id)

        # Load the test data
        condition_occurrence_data_template = self.jinja_env.from_string(
            """INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
                (condition_occurrence_id, person_id, condition_concept_id, 
                condition_start_date, condition_start_datetime, condition_type_concept_id,
                visit_occurrence_id)
                -- 36676219 is a procedure and would be rerouted to procedure_occurrence --
                -- 3009160 is a lab test (measurement) and rerouting from condition_occurrence --
                -- to measurement is not possible, therefore this record would be dropped --
                VALUES
                (100, 1, 201826, '2015-07-15', timestamp('2015-07-15'), 42894222, 1),
                (101, 2, 36676219, '2015-07-15', timestamp('2015-07-15'), 42865906, 2),
                (102, 3, 201826, '2015-07-15', timestamp('2015-07-15'), 42894222, 3),
                (103, 4, 201826, '2015-07-15', timestamp('2015-07-15'), 42894222, 4),
                (104, 5, 3009160, '2015-07-15', timestamp('2015-07-15'), 42894222, 4)"""
        )

        mapping_condition_occurrence_data_template = self.jinja_env.from_string(
            """INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_condition_occurrence`
                (condition_occurrence_id, src_dataset_id, src_condition_occurrence_id, 
                 src_hpo_id, src_table_id)
                VALUES
                (100, '{{dataset_id}}', 1, 'hpo_1', 'condition_occurrence'),
                (101, '{{dataset_id}}', 2, 'hpo_2', 'condition_occurrence'),
                (102, '{{dataset_id}}', 3, 'hpo_3', 'condition_occurrence'),
                (103, '{{dataset_id}}', 4, 'hpo_4', 'condition_occurrence'),
                (104, '{{dataset_id}}', 5, 'hpo_5', 'condition_occurrence')""")

        procedure_occurrence_tmpl = self.jinja_env.from_string(
            """INSERT INTO `{{project_id}}.{{dataset_id}}.procedure_occurrence`
                (procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, 
                 procedure_datetime, procedure_type_concept_id, visit_occurrence_id)
                 -- 320128 is essential hypertension (condition) and would be rerouted --
                 -- to condition_occurrence --
                VALUES
                (200, 5, 36676219, '2015-07-15', timestamp('2015-07-15'), 42865906, 5),
                (201, 6, 320128, '2015-08-15', timestamp('2015-08-15'), 42894222, 6)"""
        )
        mapping_procedure_occurrence_data_template = self.jinja_env.from_string(
            """INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_procedure_occurrence`
                (procedure_occurrence_id, src_dataset_id, src_procedure_occurrence_id, 
                 src_hpo_id, src_table_id)
                VALUES
                (200, '{{dataset_id}}', 10, 'hpo_1', 'procedure_occurrence'),
                (201, '{{dataset_id}}', 20, 'hpo_2', 'procedure_occurrence')""")

        observation_data_tmpl = self.jinja_env.from_string(
            """INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (observation_id, person_id, observation_concept_id, observation_date, 
            observation_type_concept_id, observation_source_value, observation_source_concept_id)
            VALUES
            (101, 1001, 8621, '2015-07-15', 45905771, 'ipaq_1_cope_a_24', 1332870),
            (102, 1002, 8621, '2015-07-15', 45905771, 'ipaq_3_cope_a_24', 1332871),
            (103, 1003, 8621, '2015-07-15', 45905771, 'ipaq_5_cope_a_24', 1332872),
            (104, 1004, 61909002, '2015-07-15', 45905771, 'Language_SpokenWrittenLanguage', 1585413),
            -- will be transferred to condition_occurence --
            (105, 1005, 45769242, '2015-07-15', 44814721, 'IS_ILLICIT_DRUG_USER', 0)
        """)

        mapping_observation_data_template = self.jinja_env.from_string(
            """INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_observation`
                (observation_id, src_dataset_id, src_observation_id, 
                 src_hpo_id, src_table_id)
                VALUES
                (101, '{{dataset_id}}', 10, 'hpo_1', 'observation'),
                (102, '{{dataset_id}}', 20, 'hpo_2', 'observation'),
                (103, '{{dataset_id}}', 30, 'hpo_3', 'observation'),
                (104, '{{dataset_id}}', 40, 'hpo_4', 'observation'),
                -- will be transferred to _mapping_condition_occurence --
                (105, '{{dataset_id}}', 50, 'hpo_5', 'observation')
            """)

        insert_condition_query = condition_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_condition_mapping_query = mapping_condition_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_procedure_query = procedure_occurrence_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_procedure_mapping_query = mapping_procedure_occurrence_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_observation_query = observation_data_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        insert_observation_mapping_query = mapping_observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            insert_condition_query, insert_condition_mapping_query,
            insert_procedure_query, insert_procedure_mapping_query,
            insert_observation_query, insert_observation_mapping_query
        ])

    def test_domain_alignment(self):

        self.maxDiff = None

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
            'loaded_ids': [100, 101, 102, 103, 104],
            'sandboxed_ids': [101, 104],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (100, 1, 201826, date(2015, 7, 15),
                 parse('2015-07-15 00:00:00 UTC').astimezone(pytz.utc),
                 42894222, 1),
                (102, 3, 201826, date(2015, 7, 15),
                 parse('2015-07-15 00:00:00 UTC').astimezone(pytz.utc),
                 42894222, 3),
                (103, 4, 201826, date(2015, 7, 15),
                 parse('2015-07-15 00:00:00 UTC').astimezone(pytz.utc),
                 42894222, 4),
                (106, 1005, 45769242, date(2015, 7, 15),
                 parse('1970-01-01 00:00:00 UTC').astimezone(pytz.utc), 0,
                 None),
                (105, 6, 320128, date(2015, 8, 15),
                 parse('2015-08-15 00:00:00 UTC').astimezone(pytz.utc), 0, 6)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PROCEDURE_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(PROCEDURE_OCCURRENCE)}',
            'loaded_ids': [200, 201],
            'sandboxed_ids': [201],
            'fields': [
                'procedure_occurrence_id', 'person_id', 'procedure_concept_id',
                'procedure_date', 'procedure_datetime',
                'procedure_type_concept_id', 'visit_occurrence_id'
            ],
            'cleaned_values': [
                (200, 5, 36676219, date(2015, 7, 15),
                 parse('2015-07-15 00:00:00 UTC').astimezone(pytz.utc),
                 42865906, 5),
                (202, 2, 36676219, date(2015, 7, 15),
                 parse('2015-07-15 00:00:00 UTC'), 0, 2)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [101, 102, 103, 104, 105],
            'sandboxed_ids': [105],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_value', 'observation_source_concept_id'
            ],
            'cleaned_values': [
                (101, 1001, 8621, date(2015, 7, 15), 45905771,
                 'ipaq_1_cope_a_24', 1332870),
                (102, 1002, 8621, date(2015, 7, 15), 45905771,
                 'ipaq_3_cope_a_24', 1332871),
                (103, 1003, 8621, date(2015, 7, 15), 45905771,
                 'ipaq_5_cope_a_24', 1332872),
                (104, 1004, 61909002, date(2015, 7, 15), 45905771,
                 'Language_SpokenWrittenLanguage', 1585413),
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}._mapping_condition_occurrence',
            'loaded_ids': [100, 101, 102, 103, 104],
            'fields': [
                'condition_occurrence_id', 'src_dataset_id',
                'src_condition_occurrence_id', 'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (100, self.dataset_id, 1, 'hpo_1', 'condition_occurrence'),
                (102, self.dataset_id, 3, 'hpo_3', 'condition_occurrence'),
                (103, self.dataset_id, 4, 'hpo_4', 'condition_occurrence'),
                (106, self.dataset_id, 50, 'hpo_5', 'observation'),
                (105, self.dataset_id, 20, 'hpo_2', 'procedure_occurrence')
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}._mapping_procedure_occurrence',
            'loaded_ids': [200, 201],
            'fields': [
                'procedure_occurrence_id', 'src_dataset_id',
                'src_procedure_occurrence_id', 'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (200, self.dataset_id, 10, 'hpo_1', 'procedure_occurrence'),
                (202, self.dataset_id, 2, 'hpo_2', 'condition_occurrence')
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}._mapping_observation',
            'loaded_ids': [101, 102, 103, 104, 105],
            'fields': [
                'observation_id', 'src_dataset_id', 'src_observation_id',
                'src_hpo_id', 'src_table_id'
            ],
            'cleaned_values': [
                (101, self.dataset_id, 10, 'hpo_1', 'observation'),
                (102, self.dataset_id, 20, 'hpo_2', 'observation'),
                (103, self.dataset_id, 30, 'hpo_3', 'observation'),
                (104, self.dataset_id, 40, 'hpo_4', 'observation'),
            ]
        }]

        self.default_test(tables_and_counts)
