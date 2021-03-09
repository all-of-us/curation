"""
Integration test for repopulate_person_controlled_tier module

Original Issues: DC-1439

The intent is to repopulate the person table using the PPI responses based on the controlled tier 
privacy requirements """

# Python Imports
import os
from dateutil import parser

from google.cloud.bigquery import Table

# Project Imports
from common import PERSON, OBSERVATION, VOCABULARY_TABLES
from utils import bq
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.repopulate_person_controlled_tier import \
    RepopulatePersonControlledTier, GENERALIZED_RACE_CONCEPT_ID, GENERALIZED_RACE_SOURCE_VALUE, \
    AOU_NONE_INDICATED_CONCEPT_ID, AOU_NONE_INDICATED_SOURCE_VALUE, \
    GENERALIZED_GENDER_IDENTITY_CONCEPT_ID, GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE, \
    HISPANIC_LATINO_CONCEPT_ID, HISPANIC_LATINO_CONCEPT_SOURCE_VALUE
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest


class RepopulatePersonControlledTierTestBase(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = RepopulatePersonControlledTier(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [PERSON, OBSERVATION] + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for sandbox_table_name in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

        # Copy vocab tables over to the test dataset
        cls.copy_vocab_tables(cls.vocabulary_id)

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
        # Load the test data
        person_data_template = self.jinja_env.from_string("""
            DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.person`;
            CREATE TABLE `{{project_id}}.{{dataset_id}}.person`
            AS (
                WITH w AS (
                    SELECT ARRAY<STRUCT<
                        person_id int64,
                        gender_concept_id int64,
                        year_of_birth int64,
                        month_of_birth int64,
                        day_of_birth int64,
                        birth_datetime datetime,
                        race_concept_id int64,
                        ethnicity_concept_id int64,
                        location_id int64,
                        provider_id int64,
                        care_site_id int64,
                        person_source_value string,
                        gender_source_value string,
                        gender_source_concept_id int64,
                        race_source_value string,
                        race_source_concept_id int64,
                        ethnicity_source_value string,
                        ethnicity_source_concept_id int64
                        >>
                      [(1, 0, 1990, 1, 1, DATETIME(TIMESTAMP '1990-01-01T00:00:01'), 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
                       (2, 0, 1980, 1, 1, DATETIME(TIMESTAMP '1980-01-01T00:00:01'), 0, 0, 1, 1, 1, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0)] col
                )
                SELECT 
                    person_id,
                    gender_concept_id,
                    year_of_birth,
                    month_of_birth,
                    day_of_birth,
                    birth_datetime,
                    race_concept_id,
                    ethnicity_concept_id,
                    location_id,
                    provider_id,
                    care_site_id,
                    person_source_value,
                    gender_source_value,
                    gender_source_concept_id,
                    race_source_value,
                    race_source_concept_id,
                    ethnicity_source_value,
                    ethnicity_source_concept_id
                FROM w, UNNEST(w.col)
            )
        """)

        observation_data_template = self.jinja_env.from_string("""
            DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.observation`;
            CREATE TABLE `{{project_id}}.{{dataset_id}}.observation`
            AS (
            WITH w AS (
                SELECT ARRAY<STRUCT<
                        observation_id int64, 
                        person_id int64, 
                        observation_datetime datetime,
                        value_as_concept_id int64,
                        observation_source_concept_id int64,
                        value_source_concept_id int64
                        >>
                      [(1, 1, DATETIME(TIMESTAMP '2020-01-01T00:00:01'), 45877987, 1586140, 1586146),
                       (2, 1, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 1586143, 1586140, 1586143),
                       (3, 2, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 45879439, 1586140, 1586142),
                       (4, 2, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 1586147, 1586140, 1586147),
                       (5, 1, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 45878463, 1585838, 1585840),
                       (6, 2, DATETIME(TIMESTAMP '2020-01-01T00:00:01'), 45880669, 1585838, 1585839),
                       (7, 2, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 1585841, 1585838, 1585841),
                       (8, 1, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 45878463, 1585845, 1585847),
                       (9, 2, DATETIME(TIMESTAMP '2020-01-01T00:00:00'), 45880669, 1585845, 1585846)] col
                )
                SELECT 
                    observation_id, 
                    person_id, 
                    observation_datetime,
                    value_as_concept_id,
                    observation_source_concept_id,
                    value_source_concept_id
                FROM w, UNNEST(w.col)
            )
        """)
        insert_person_query = person_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{insert_person_query};
                {insert_observation_query};'''
        ])

    def test_repopulate_person_controlled_tier(self):

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.person',
            'loaded_ids': [1, 2],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'month_of_birth', 'day_of_birth', 'birth_datetime',
                'race_concept_id', 'ethnicity_concept_id', 'location_id',
                'provider_id', 'care_site_id', 'person_source_value',
                'gender_source_value', 'gender_source_concept_id',
                'race_source_value', 'race_source_concept_id',
                'ethnicity_source_value', 'ethnicity_source_concept_id',
                'sex_at_birth_concept_id', 'sex_at_birth_source_concept_id',
                'sex_at_birth_source_value'
            ],
            'cleaned_values': [
                (1, 45878463, 1990, None, None,
                 parser.parse('1990-06-15 00:00:00 UTC'),
                 GENERALIZED_RACE_CONCEPT_ID, AOU_NONE_INDICATED_CONCEPT_ID,
                 None, None, None, 'person_source_value',
                 'GenderIdentity_Woman', 1585840, GENERALIZED_RACE_SOURCE_VALUE,
                 GENERALIZED_RACE_CONCEPT_ID, AOU_NONE_INDICATED_SOURCE_VALUE,
                 AOU_NONE_INDICATED_CONCEPT_ID, 45878463, 1585847,
                 'SexAtBirth_Female'),
                (2, GENERALIZED_GENDER_IDENTITY_CONCEPT_ID, 1980, None, None,
                 parser.parse('1980-06-15 00:00:00 UTC'), 8515, 38003563, 1, 1,
                 1, 'person_source_value',
                 GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE,
                 GENERALIZED_GENDER_IDENTITY_CONCEPT_ID,
                 'WhatRaceEthnicity_Asian', 1586142,
                 HISPANIC_LATINO_CONCEPT_SOURCE_VALUE,
                 HISPANIC_LATINO_CONCEPT_ID, 45880669, 1585846,
                 'SexAtBirth_Male')
            ]
        }]

        self.default_test(tables_and_counts)
