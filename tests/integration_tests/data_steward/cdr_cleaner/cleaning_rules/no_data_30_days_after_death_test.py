"""
Integration test for fix_unmapped_survey_answers module

Original Issues: DC-1043, DC-1053

The intent is to map the unmapped survey answers (value_as_concept_ids=0) using 
value_source_concept_id through 'Maps to' relationship """

# Python Imports
import os
import pytz
import datetime

# Project Imports
from common import PERSON, VISIT_OCCURRENCE, OBSERVATION
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.no_data_30_days_after_death import (
    NoDataAfterDeath, get_affected_tables)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
-- The existing person table is created and partitioned on the pseudo column _PARTITIONTIME,
-- partitioning by _PARTITIONTIME doesn't work using a query_statement for creating a table,
-- therefore CREATE OR REPLACE TABLE doesn' work and we need to DROP the table first.
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.person`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.person`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<person_id int64, birth_datetime TIMESTAMP>>
      [(1, '1970-01-01 00:00:00 EST'),
       (2, '1970-01-01 00:00:00 EST'),
       (3, '1970-01-01 00:00:00 EST')] col
)
SELECT person_id, birth_datetime FROM w, UNNEST(w.col))
""")

DEATH_DATA_TEMPLATE = JINJA_ENV.from_string("""
-- The existing death table is created and partitioned on the pseudo column _PARTITIONTIME,
-- partitioning by _PARTITIONTIME doesn't work using a query_statement for creating a table,
-- therefore CREATE OR REPLACE TABLE doesn' work and we need to DROP the table first.
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.death`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.death`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<person_id int64, death_date DATE>>
      [(1, '1969-01-01'),
       (2, '2020-01-01')] col
)
SELECT person_id, death_date FROM w, UNNEST(w.col))
""")

VISIT_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
-- The existing visit_occurrence table is created and partitioned on the pseudo column _PARTITIONTIME,
-- partitioning by _PARTITIONTIME doesn't work using a query_statement for creating a table,
-- therefore CREATE OR REPLACE TABLE doesn' work and we need to DROP the table first.
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.visit_occurrence`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.visit_occurrence`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<visit_occurrence_id int64, person_id int64, visit_start_date DATE, visit_end_date DATE>>
      [(1, 1, '2000-01-01', '2000-01-02'),
       (2, 1, '2000-01-02', '2000-01-03'),
       (3, 2, '2000-01-01', '2020-03-01'),
       (4, 3, '2000-01-02', '2000-01-03')] col
)
SELECT visit_occurrence_id, person_id, visit_start_date, visit_end_date FROM w, UNNEST(w.col))
""")

OBSERVATION_DATA_TEMPLATE = JINJA_ENV.from_string("""
-- The existing observation table is created and partitioned on the pseudo column _PARTITIONTIME,
-- partitioning by _PARTITIONTIME doesn't work using a query_statement for creating a table,
-- therefore CREATE OR REPLACE TABLE doesn' work and we need to DROP the table first.
DROP TABLE IF EXISTS `{{project_id}}.{{dataset_id}}.observation`;
CREATE TABLE `{{project_id}}.{{dataset_id}}.observation`
AS (
WITH w AS (
  SELECT ARRAY<STRUCT<observation_id int64, person_id int64, observation_date DATE>>
      [(1, 1, '2000-01-01'),
       (2, 1, '2000-01-02'),
       (3, 2, '2020-03-01'),
       (4, 2, '2020-01-05'),
       (5, 3, '2020-05-05')] col
)
SELECT observation_id, person_id, observation_date FROM w, UNNEST(w.col))
""")


class NoDataAfterDeathTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = NoDataAfterDeath(cls.project_id, cls.dataset_id,
                                             cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table_name in get_affected_tables():
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
        # Create the observation, concept, and concept_relationship tables required for the test
        super().setUp()

        person_data_query = PERSON_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        death_data_query = DEATH_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        visit_occurrence_data_query = VISIT_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        observation_data_query = OBSERVATION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{person_data_query};
                {death_data_query};
                {visit_occurrence_data_query};
                {observation_data_query}'''
        ])

    def test_no_data_30_days_after_death(self):
        """
        1. person_id=1, this hypothetical person passed away on 1969-01-01, which is a year 
        before the person was born. This person should be removed. This person also had two 
        visits ( visit_occurrence_id=1, visit_occurrence_id=2) that occurred after 2000, 
        they should be removed too. The person had two observations (observation_id=1, 
        observation=2) that occurred after 2000, they should be removed. 
        
        2. person_id=2, this hypothetical person passed away on 2020-01-01. This person had a 
        visit (visit_occurrence_id = 3) that started on 2000-01-01 and ended on 2020-03-01, 
        because we take the maximum date of (visit_start_date and visit_end_date), this visit 
        exceeded the death date more than 30 days, therefore it should be removed. 
        
        3. person_id=3, this hypothetical person didn't pass away, so we keep all of the records 
        associated with this person. 
         
        """

        # BigQuery returns timestamp in UTC, the expected datetime needs to be converted to UTC.
        local_timezone = pytz.timezone('US/Eastern')
        native_time = datetime.datetime.strptime('1970-01-01 00:00:00',
                                                 '%Y-%m-%d %H:%M:%S')
        expected_birth_datetime = local_timezone.localize(
            native_time, is_dst=None).astimezone(pytz.utc)
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(PERSON)}',
            'loaded_ids': [1, 2, 3],
            'sandboxed_ids': [1],
            'fields': ['person_id', 'birth_datetime'],
            'cleaned_values': [(2, expected_birth_datetime),
                               (3, expected_birth_datetime)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{VISIT_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(VISIT_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1, 2, 3],
            'fields': [
                'visit_occurrence_id', 'person_id', 'visit_start_date',
                'visit_end_date'
            ],
            'cleaned_values': [
                (4, 3, datetime.datetime.strptime('2000-01-02',
                                                  '%Y-%m-%d').date(),
                 datetime.datetime.strptime('2000-01-03', '%Y-%m-%d').date())
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [1, 2, 3],
            'fields': ['observation_id', 'person_id', 'observation_date'],
            'cleaned_values': [
                (4, 2, datetime.datetime.strptime('2020-01-05',
                                                  '%Y-%m-%d').date()),
                (5, 3, datetime.datetime.strptime('2020-05-05',
                                                  '%Y-%m-%d').date())
            ]
        }]

        self.default_test(tables_and_counts)
