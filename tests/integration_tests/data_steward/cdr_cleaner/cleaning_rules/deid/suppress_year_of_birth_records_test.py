"""
Integration test for year_of_birth_records_suppression module

Original Issues: DC-1977, DC-2205

The intent is to suppress all records (specifically in the CT) if a concept is associated with a delivery or birth concept
and the record date/start_date is within a participant's year of birth."""

# Python Imports
import os
import datetime

# Project Imports
from common import AOU_DEATH, DEATH, JINJA_ENV, OBSERVATION, PERSON, VISIT_OCCURRENCE
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.year_of_birth_records_suppression import (
    YearOfBirthRecordsSuppression, LOOKUP_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

AFFECTED_TABLES = [AOU_DEATH, DEATH, OBSERVATION, PERSON, VISIT_OCCURRENCE]

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, birth_datetime, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
      /* Adding participants with different ranges of birthdays.*/
      /* The data belonging to participants with delivery or birth concepts with a start date < birth year + 2 */
      /* should be dropped from all the domain tables.*/  
      (1, '1970-01-01 00:00:00 UTC', 0, 1970, 0, 0),
      (2, '2002-01-01 00:00:00 UTC', 0, 2002, 0, 0),
      (3, '2003-01-01 00:00:00 UTC', 0, 2003, 0, 0),
      (4, '2015-01-01 00:00:00 UTC', 0, 2015, 0, 0)
""")
VISIT_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.visit_occurrence`
      (visit_occurrence_id, person_id, visit_start_date, visit_start_datetime, visit_end_date, visit_end_datetime, 
      visit_concept_id, visit_type_concept_id)
VALUES
    /* For this data, visit_occurrence_id 2 and 4 should be dropped.*/
    /* visit_occurrence_id 2 is within the same year as participant's year of birth.*/
    /* visit_occurrence_id 4 exactly matches the participant's birthday.*/    
      (1, 1, '2020-01-01', '2020-01-01 00:00:00 UTC', '2020-01-02', '2020-01-02 00:00:00 UTC', 0, 0),
      (2, 3, '2003-07-03', '2003-07-03 00:00:00 UTC', '2003-07-30', '2003-07-30 00:00:00 UTC', 0, 0),
      (3, 2, '2020-01-01', '2020-01-01 00:00:00 UTC', '2020-03-01', '2020-03-01 00:00:00 UTC', 0, 0),
      (4, 4, '2015-01-01', '2015-01-01 00:00:00 UTC', '2015-01-01', '2015-01-01 00:00:00 UTC', 0, 0)
""")
OBSERVATION_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
    (observation_id, person_id, observation_date, observation_datetime, observation_concept_id, 
    observation_type_concept_id, value_as_concept_id, qualifier_concept_id, unit_concept_id, 
    observation_source_concept_id, value_source_concept_id)
    /* For this data, observation_id 2 and 5 should be dropped.*/
    /* observation_id 2 is within the same year as participant's year of birth.*/
    /* observation_id 5 is the last possible date that is less than participant's birth year + 2 (2005-01-01) */
VALUES
      (1, 1, '2020-06-01', '2020-06-01 00:00:00 UTC', 0, 0, 0, 0, 0, 0, 0),
      (2, 2, '2002-06-01', '2002-06-01 00:00:00 UTC', 0, 0, 0, 0, 0, 0, 0),
      (3, 3, '2020-03-01', '2020-03-01 00:00:00 UTC', 0, 0, 0, 0, 0, 0, 0),
      (4, 4, '2020-01-05', '2020-01-05 00:00:00 UTC', 0, 0, 0, 0, 0, 0, 0),
      (5, 3, '2004-12-31', '2004-12-31 00:00:00 UTC', 0, 0, 0, 0, 0, 0, 0)
""")
DEATH_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.death`
    (person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id)
VALUES
    -- NOT dropped. --
    (1, date('2020-05-05'), 0, 0, 0),
    -- Dropped. Within the same year or +1 as participant's year of birth. --
    (2, date('2002-06-01'), 0, 0, 0)
""")
AOU_DEATH_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
    (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
VALUES
    -- NOT dropped. --
    ('a1', 1, date('2020-05-05'), 0, 0, 0, 'rdr', False),
    ('b1', 1, date('2021-05-05'), 0, 0, 0, 'hpo_b', False),
    ('a2', 2, date('2020-05-05'), 0, 0, 0, 'hpo_a', False),
    ('b2', 2, date('2021-05-05'), 0, 0, 0, 'hpo_b', False),
    -- Dropped. Within the same year or +1 as participant's year of birth. --
    ('c1', 1, date('1970-12-31'), 0, 0, 0, 'hpo_c', True),
    ('d1', 1, date('1971-12-31'), 0, 0, 0, 'hpo_d', False),
    ('c2', 2, date('2002-01-01'), 0, 0, 0, 'rdr', False),
    ('d2', 2, date('2003-01-01'), 0, 0, 0, 'hpo_d', True)
""")


class SuppressYearOfBirthRecordsTest(BaseTest.CleaningRulesTestBase):

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
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = YearOfBirthRecordsSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        # adding death table name for setup/cleanup operations
        for table_name in AFFECTED_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # clean out the lookup table too
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(LOOKUP_TABLE)}'
        )

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
        visit_occurrence_data_query = VISIT_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        observation_data_query = OBSERVATION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        death_data_query = DEATH_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        aou_death_data_query = AOU_DEATH_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            person_data_query, visit_occurrence_data_query,
            observation_data_query, death_data_query, aou_death_data_query
        ])

    def test_suppress_year_of_birth_records(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(DEATH)}',
            'loaded_ids': [1, 2],
            'sandboxed_ids': [2],
            'fields': ['person_id'],
            'cleaned_values': [(1,),]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(AOU_DEATH)}',
            'loaded_ids': ['a1', 'b1', 'c1', 'd1', 'a2', 'b2', 'c2', 'd2'],
            'sandboxed_ids': ['c1', 'd1', 'c2', 'd2'],
            'fields': ['aou_death_id'],
            'cleaned_values': [('a1',), ('b1',), ('a2',), ('b2',)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{VISIT_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(VISIT_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [2, 4],
            'fields': [
                'visit_occurrence_id', 'person_id', 'visit_start_date',
                'visit_end_date'
            ],
            'cleaned_values': [
                (1, 1, datetime.datetime.strptime('2020-01-01',
                                                  '%Y-%m-%d').date(),
                 datetime.datetime.strptime('2020-01-02', '%Y-%m-%d').date()),
                (3, 2, datetime.datetime.strptime('2020-01-01',
                                                  '%Y-%m-%d').date(),
                 datetime.datetime.strptime('2020-03-01', '%Y-%m-%d').date())
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [2, 5],
            'fields': ['observation_id', 'person_id', 'observation_date'],
            'cleaned_values': [
                (1, 1, datetime.datetime.strptime('2020-06-01',
                                                  '%Y-%m-%d').date()),
                (3, 3, datetime.datetime.strptime('2020-03-01',
                                                  '%Y-%m-%d').date()),
                (4, 4, datetime.datetime.strptime('2020-01-05',
                                                  '%Y-%m-%d').date())
            ]
        }]

        self.default_test(tables_and_counts)
