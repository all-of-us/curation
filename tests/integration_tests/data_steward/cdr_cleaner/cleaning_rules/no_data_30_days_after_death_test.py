"""
Integration test for fix_unmapped_survey_answers module

Original Issues: DC-1043, DC-1053, DC-2788

The intent is to map the unmapped survey answers (value_as_concept_ids=0) using
value_source_concept_id through 'Maps to' relationship """

# Python Imports
import os
import datetime
import pytz

# Project Imports
from common import PERSON, VISIT_OCCURRENCE, OBSERVATION
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.no_data_30_days_after_death import (
    NoDataAfterDeath, get_affected_tables)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, birth_datetime, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
      (1, '1970-01-01 00:00:00 EST', 0, 1970, 0, 0),
      (2, '1970-01-01 00:00:00 EST', 0, 1970, 0, 0),
      (3, '1970-01-01 00:00:00 EST', 0, 1970, 0, 0)
""")

DEATH_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.death`
(person_id, death_date, death_type_concept_id)
VALUES
      (1, '1969-01-01', 0),
      (2, '2020-01-01', 0)
""")

VISIT_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.visit_occurrence`
(visit_occurrence_id, person_id, visit_start_date, visit_end_date, visit_concept_id, visit_type_concept_id)
VALUES
      (1, 1, '2000-01-01', '2000-01-02', 0, 0),
      (2, 1, '2000-01-02', '2000-01-03', 0, 0),
      (3, 2, '2000-01-01', '2020-03-01', 0, 0),
      (4, 3, '2000-01-02', '2000-01-03', 0, 0)
""")

OBSERVATION_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
(observation_id, person_id, observation_date, observation_concept_id, observation_type_concept_id)
VALUES
      (1, 1, '2000-01-01', 0, 0),
      (2, 1, '2000-01-02', 0, 0),
      (3, 2, '2020-03-01', 0, 0),
      (4, 2, '2020-01-05', 0, 0),
      (5, 3, '2020-05-05', 0, 0)
""")

CONDITION_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence` (
(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id)
VALUES
    (1, 1, 80502, "2019-08-20", "2019-08-20 01:00:00 UTC", null, null, 38000245),
    (2, 2, 321661, "2018-09-10", "2018-09-10 01:00:00 UTC", null, null, 38000245),
    (3, 3, 435928, "2017-08-15", "2017-08-15 00:00:00 UTC" , null, null, 38000245),
    (4, 4, 434005, "2018-08-03", "2018-08-03 05:00:00 UTC" , null, null, 32020)
""")

INSERT_DRUG_EXPOSURE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.drug_exposure`
(drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, drug_type_concept_id)
VALUES
    (401, 1, 1126658, '2015-07-15', timestamp('2015-07-15'), 99999),
    (402, 2, 320128, '2015-07-15', timestamp('2015-07-15'), 99999),
    (403, 3, 36676219, '2015-07-15', timestamp('2015-07-15'), 99999),
    (404, 4, 2414345, '2015-07-15', timestamp('2015-07-15'), 99999),
    (405, 5, 45887635, '2015-07-15', timestamp('2015-07-15'), 99999),
    (406, 6, 45077152, '2015-07-15', timestamp('2015-07-15'), 99999)
""")

INSERT_DEVICE_EXPOSURE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.device_exposure`
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, device_exposure_start_datetime, device_type_concept_id)
VALUES
    (501, 1, 4206863, '2015-07-15', timestamp('2015-07-15'), 44818707),
    (502, 2, 320128, '2015-07-15', timestamp('2015-07-15'), 99999),
    (503, 3, 2101931, '2015-07-15', timestamp('2015-07-15'), 99999),
    (504, 4, 740910, '2015-07-15', timestamp('2015-07-15'), 44818707),
    (505, 5, 2106252, '2015-07-15', timestamp('2015-07-15'), 99999),
    (506, 6, 45887635, '2015-07-15', timestamp('2015-07-15'), 99999)
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
        # adding death table name for setup/cleanup operations
        for table_name in get_affected_tables() + ['death']:
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
