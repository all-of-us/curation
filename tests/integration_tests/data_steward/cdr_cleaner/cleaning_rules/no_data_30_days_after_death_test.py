"""
Integration test for NoDataAfterDeath
"""

# Python Imports
import os
import datetime
import pytz
from dateutil import parser

# Project Imports
from common import (AOU_DEATH, PERSON, VISIT_OCCURRENCE, OBSERVATION,
                    CONDITION_OCCURRENCE, DEVICE_EXPOSURE)
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

AOU_DEATH_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
(aou_death_id, person_id, death_date, death_type_concept_id, src_id, primary_death_record)
VALUES
      ('a1', 1, '1969-01-01', 0, 'rdr', False),
      ('b1', 1, '2023-01-01', 0, 'hpo_a', True),
      ('a2', 2, '2020-01-01', 0, 'hpo_b', True),
      ('b2', 2, '2023-01-01', 0, 'hpo_c', False)
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
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
(condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id)
VALUES
    (1, 1, 80502, '1969-08-20', '1968-08-20 00:00:00 UTC', null, null, 38000245),
    (2, 2, 321661, '2020-09-10', '2020-09-10 00:00:00 UTC', '2020-10-10', null, 38000245),
    (3, 3, 435928, '2017-08-15', '2017-08-15 00:00:00 UTC' , null, null, 38000245)
""")

DEVICE_EXPOSURE_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.device_exposure`
(device_exposure_id, person_id, device_concept_id, device_exposure_start_date, device_exposure_start_datetime, device_exposure_end_date, device_type_concept_id)
VALUES
    (1, 1, 4206863, '1969-05-15', TIMESTAMP('2015-07-15'), null, 44818707),
    (2, 2, 320128, '2021-07-15', TIMESTAMP('2015-07-15'), null, 99999),
    (3, 3, 2101931, '2015-07-15', TIMESTAMP('2015-07-15'), null, 99999)
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
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = NoDataAfterDeath(cls.project_id, cls.dataset_id,
                                             cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        # adding aou_death table name for setup/cleanup operations
        for table_name in get_affected_tables() + [AOU_DEATH]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
            sandbox_table_name = cls.rule_instance.sandbox_table_for(table_name)
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create the observation, concept, and concept_relationship tables required for the test
        super().setUp()

        templates = [
            PERSON_DATA_TEMPLATE, AOU_DEATH_DATA_TEMPLATE,
            VISIT_OCCURRENCE_DATA_TEMPLATE, OBSERVATION_DATA_TEMPLATE,
            CONDITION_OCCURRENCE_DATA_TEMPLATE, DEVICE_EXPOSURE_DATA_TEMPLATE
        ]

        test_queries = []
        for template in templates:
            test_queries.append(
                template.render(project_id=self.project_id,
                                dataset_id=self.dataset_id))

        # Load test data
        self.load_test_data(test_queries)

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
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3],
            'sandboxed_ids': [1, 2],
            'fields': [
                'condition_occurrence_id', 'person_id', 'condition_concept_id',
                'condition_start_date', 'condition_start_datetime',
                'condition_end_date', 'condition_end_datetime',
                'condition_type_concept_id'
            ],
            'cleaned_values': [
                (3, 3, 435928,
                 datetime.datetime.strptime('2017-08-15', '%Y-%m-%d').date(),
                 parser.parse('2017-08-15 00:00:00 UTC'), None, None, 38000245)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DEVICE_EXPOSURE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(DEVICE_EXPOSURE)}',
            'loaded_ids': [1, 2, 3],
            'sandboxed_ids': [1, 2],
            'fields': [
                'device_exposure_id', 'person_id', 'device_concept_id',
                'device_exposure_start_date', 'device_exposure_start_datetime',
                'device_exposure_end_date', 'device_type_concept_id'
            ],
            'cleaned_values': [
                (3, 3, 2101931,
                 datetime.datetime.strptime('2015-07-15', '%Y-%m-%d').date(),
                 parser.parse('2015-07-15 00:00:00 UTC'), None, 99999)
            ]
        }]

        self.default_test(tables_and_counts)
