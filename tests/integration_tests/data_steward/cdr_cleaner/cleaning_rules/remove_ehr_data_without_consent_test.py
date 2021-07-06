"""
Integration test for remove_ehr_data_without_consent module

Original Issues: DC-1644

The intent is to remove all ehr data for unconsented participants for EHR.

"""

# Python Imports
import os
from dateutil import parser

# Project Imports
from common import PERSON, VISIT_OCCURRENCE, OBSERVATION
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_ehr_data_without_consent import (
    RemoveEhrDataWithoutConsent, EHR_UNCONSENTED_PARTICIPANTS_LOOKUP_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}.person` (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
      VALUES (1, 0, 0, 0, 0),
      (2, 0, 0, 0, 0)
""")

VISIT_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}.visit_occurrence` 
(visit_occurrence_id,
 person_id,
 visit_concept_id,
 visit_start_date,
 visit_end_date,
 visit_type_concept_id)
      VALUES (1, 1, 0, '2020-01-01', '2020-01-01', 0),
       (2, 1, 0, '2020-01-01', '2020-01-01', 0),
       (3, 1, 0, '2020-01-01', '2020-01-01', 0),
       (4, 2, 0, '2020-01-01', '2020-01-01', 0),
       (5, 2, 0, '2020-01-01', '2020-01-01', 0),
       (6, 2, 0, '2020-01-01', '2020-01-01', 0)
""")

MAPPING_VISIT_OCCURRENCE_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}._mapping_visit_occurrence`
(visit_occurrence_id, src_dataset_id)
      VALUES (1, 'rdr2021'),
       (2, 'rdr2021'),
       (3, 'unioned_ehr'),
       (4, 'unioned_ehr'),
       (5, 'unioned_ehr'),
       (6, 'rdr2021')
""")

OBSERVATION_DATA_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}.observation`
(observation_id,
 person_id,
 observation_concept_id,
 observation_date,
 observation_datetime,
 observation_type_concept_id,
 value_source_concept_id,
 observation_source_value )
VALUES(1, 1, 0, '2020-01-01', '2020-01-01 00:00:00 UTC', 0, 1586100, 'EHRConsentPII_ConsentPermission'),
       (2, 1, 0, '2021-01-02', '2021-01-02 00:00:00 UTC', 0,  1586100, 'EHRConsentPII_ConsentPermission'),
       (3, 1, 0, '2020-05-01', '2020-05-01 00:00:00 UTC', 0, 123, 'test_value_0'),
       (4, 2, 0, '2020-03-01', '2020-03-01 00:00:00 UTC', 0, 234, 'test_value_1'),
       (5, 2, 0, '2020-01-05', '2020-01-05 00:00:00 UTC', 0,  345, 'test_value_2'),
       (6, 2, 0, '2020-05-05', '2020-05-05 00:00:00 UTC', 0, 456, 'test_value_3')
""")

MAPPING_OBSERVATION_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}._mapping_observation`
(observation_id, src_dataset_id)
VALUES(1, 'rdr2021'),
       (2, 'rdr2021'),
       (3, 'unioned_ehr'),
       (4, 'unioned_ehr'),
       (5, 'unioned_ehr'),
       (6, 'rdr2021')
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

        cls.rule_instance = RemoveEhrDataWithoutConsent(cls.project_id,
                                                        cls.dataset_id,
                                                        cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.extend([
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.{PERSON}',
            f'{cls.project_id}.{cls.dataset_id}.{VISIT_OCCURRENCE}',
            f'{cls.project_id}.{cls.dataset_id}._mapping_{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}._mapping_{VISIT_OCCURRENCE}',
        ])
        cls.fq_sandbox_table_names.extend([
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.issue_numbers[0].lower()}_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.issue_numbers[0].lower()}_{VISIT_OCCURRENCE}',
            f'{cls.project_id}.{cls.sandbox_id}.{EHR_UNCONSENTED_PARTICIPANTS_LOOKUP_TABLE}'
        ])

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create the person, observation, _mapping_observation, visit_occurrence, _mapping_visit_occurrence
        # tables required for the test
        super().setUp()

        person_data_query = PERSON_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        visit_occurrence_data_query = VISIT_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        observation_data_query = OBSERVATION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        mapping_observation_query = MAPPING_OBSERVATION_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        mapping_visit_query = MAPPING_VISIT_OCCURRENCE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            person_data_query, visit_occurrence_data_query,
            observation_data_query, mapping_observation_query,
            mapping_visit_query
        ])

    def test_remove_ehr_data_without_consent(self):
        """
        1. person_id=1, has valid consent status

        2. person_id=2, does not have valid consent record
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{VISIT_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(VISIT_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [4, 5],
            'fields': ['visit_occurrence_id', 'person_id'],
            'cleaned_values': [(1, 1), (2, 1), (3, 1), (6, 2)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [4, 5],
            'fields': [
                'observation_id', 'person_id', 'value_source_concept_id',
                'observation_source_value'
            ],
            'cleaned_values': [
                (1, 1, 1586100, 'EHRConsentPII_ConsentPermission'),
                (2, 1, 1586100, 'EHRConsentPII_ConsentPermission'),
                (3, 1, 123, 'test_value_0'), (6, 2, 456, 'test_value_3')
            ]
        }]

        self.default_test(tables_and_counts)
