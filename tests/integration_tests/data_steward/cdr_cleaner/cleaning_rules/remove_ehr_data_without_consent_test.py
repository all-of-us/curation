"""
Integration test for remove_ehr_data_without_consent module

Original Issues: DC-1644

The intent is to remove all ehr data for unconsented participants for EHR.

"""

# Python Imports
import os

# Project Imports
from common import PERSON, VISIT_OCCURRENCE, OBSERVATION, EHR_CONSENT_VALIDATION
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_ehr_data_without_consent import (
    RemoveEhrDataWithoutConsent, EHR_UNCONSENTED_PARTICIPANTS_LOOKUP_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}.person` (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
      VALUES (1, 0, 0, 0, 0),
      (2, 0, 0, 0, 0),
      (3, 0, 0, 0, 0),
      (4, 0, 0, 0, 0)
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
       (6, 2, 0, '2020-01-01', '2020-01-01', 0),
       (7, 3, 0, '2020-01-01', '2020-01-01', 0),
       (8, 4, 0, '2020-01-01', '2020-01-01', 0)
""")

MAPPING_VISIT_OCCURRENCE_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}._mapping_visit_occurrence`
(visit_occurrence_id, src_dataset_id)
      VALUES (1, 'rdr2021'),
       (2, 'rdr2021'),
       (3, 'unioned_ehr'),
       (4, 'unioned_ehr'),
       (5, 'unioned_ehr'),
       (6, 'rdr2021'),
       (7, 'unioned_ehr'),
       (8, 'unioned_ehr')
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
VALUES (1, 1, 0, '2020-01-01', '2020-01-01 00:00:00 UTC', 0, 1586100, 'EHRConsentPII_ConsentPermission'),
       (2, 1, 0, '2021-01-02', '2021-01-02 00:00:00 UTC', 0, 1586100, 'EHRConsentPII_ConsentPermission'),
       (3, 1, 0, '2020-05-01', '2020-05-01 00:00:00 UTC', 0, 123, 'test_value_0'),
       (4, 2, 0, '2020-03-01', '2020-03-01 00:00:00 UTC', 0, 234, 'test_value_1'),
       (5, 2, 0, '2020-01-05', '2020-01-05 00:00:00 UTC', 0, 345, 'test_value_2'),
       (6, 2, 0, '2020-05-05', '2020-05-05 00:00:00 UTC', 0, 456, 'test_value_3'),
       (7, 3, 0, '2020-01-01', '2020-01-01 00:00:00 UTC', 0, 1586100, 'EHRConsentPII_ConsentPermission'),
       (8, 4, 0, '2021-01-02', '2021-01-02 00:00:00 UTC', 0, 1586100, 'EHRConsentPII_ConsentPermission')
""")

MAPPING_OBSERVATION_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}._mapping_observation`
(observation_id, src_dataset_id)
VALUES (1, 'rdr2021'),
       (2, 'rdr2021'),
       (3, 'unioned_ehr'),
       (4, 'unioned_ehr'),
       (5, 'unioned_ehr'),
       (6, 'rdr2021'),
       (7, 'unioned_ehr'),
       (8, 'unioned_ehr')
""")

DUPLICATE_RECORDS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{duplicates_dataset}}.{{duplicates_table}}`
(person_id) VALUES (5)
""")

CONSENT_VALIDATION_TEMPLATE = JINJA_ENV.from_string("""
insert into `{{project_id}}.{{dataset_id}}.consent_validation`
(person_id, research_id, consent_for_electronic_health_records, consent_for_electronic_health_records_authored, src_id)
VALUES
     -- validated consent with varying casing, not cleaned --
       (1, 0, 'Submitted', (DATETIME '2018-11-26 00:00:00'), 'rdr'),
     -- validated consent but no consent record in observation, cleaned --
       (2, 0, 'Submitted', (DATETIME '2018-11-26 00:00:00'), 'rdr'),
     -- multiple validation records with one valid('submitted'). invalid consent, cleaned --
       (3, 0, 'Submitted_No', (DATETIME '2018-11-26 00:00:00'), 'rdr'),
       (3, 0, 'Submitted', (DATETIME '2018-11-26 00:00:00'), 'rdr'),
     -- null status. invalid consent, cleaned --
       (4, 0, NULL, (DATETIME '2018-11-26 00:00:00'), 'rdr')
     -- duplicated record --
       (5, 0, 'Submitted', (DATETIME '2018-11-26 00:00:00'), 'rdr'),
       (5, 0, 'Submitted', (DATETIME '2018-11-26 00:00:00'), 'rdr'),)
""")

class RemoveEhrDataWithoutConsentTest(BaseTest.CleaningRulesTestBase):

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
        cls.duplicates_dataset = 'duplicates_dataset'
        cls.duplicates_table = 'duplicates_report'

        cls.rule_instance = RemoveEhrDataWithoutConsent(cls.project_id,
                                                        cls.dataset_id,
                                                        cls.sandbox_id,
                                                        cls.duplicates_dataset,
                                                        cls.duplicates_table)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.extend([
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.{PERSON}',
            f'{cls.project_id}.{cls.dataset_id}.{VISIT_OCCURRENCE}',
            f'{cls.project_id}.{cls.dataset_id}._mapping_{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}._mapping_{VISIT_OCCURRENCE}',
            f'{cls.project_id}.{cls.dataset_id}.{EHR_CONSENT_VALIDATION}',
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
        consent_validation_query = CONSENT_VALIDATION_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        duplicates_data_query = DUPLICATE_RECORDS_TEMPLATE.render(
            project_id=self.project_id, duplicates_dataset=self.duplicates_dataset, duplicates_table=self.duplicates_table)
        )

        # Load test data
        self.load_test_data([
            person_data_query, visit_occurrence_data_query,
            observation_data_query, mapping_observation_query,
            mapping_visit_query, consent_validation_query,
            duplicates_data_query
        ])

    def test_remove_ehr_data_without_consent(self):
        """
        1. person_id=1, has a validated, affirmative consent record.

        2. person_id=2, does not have an affirmative consent record.

        3. person_id=3. has a invalid, affirmative consent record.

        4. person_id=4. has a invalid(null status), affirmative consent record.

        5. person_id=5. has a duplicated record of person_id=6
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{VISIT_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(VISIT_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [4, 5, 7, 8],
            'fields': ['visit_occurrence_id', 'person_id'],
            'cleaned_values': [(1, 1), (2, 1), (3, 1), (6, 2)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8],
            'sandboxed_ids': [4, 5, 7, 8],
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
