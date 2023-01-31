"""
Test for RemediateBasics
"""

# Python imports
import os
from unittest import mock

# Third party imports
import pytz
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.manual_cleaning_rules.remediate_basics import RemediateBasics
from common import DEID_MAP, OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RemediateBasicsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.lookup_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.lookup_table_id = f'lookup_{OBSERVATION}'
        cls.deid_map_dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.deid_map_table_id = DEID_MAP

        cls.kwargs.update({
            'lookup_dataset_id': cls.lookup_dataset_id,
            'lookup_table_id': cls.lookup_table_id,
            'deid_map_dataset_id': cls.deid_map_dataset_id,
            'deid_map_table_id': cls.deid_map_table_id
        })

        cls.rule_instance = RemediateBasics(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
            lookup_dataset_id=cls.lookup_dataset_id,
            lookup_table_id=cls.lookup_table_id,
            deid_map_dataset_id=cls.deid_map_dataset_id,
            deid_map_table_id=cls.deid_map_table_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{cls.project_id}.{cls.deid_map_dataset_id}.{cls.deid_map_table_id}'
        ]

        sb_table_name = cls.rule_instance.sandbox_table_for(OBSERVATION)

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}')

        cls.fq_lookup_table_name = f'{cls.project_id}.{cls.lookup_dataset_id}.{cls.lookup_table_id}'

        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """

        super().setUp()

        insert_obs_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_observation}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_datetime, observation_type_concept_id, observation_source_concept_id,
                 value_source_concept_id, value_source_value)
            VALUES
                (101, 1, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish'),
                (102, 1, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female'),
                (103, 1, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 903096, 'PMI_Skip'),
                (201, 2, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish'),
                (202, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female'),
                (203, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 903096, 'PMI_Skip'),
                (204, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586143, 'WhatRaceEthnicity_Black'),
                (205, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586144, 'WhatRaceEthnicity_MENA'),
                (301, 3, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish'),
                (302, 3, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female'),
                (303, 3, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 903096, 'PMI_Skip'),
                (304, 3, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586143, 'WhatRaceEthnicity_Black'),
                (305, 3, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586144, 'WhatRaceEthnicity_MENA')
            """).render(fq_observation=self.fq_table_names[0])

        create_lookup_query = self.jinja_env.from_string("""
            CREATE TABLE `{{fq_lookup_table}}` LIKE `{{fq_observation}}`
            """).render(fq_lookup_table=self.fq_lookup_table_name,
                        fq_observation=self.fq_table_names[0])

        insert_lookup_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_lookup_table}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_datetime, observation_type_concept_id, observation_source_concept_id,
                 value_source_concept_id, value_source_value)
            VALUES
                (902, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585846, 'SexAtBirth_Male'),
                (903, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 1585839, 'GenderIdentity_Man'),
                (904, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586147, 'WhatRaceEthnicity_Hispanic'),
                (905, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586146, 'WhatRaceEthnicity_White'),
                (906, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586142, 'WhatRaceEthnicity_Asian')
            """).render(fq_lookup_table=self.fq_lookup_table_name,)

        insert_deid_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_deid_table}}`
                (person_id, research_id, shift)
            VALUES
                (1, 9, 100),
                (2, 3, 200)
            """).render(fq_deid_table=self.fq_table_names[1],)

        self.load_test_data([
            insert_obs_query, create_lookup_query, insert_lookup_query,
            insert_deid_query
        ])

        self.unshifted_date = parser.parse('2022-01-01').date()
        self.unshifted_datetime = pytz.utc.localize(
            parser.parse('2022-01-01 12:34:56'))
        self.shifted_date = parser.parse('2021-06-15').date()
        self.shifted_datetime = pytz.utc.localize(
            parser.parse('2021-06-15 12:34:56'))

    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_rt_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_ct_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_combined_dataset'
    )
    def test_remediate_basics_combined(self, mock_is_combined, mock_is_deid,
                                       mock_is_ct, mock_is_rt):
        """Test to ensure RemediateBasics is working as expected for COMBINED dataset.
        Test cases per person_id and observation_id:
            person_id == 1:
                This person has no updated basics records. No change will be made to its records.
            person_id == 2:
                This person has updated basics records.
                201 ... There is no corresponding records in the lookup table. It stays.
                202 ... It has corresponding record (=902). 202 gets dropped and 902 gets inserted.
                203 ... Similar to 202, the same behavior even when the source value is 'PMI_Skip'.
                204, 205 ... Those have multiple corresponding records (=904,905,906).
                             204 and 205 get dropped and 904, 905, and 906 get inserted.
            person_id == 3:
                This person has no updated basics records. No change will be made to its records.
        """

        mock_is_combined.return_value, mock_is_deid.return_value, mock_is_ct.return_value, mock_is_rt.return_value = True, False, False, False

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 201, 202, 203, 204, 205, 301, 302, 303, 304, 305
            ],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': [
                'observation_id', 'observation_date', 'observation_datetime'
            ],
            'cleaned_values': [
                (101, self.unshifted_date, self.unshifted_datetime),
                (102, self.unshifted_date, self.unshifted_datetime),
                (103, self.unshifted_date, self.unshifted_datetime),
                (201, self.unshifted_date, self.unshifted_datetime),
                (902, self.unshifted_date, self.unshifted_datetime),
                (903, self.unshifted_date, self.unshifted_datetime),
                (904, self.unshifted_date, self.unshifted_datetime),
                (905, self.unshifted_date, self.unshifted_datetime),
                (906, self.unshifted_date, self.unshifted_datetime),
                (301, self.unshifted_date, self.unshifted_datetime),
                (302, self.unshifted_date, self.unshifted_datetime),
                (303, self.unshifted_date, self.unshifted_datetime),
                (304, self.unshifted_date, self.unshifted_datetime),
                (305, self.unshifted_date, self.unshifted_datetime)
            ]
        }]

        self.default_test(tables_and_counts)

    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_rt_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_ct_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_combined_dataset'
    )
    def test_remediate_basics_ct(self, mock_is_combined, mock_is_deid,
                                 mock_is_ct, mock_is_rt):
        """Test to ensure RemediateBasics is working as expected for CONTROLLED TIER dataset.
        Test cases per research_id and observation_id:
            research_id == 1:
                This person has no updated basics records. No change will be made to its records.
            research_id == 2:
                This person has no updated basics records. No change will be made to its records.
            research_id == 3:
                This person has updated basics records.
                301 ... There is no corresponding records in the lookup table. It stays.
                302 ... It has corresponding record (=902). 302 gets dropped and 902 gets inserted.
                303 ... Similar to 302, the same behavior even when the source value is 'PMI_Skip'.
                304, 305 ... Those have multiple corresponding records (=904,905,906).
                             304 and 305 get dropped and 904, 905, and 906 get inserted.
        """
        mock_is_combined.return_value, mock_is_deid.return_value, mock_is_ct.return_value, mock_is_rt.return_value = False, True, True, False

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 201, 202, 203, 204, 205, 301, 302, 303, 304, 305
            ],
            'sandboxed_ids': [302, 303, 304, 305],
            'fields': [
                'observation_id', 'observation_date', 'observation_datetime'
            ],
            'cleaned_values': [
                (101, self.unshifted_date, self.unshifted_datetime),
                (102, self.unshifted_date, self.unshifted_datetime),
                (103, self.unshifted_date, self.unshifted_datetime),
                (201, self.unshifted_date, self.unshifted_datetime),
                (202, self.unshifted_date, self.unshifted_datetime),
                (203, self.unshifted_date, self.unshifted_datetime),
                (204, self.unshifted_date, self.unshifted_datetime),
                (205, self.unshifted_date, self.unshifted_datetime),
                (301, self.unshifted_date, self.unshifted_datetime),
                (902, self.unshifted_date, self.unshifted_datetime),
                (903, self.unshifted_date, self.unshifted_datetime),
                (904, self.unshifted_date, self.unshifted_datetime),
                (905, self.unshifted_date, self.unshifted_datetime),
                (906, self.unshifted_date, self.unshifted_datetime)
            ]
        }]

        self.default_test(tables_and_counts)

    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_rt_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_ct_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_combined_dataset'
    )
    def test_remediate_basics_rt(self, mock_is_combined, mock_is_deid,
                                 mock_is_ct, mock_is_rt):
        """Test to ensure RemediateBasics is working as expected for REGISTERED TIER dataset.
        Test cases per research_id and observation_id:
            research_id == 1:
                This person has no updated basics records. No change will be made to its records.
            research_id == 2:
                This person has no updated basics records. No change will be made to its records.
            research_id == 3:
                This person has updated basics records.
                301 ... There is no corresponding records in the lookup table. It stays.
                302 ... It has corresponding record (=902). 302 gets dropped and 902 gets inserted.
                303 ... Similar to 302, the same behavior even when the source value is 'PMI_Skip'.
                304, 305 ... Those have multiple corresponding records (=904,905,906).
                             304 and 305 get dropped and 904, 905, and 906 get inserted.
        """
        mock_is_combined.return_value, mock_is_deid.return_value, mock_is_ct.return_value, mock_is_rt.return_value = False, True, False, True

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 201, 202, 203, 204, 205, 301, 302, 303, 304, 305
            ],
            'sandboxed_ids': [302, 303, 304, 305],
            'fields': [
                'observation_id', 'observation_date', 'observation_datetime'
            ],
            'cleaned_values': [
                (101, self.unshifted_date, self.unshifted_datetime),
                (102, self.unshifted_date, self.unshifted_datetime),
                (103, self.unshifted_date, self.unshifted_datetime),
                (201, self.unshifted_date, self.unshifted_datetime),
                (202, self.unshifted_date, self.unshifted_datetime),
                (203, self.unshifted_date, self.unshifted_datetime),
                (204, self.unshifted_date, self.unshifted_datetime),
                (205, self.unshifted_date, self.unshifted_datetime),
                (301, self.unshifted_date, self.unshifted_datetime),
                (902, self.shifted_date, self.shifted_datetime),
                (903, self.shifted_date, self.shifted_datetime),
                (904, self.shifted_date, self.shifted_datetime),
                (905, self.shifted_date, self.shifted_datetime),
                (906, self.shifted_date, self.shifted_datetime)
            ]
        }]

        self.default_test(tables_and_counts)

    def tearDown(self):
        self.client.delete_table(self.fq_lookup_table_name, not_found_ok=True)
        super().tearDown()