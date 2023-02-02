"""
Test for RemediateBasics
"""

# Python imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.manual_cleaning_rules.remediate_basics import RemediateBasics
from common import OBSERVATION
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

        cls.kwargs.update({
            'lookup_dataset_id': cls.lookup_dataset_id,
            'lookup_table_id': cls.lookup_table_id
        })

        cls.rule_instance = RemediateBasics(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
            lookup_dataset_id=cls.lookup_dataset_id,
            lookup_table_id=cls.lookup_table_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}'
        ]

        sb_table_name = cls.rule_instance.sandbox_table_for(OBSERVATION)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}',
            f'{cls.project_id}.{cls.sandbox_id}._observation_id_map'
        ]

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
                 value_source_concept_id, value_source_value, questionnaire_response_id)
            VALUES
                (101, 1, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish', 1001),
                (102, 1, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female', 1001),
                (103, 1, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 903096, 'PMI_Skip', 1001),
                (201, 2, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish', 1002),
                (202, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female', 1002),
                (203, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 903096, 'PMI_Skip', 1002),
                (204, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586143, 'WhatRaceEthnicity_Black', 1002),
                (205, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586144, 'WhatRaceEthnicity_MENA', 1002),
                (301, 3, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish', 1003),
                (302, 3, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female', 1003),
                (303, 3, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 903096, 'PMI_Skip', 1003),
                (304, 3, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586143, 'WhatRaceEthnicity_Black', 1003),
                (305, 3, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586144, 'WhatRaceEthnicity_MENA', 1003)
            """).render(fq_observation=self.fq_table_names[0])

        create_lookup_query = self.jinja_env.from_string("""
            CREATE TABLE `{{fq_lookup_table}}` LIKE `{{fq_observation}}`
            """).render(fq_lookup_table=self.fq_lookup_table_name,
                        fq_observation=self.fq_table_names[0])

        insert_lookup_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_lookup_table}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_datetime, observation_type_concept_id, observation_source_concept_id,
                 value_source_concept_id, value_source_value, questionnaire_response_id)
            VALUES
                (902, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585846, 'SexAtBirth_Male', 1002),
                (903, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 1585839, 'GenderIdentity_Man', 1002),
                (904, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586147, 'WhatRaceEthnicity_Hispanic', 1002),
                (905, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586146, 'WhatRaceEthnicity_White', 1002),
                (906, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586142, 'WhatRaceEthnicity_Asian', 1002)
            """).render(fq_lookup_table=self.fq_lookup_table_name,)

        self.load_test_data(
            [insert_obs_query, create_lookup_query, insert_lookup_query])

    def test_remediate_basics(self):
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
                * 902 - 906 become 306 - 310 respectively after mapping.
            person_id == 3:
                This person has no updated basics records. No change will be made to its records.
        """

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 201, 202, 203, 204, 205, 301, 302, 303, 304, 305
            ],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (201,), (306,), (307,),
                               (308,), (309,), (310,), (301,), (302,), (303,),
                               (304,), (305,)]
        }]

        self.default_test(tables_and_counts)

    def tearDown(self):
        self.client.delete_table(self.fq_lookup_table_name, not_found_ok=True)