"""
Test for RemediateBasics
"""

# Python imports
import os
from unittest import mock

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.manual_cleaning_rules.remediate_basics import (
    RemediateBasics, NEW_OBS_ID_LOOKUP, OBS_EXT, OBS_MAPPING, PERS_EXT, SC_EXT,
    SC_MAPPING)
from common import OBSERVATION, PERSON, SURVEY_CONDUCT
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
        cls.incremental_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')

        cls.kwargs.update(
            {'incremental_dataset_id': cls.incremental_dataset_id})

        cls.rule_instance = RemediateBasics(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
            incremental_dataset_id=cls.incremental_dataset_id)

        for dataset in [cls.dataset_id, cls.incremental_dataset_id]:
            cls.fq_table_names.extend([
                f'{cls.project_id}.{dataset}.{OBSERVATION}',
                f'{cls.project_id}.{dataset}.{OBS_MAPPING}',
                f'{cls.project_id}.{dataset}.{OBS_EXT}',
                f'{cls.project_id}.{dataset}.{SURVEY_CONDUCT}',
                f'{cls.project_id}.{dataset}.{SC_MAPPING}',
                f'{cls.project_id}.{dataset}.{SC_EXT}',
                f'{cls.project_id}.{dataset}.{PERSON}',
                f'{cls.project_id}.{dataset}.{PERS_EXT}'
            ])

        cls.sb_obs = cls.rule_instance.sandbox_table_for(OBSERVATION)
        cls.sb_obs_mapping = cls.rule_instance.sandbox_table_for(OBS_MAPPING)
        cls.sb_obs_ext = cls.rule_instance.sandbox_table_for(OBS_EXT)
        cls.sb_sc = cls.rule_instance.sandbox_table_for(SURVEY_CONDUCT)
        cls.sb_sc_mapping = cls.rule_instance.sandbox_table_for(SC_MAPPING)
        cls.sb_sc_ext = cls.rule_instance.sandbox_table_for(SC_EXT)
        cls.sb_pers = cls.rule_instance.sandbox_table_for(PERSON)
        cls.sb_pers_ext = cls.rule_instance.sandbox_table_for(PERS_EXT)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_obs}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_obs_mapping}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_obs_ext}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_sc}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_sc_mapping}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_sc_ext}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_pers}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.sb_pers_ext}',
            f'{cls.project_id}.{cls.sandbox_id}.{NEW_OBS_ID_LOOKUP}'
        ]

        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """

        super().setUp()

        insert_obs = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_datetime, observation_type_concept_id, observation_source_concept_id,
                 value_source_concept_id, value_source_value, questionnaire_response_id)
            VALUES
                (101, 1, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish', 1001),
                (102, 1, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female', 1001),
                (103, 1, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 903096, 'PMI_Skip', 1001),
                (104, 1, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 1585839, 'GenderIdentity_Man', 1001),
                (201, 2, 1586155, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586155, 1585336, 'WhiteSpecific_Spanish', 1002),
                (202, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585847, 'SexAtBirth_Female', 1002),
                (203, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 903096, 'PMI_Skip', 1002),
                (204, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586143, 'WhatRaceEthnicity_Black', 1002),
                (205, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586144, 'WhatRaceEthnicity_MENA', 1002)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        obs=OBSERVATION)

        insert_incremental_obs = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{obs}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_datetime, observation_type_concept_id, observation_source_concept_id,
                 value_source_concept_id, value_source_value, questionnaire_response_id)
            VALUES
                (902, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585846, 'SexAtBirth_Male', 1002),
                (903, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 1585839, 'GenderIdentity_Man', 1002),
                (904, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586147, 'WhatRaceEthnicity_Hispanic', 1002),
                (905, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586146, 'WhatRaceEthnicity_White', 1002),
                (906, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586142, 'WhatRaceEthnicity_Asian', 1002)
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        obs=OBSERVATION)

        insert_obs_mapping = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs_mapping}}`
                (observation_id, src_dataset_id, src_observation_id, src_hpo_id, src_table_id)
            VALUES
                (101, 'dummy_rdr_1', 11, 'rdr', 'observation'),
                (102, 'dummy_rdr_1', 12, 'rdr', 'observation'),
                (103, 'dummy_rdr_1', 13, 'rdr', 'observation'),
                (104, 'dummy_rdr_1', 14, 'rdr', 'observation'),
                (201, 'dummy_rdr_1', 21, 'rdr', 'observation'),
                (202, 'dummy_rdr_1', 22, 'rdr', 'observation'),
                (203, 'dummy_rdr_1', 23, 'rdr', 'observation'),
                (204, 'dummy_rdr_1', 24, 'rdr', 'observation'),
                (205, 'dummy_rdr_1', 25, 'rdr', 'observation')
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        obs_mapping=OBS_MAPPING)

        insert_incremental_obs_mapping = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{obs_mapping}}`
                (observation_id, src_dataset_id, src_observation_id, src_hpo_id, src_table_id)
            VALUES
                (901, 'dummy_rdr_2', 91, 'rdr', 'observation'),
                (902, 'dummy_rdr_2', 92, 'rdr', 'observation'),
                (903, 'dummy_rdr_2', 93, 'rdr', 'observation'),
                (904, 'dummy_rdr_2', 94, 'rdr', 'observation'),
                (905, 'dummy_rdr_2', 95, 'rdr', 'observation'),
                (906, 'dummy_rdr_2', 96, 'rdr', 'observation')
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        obs_mapping=OBS_MAPPING)

        insert_obs_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs_ext}}`
                (observation_id, src_id, survey_version_concept_id)
            VALUES
                (101, 'PPI/PM', NULL),
                (102, 'PPI/PM', NULL),
                (103, 'PPI/PM', NULL),
                (104, 'PPI/PM', NULL),
                (201, 'PPI/PM', NULL),
                (202, 'PPI/PM', NULL),
                (203, 'PPI/PM', NULL),
                (204, 'PPI/PM', NULL),
                (205, 'PPI/PM', NULL)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        obs_ext=OBS_EXT)

        insert_incremental_obs_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{obs_ext}}`
                (observation_id, src_id, survey_version_concept_id)
            VALUES
                (901, 'PPI/PM', NULL),
                (902, 'PPI/PM', NULL),
                (903, 'PPI/PM', NULL),
                (904, 'PPI/PM', NULL),
                (905, 'PPI/PM', NULL),
                (906, 'PPI/PM', NULL)
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        obs_ext=OBS_EXT)

        insert_sc = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{sc}}`
                (survey_conduct_id, person_id, survey_concept_id, survey_end_datetime,
                 assisted_concept_id, respondent_type_concept_id, timing_concept_id,
                 collection_method_concept_id, survey_source_concept_id,
                 validated_survey_concept_id)
            VALUES
                (1001, 1, 1586134, timestamp('2022-01-01 12:34:56'), 0, 0, 0, 42531021, 1586134, 0),
                (1002, 2, 1586134, timestamp('2022-01-01 12:34:56'), 0, 0, 0, 42531021, 1586134, 0)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        sc=SURVEY_CONDUCT)

        insert_incremental_sc = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{sc}}`
                (survey_conduct_id, person_id, survey_concept_id, survey_end_datetime,
                 assisted_concept_id, respondent_type_concept_id, timing_concept_id,
                 collection_method_concept_id, survey_source_concept_id,
                 validated_survey_concept_id)
            VALUES
                (1002, 2, 1586134, timestamp('2022-01-01 12:34:56'), 0, 0, 0, 42531021, 1586134, 99999)
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        sc=SURVEY_CONDUCT)

        insert_sc_mapping = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{sc_mapping}}`
                (survey_conduct_id, src_dataset_id, src_survey_conduct_id, src_hpo_id, src_table_id)
            VALUES
                (1001, 'dummy_rdr_1', 1001, 'rdr', 'survey_conduct'),
                (1002, 'dummy_rdr_1', 1002, 'rdr', 'survey_conduct')
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        sc_mapping=SC_MAPPING)

        insert_incremental_sc_mapping = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{sc_mapping}}`
                (survey_conduct_id, src_dataset_id, src_survey_conduct_id, src_hpo_id, src_table_id)
            VALUES
                (1002, 'dummy_rdr_2', 1002, 'rdr', 'survey_conduct')
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        sc_mapping=SC_MAPPING)

        insert_sc_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{sc_ext}}`
                (survey_conduct_id, src_id, language)
            VALUES
                (1001, 'PPI/PM', 'en'),
                (1002, 'PPI/PM', 'en')
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        sc_ext=SC_EXT)

        insert_incremental_sc_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{sc_ext}}`
                (survey_conduct_id, src_id, language)
            VALUES
                (1002, 'PPI/PM', 'es')
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        sc_ext=SC_EXT)

        insert_pers = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{pers}}`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES
                (1, 1585839, 1990, 1585336, 38003563),
                (2, 903096, 1995, 1585336, 38003563)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        pers=PERSON)

        insert_incremental_pers = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{pers}}`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES
                (2, 1585839, 1995, 1585841, 38003563)
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        pers=PERSON)

        insert_pers_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{pers_ext}}`
                (person_id, sex_at_birth_source_concept_id)
            VALUES
                (1, 1585847),
                (2, 1585847)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        pers_ext=PERS_EXT)

        insert_incremental_pers_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{pers_ext}}`
                (person_id, sex_at_birth_source_concept_id)
            VALUES
                (2, 1585846)
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        pers_ext=PERS_EXT)

        self.load_test_data([
            insert_obs, insert_incremental_obs, insert_obs_mapping,
            insert_incremental_obs_mapping, insert_obs_ext,
            insert_incremental_obs_ext, insert_sc, insert_incremental_sc,
            insert_sc_mapping, insert_incremental_sc_mapping, insert_sc_ext,
            insert_incremental_sc_ext, insert_pers, insert_incremental_pers,
            insert_pers_ext, insert_incremental_pers_ext
        ])

    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_combined_dataset'
    )
    def test_remediate_basics_combined(self, mock_is_combined, mock_is_deid):
        """Test to ensure RemediateBasics works as expected for COMBINED dataset.
        [1] OBSERVATION and its ext/mapping tables
            person_id == 1:
                This person has no updated basics records. No change will be made to its records.
            person_id == 2:
                This person has updated basics records.
                201 ... There is no corresponding records in the incremental observation table. It stays.
                202 ... It has corresponding record (=902). 202 gets dropped and 902 gets inserted.
                203 ... Similar to 202, the same behavior even when the source value is 'PMI_Skip'.
                204, 205 ... Those have multiple corresponding records (=904,905,906).
                             204 and 205 get dropped and 904, 905, and 906 get inserted.
                * 902 - 906 become 206 - 210 respectively after re-mapping using NEW_OBS_ID_LOOKUP.

        [2] SURVEY_CONDUCT and its ext/mapping tables
            survey_conduct_id = 1002 gets sandboxed and updated since it exists in the incremental dataset.

        [3] PERSON table
            person_id = 2 gets sandboxed and updated since it exists in the incremental dataset.

        [4] PERSON_EXT table
            This table does not exist in combined dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_combined.return_value, mock_is_deid.return_value = True, False

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (206,),
                               (207,), (208,), (209,), (210,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_MAPPING}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_mapping}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id', 'src_dataset_id'],
            'cleaned_values': [(101, 'dummy_rdr_1'), (102, 'dummy_rdr_1'),
                               (103, 'dummy_rdr_1'), (104, 'dummy_rdr_1'),
                               (201, 'dummy_rdr_1'), (206, 'dummy_rdr_2'),
                               (207, 'dummy_rdr_2'), (208, 'dummy_rdr_2'),
                               (209, 'dummy_rdr_2'), (210, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (206,),
                               (207,), (208,), (209,), (210,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 99999)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_MAPPING}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'src_dataset_id'],
            'cleaned_values': [(1001, 'dummy_rdr_1'), (1002, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'language'],
            'cleaned_values': [(1001, 'en'), (1002, 'es')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_pers}',
            'loaded_ids': [1, 2],
            'sandboxed_ids': [2],
            'fields': ['person_id', 'gender_concept_id'],
            'cleaned_values': [(1, 1585839), (2, 1585839)]
        }]

        self.default_test(tables_and_counts)

        # Sandbox for PERSON_EXT does not exist because it's combined dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}')

    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_dataset')
    @mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_combined_dataset'
    )
    def test_remediate_basics_deid(self, mock_is_combined, mock_is_deid):
        """Test to ensure RemediateBasics works as expected for DEID (= CT and RT) dataset.
        [1] OBSERVATION and its ext table
            Same result as test_remediate_basics_combined

        [2] SURVEY_CONDUCT and its ext table
            Same result as test_remediate_basics_combined

        [3] PERSON and its ext table
            person_id = 2 gets sandboxed and updated since it exists in the incremental dataset.

        [4] OBSERVATION_MAPPING and SURVEY_CONDUCT_MAPPING tables
            These tables do not exist in combined dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_combined.return_value, mock_is_deid.return_value = False, True

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (206,),
                               (207,), (208,), (209,), (210,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (206,),
                               (207,), (208,), (209,), (210,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 99999)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'language'],
            'cleaned_values': [(1001, 'en'), (1002, 'es')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_pers}',
            'loaded_ids': [1, 2],
            'sandboxed_ids': [2],
            'fields': ['person_id', 'gender_concept_id'],
            'cleaned_values': [(1, 1585839), (2, 1585839)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}',
            'loaded_ids': [1, 2],
            'sandboxed_ids': [2],
            'fields': ['person_id', 'sex_at_birth_source_concept_id'],
            'cleaned_values': [(1, 1585847), (2, 1585846)]
        }]

        self.default_test(tables_and_counts)

        # Sandbox for OBS_MAP and SC_MAP do not exist because it's deid dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_mapping}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}')
