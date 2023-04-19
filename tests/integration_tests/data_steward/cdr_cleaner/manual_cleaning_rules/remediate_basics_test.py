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


def mock_patch_decorator_bundle(*decorators):
    """Since the test env's dataset names are different from the prod env's,
    is_xyz_dataset() do not work as designed. So, they need to be patched for
    the tests. This function bundles all the patches into one so we only need
    to write one mock patch decorator for each test.
    """

    def _chain(patch):
        for dec in reversed(decorators):
            patch = dec(patch)
        return patch

    return _chain


mock_patch_bundle = mock_patch_decorator_bundle(
    mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_release_dataset'
    ),
    mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_deid_dataset'),
    mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_combined_release_dataset'
    ),
    mock.patch(
        'cdr_cleaner.manual_cleaning_rules.remediate_basics.is_rdr_dataset'))


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
        cls.dataset_with_largest_observation_id = cls.dataset_id
        cls.obs_id_lookup_dataset = cls.sandbox_id
        cls.exclude_lookup_dataset = cls.sandbox_id
        cls.exclude_lookup_table = 'aian_participant'

        cls.kwargs.update(
            {'incremental_dataset_id': cls.incremental_dataset_id})
        cls.kwargs.update({
            'dataset_with_largest_observation_id':
                cls.dataset_with_largest_observation_id
        })
        cls.kwargs.update({'obs_id_lookup_dataset': cls.obs_id_lookup_dataset})
        cls.kwargs.update(
            {'exclude_lookup_dataset': cls.exclude_lookup_dataset})
        cls.kwargs.update({'exclude_lookup_table': cls.exclude_lookup_table})

        cls.rule_instance = RemediateBasics(cls.project_id, cls.dataset_id,
                                            cls.sandbox_id)

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

        cls.fq_exclusion_table = f'{cls.project_id}.{cls.exclude_lookup_dataset}.{cls.exclude_lookup_table}'

        super().setUpClass()

    def setUp(self):
        """
        Create test tables for the rule to run on.

        Test cases:
        person_id==1: Exists in the original dataset but not in the incremental dataset. No change to this participant's records.
        person_id==2: Exists both in the original dataset but in the incremental dataset. Records will be updated.
        person_id==3: Does not exist in the original dataset but exists in the incremental dataset. Records will be ignored.
        person_id==9: Same as 3, but listed in the exlude_lookup_table. Must be ignored when exlude_lookup_table is specified.
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
                (901, 9, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585846, 'SexAtBirth_Male', 9999), -- new ID: 206 --
                (902, 2, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585846, 'SexAtBirth_Male', 1002), -- new ID: 207 --
                (903, 2, 1585838, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585838, 1585839, 'GenderIdentity_Man', 1002),  -- new ID: 208 --
                (904, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586147, 'WhatRaceEthnicity_Hispanic', 1002),  -- new ID: 209 --
                (905, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586146, 'WhatRaceEthnicity_White', 1002), -- new ID: 210 --
                (906, 2, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586142, 'WhatRaceEthnicity_Asian', 1002), -- new ID: 211 --
                (907, 3, 1585845, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1585845, 1585846, 'SexAtBirth_Male', 1003), -- new ID: 212 (This new id will not be used anywhere but it is assigned anyway in the new_obs_id_lookup table) --
                (999, 9, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586142, 'WhatRaceEthnicity_Asian', 9999) -- new ID: 213 --
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
                (906, 'dummy_rdr_2', 96, 'rdr', 'observation'),
                (907, 'dummy_rdr_2', 97, 'rdr', 'observation'),
                (999, 'dummy_rdr_2', 99, 'rdr', 'observation')
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
                (906, 'PPI/PM', NULL),
                (907, 'PPI/PM', NULL),
                (999, 'PPI/PM', NULL)
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
                (1002, 2, 1586134, timestamp('2022-01-01 12:34:56'), 0, 0, 0, 42531021, 1586134, 1),
                (1020, 2, 1586134, timestamp('2022-01-02 12:34:56'), 0, 0, 0, 42531021, 1586134, 1),
                (1003, 3, 1586134, timestamp('2022-01-01 12:34:56'), 0, 0, 0, 42531021, 1586134, 1),
                (9999, 9, 1586134, timestamp('2022-01-01 12:34:56'), 0, 0, 0, 42531021, 1586134, 0)
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
                (1002, 'dummy_rdr_2', 1002, 'rdr', 'survey_conduct'),
                (1020, 'dummy_rdr_2', 1020, 'rdr', 'survey_conduct'),
                (1003, 'dummy_rdr_2', 1003, 'rdr', 'survey_conduct'),
                (9999, 'dummy_rdr_2', 9999, 'rdr', 'survey_conduct')
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
                (1002, 'PPI/PM', 'es'),
                (1020, 'PPI/PM', 'es'),
                (1003, 'PPI/PM', 'es'),
                (9999, 'PPI/PM', 'es')
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

        # person_id 9 is a participant that we must not include to the output.
        # (e.g. AIAN participants are included in incremental_dataset while
        #  the final output must not include AIAN participants for releases W/O AIAN)
        insert_incremental_pers = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{pers}}`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES
                (2, 1585839, 1995, 1585841, 38003563), -- existing in original --
                (3, 1585839, 1995, 1585841, 38003563), -- new from incremental --
                (9, 1585839, 1995, 1585841, 38003563)  -- new from incremental, must be excluded when exclude_lookup_table is present --
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        pers=PERSON)

        insert_pers_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{pers_ext}}`
                (person_id, state_of_residence_concept_id, state_of_residence_source_value, sex_at_birth_source_concept_id)
            VALUES
                (1, 1585266, 'PII State: CA', 1585847),
                (2, 1585266, 'PII State: CA', 1585847)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        pers_ext=PERS_EXT)

        insert_incremental_pers_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{incremental_dataset}}.{{pers_ext}}`
                (person_id, state_of_residence_concept_id, state_of_residence_source_value, sex_at_birth_source_concept_id)
            VALUES
                (2, NULL, NULL, 1585846), -- existing in original --
                (3, NULL, NULL, 1585846), -- new from incremental --
                (9, NULL, NULL, 1585846)  -- new from incremental, must be excluded when exclude_lookup_table is present --
            """).render(project=self.project_id,
                        incremental_dataset=self.incremental_dataset_id,
                        pers_ext=PERS_EXT)

        create_exclusion_table = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{fq_exclusion_table}}` (person_id INT64, research_id INT64)
        """).render(fq_exclusion_table=self.fq_exclusion_table)

        insert_exclusion_table = self.jinja_env.from_string("""
        INSERT INTO `{{fq_exclusion_table}}` (person_id, research_id)
        VALUES
        (9, 9)
        """).render(fq_exclusion_table=self.fq_exclusion_table)

        self.load_test_data([
            insert_obs, insert_incremental_obs, insert_obs_mapping,
            insert_incremental_obs_mapping, insert_obs_ext,
            insert_incremental_obs_ext, insert_sc, insert_incremental_sc,
            insert_sc_mapping, insert_incremental_sc_mapping, insert_sc_ext,
            insert_incremental_sc_ext, insert_pers, insert_incremental_pers,
            insert_pers_ext, insert_incremental_pers_ext,
            create_exclusion_table, insert_exclusion_table
        ])

    @mock_patch_bundle
    def test_remediate_basics_combined_release(self, mock_is_rdr,
                                               mock_is_combined_release,
                                               mock_is_deid,
                                               mock_is_deid_release):
        """Test to ensure RemediateBasics works as expected for COMBINED_RELEASE dataset.
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
                * 901 - 999 become 206 - 213 respectively after re-mapping using NEW_OBS_ID_LOOKUP.
            person_id == 3:
                This person is new. The records are ignored.
            person_id == 9 (observation_id 901 and 999):
                This person only exists in the incremental dataset. Such data MUST NOT be included
                in the final output. (e.g. AIAN participants are included in incremental_dataset while
                the final output must not include AIAN participants for releases W/O AIAN)

        [2] SURVEY_CONDUCT and its ext/mapping tables
            survey_conduct_id = 1001 does not change.
            survey_conduct_id = 1002 gets sandboxed and updated since it exists in the incremental dataset.
            survey_conduct_id = 1020 is a brand-new ID and inserted into final output.
            survey_conduct_id = 1003 gets ignored because it belongs to person_id = 3
            survey_conduct_id = 9999 gets ignored because it belongs to person_id = 9

        [3] PERSON table
            person_id = 1 does not change.
            person_id = 2 gets sandboxed and updated since it exists in the incremental dataset.
            person_id = 3 is a new person from the incremental dataset. Will be ignored.
            person_id = 9 gets ignored because it only exists in the incremental dataset.

        [4] PERSON_EXT table
            This table does not exist in combined dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_rdr.return_value, mock_is_combined_release.return_value, mock_is_deid.return_value, mock_is_deid_release.return_value = False, True, False, False

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
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
                               (201, 'dummy_rdr_1'), (207, 'dummy_rdr_2'),
                               (208, 'dummy_rdr_2'), (209, 'dummy_rdr_2'),
                               (210, 'dummy_rdr_2'), (211, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 1), (1020, 1)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_MAPPING}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'src_dataset_id'],
            'cleaned_values': [(1001, 'dummy_rdr_1'), (1002, 'dummy_rdr_2'),
                               (1020, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'language'],
            'cleaned_values': [(1001, 'en'), (1002, 'es'), (1020, 'es')]
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

        # Sandbox for PERSON_EXT does not exist because it's combined release dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}')

    @mock_patch_bundle
    def test_remediate_basics_combined(self, mock_is_rdr,
                                       mock_is_combined_release, mock_is_deid,
                                       mock_is_deid_release):
        """Test to ensure RemediateBasics works as expected for COMBINED dataset.
        [1] OBSERVATION and its mapping table
            Same result as test_remediate_basics_combined_release

        [2] SURVEY_CONDUCT and its ext/mapping tables
            Same result as test_remediate_basics_combined_release

        [3] PERSON table
            Same result as test_remediate_basics_combined_release

        [4] PERSON_EXT, OBS_EXT, SC_EXT tables
            These tables do not exist in combined dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_rdr.return_value, mock_is_combined_release.return_value, mock_is_deid.return_value, mock_is_deid_release.return_value = False, False, False, False

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
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
                               (201, 'dummy_rdr_1'), (207, 'dummy_rdr_2'),
                               (208, 'dummy_rdr_2'), (209, 'dummy_rdr_2'),
                               (210, 'dummy_rdr_2'), (211, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 1), (1020, 1)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_MAPPING}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'src_dataset_id'],
            'cleaned_values': [(1001, 'dummy_rdr_1'), (1002, 'dummy_rdr_2'),
                               (1020, 'dummy_rdr_2')]
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

        # Sandbox tables for EXT tables do not exist because it's combined dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}')

    @mock_patch_bundle
    def test_remediate_basics_deid(self, mock_is_rdr, mock_is_combined_release,
                                   mock_is_deid, mock_is_deid_release):
        """Test to ensure RemediateBasics works as expected for DEID (= CT and RT) NOT BASE/CLEAN dataset.
        [1] OBSERVATION and its ext table
            Same result as test_remediate_basics_combined_release

        [2] SURVEY_CONDUCT and its ext table
            Same result as test_remediate_basics_combined_release

        [3] PERSON table
            Same result as test_remediate_basics_combined_release

        [4] PERSON_EXT table
            This table does not exist in deid dataset. No sandboxing/deleting/inserting will run on it.

        [5] OBSERVATION_MAPPING and SURVEY_CONDUCT_MAPPING tables
            These tables do not exist in deid dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_rdr.return_value, mock_is_combined_release.return_value, mock_is_deid.return_value, mock_is_deid_release.return_value = False, False, True, False

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 1), (1020, 1)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'language'],
            'cleaned_values': [(1001, 'en'), (1002, 'es'), (1020, 'es')]
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

        # Sandbox for PERS_EXT, OBS_MAP and SC_MAP do not exist because it's deid dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_mapping}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}')

    @mock_patch_bundle
    def test_remediate_basics_deid_release(self, mock_is_rdr,
                                           mock_is_combined_release,
                                           mock_is_deid, mock_is_deid_release):
        """Test to ensure RemediateBasics works as expected for DEID (= CT and RT) BASE/CLEAN dataset.
        [1] OBSERVATION and its ext table
            Same result as test_remediate_basics_combined_release

        [2] SURVEY_CONDUCT and its ext table
            Same result as test_remediate_basics_combined_release

        [3] PERSON table
            Same result as test_remediate_basics_combined_release

        [4] PERSON_EXT table
            Just like PERSON table...
            1 stays the same, 2 is updated, 3 is inserted, and 9 is ignored.
            3's state columns are NULL because it does not exist in the source dataset.

        [5] OBSERVATION_MAPPING and SURVEY_CONDUCT_MAPPING tables
            These tables do not exist in deid dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_rdr.return_value, mock_is_combined_release.return_value, mock_is_deid.return_value, mock_is_deid_release.return_value = False, False, True, True

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 1), (1020, 1)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'language'],
            'cleaned_values': [(1001, 'en'), (1002, 'es'), (1020, 'es')]
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
            'fields': [
                'person_id', 'state_of_residence_concept_id',
                'state_of_residence_source_value',
                'sex_at_birth_source_concept_id'
            ],
            'cleaned_values': [(1, 1585266, 'PII State: CA', 1585847),
                               (2, 1585266, 'PII State: CA', 1585846)]
        }]

        self.default_test(tables_and_counts)

        # Sandbox for OBS_MAP and SC_MAP do not exist because it's deid dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_mapping}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}')

    @mock_patch_bundle
    def test_remediate_basics_rdr(self, mock_is_rdr, mock_is_combined_release,
                                  mock_is_deid, mock_is_deid_release):
        """Test to ensure RemediateBasics works as expected for RDR dataset.
        [1] OBSERVATION, SURVEY_CONDUCT, PERSON tables
            Same result as test_remediate_basics_combined_release

        [2] mapping and ext tables
            These tables do not exist in rdr dataset. No sandboxing/deleting/inserting will run on it.
        """
        mock_is_rdr.return_value, mock_is_combined_release.return_value, mock_is_deid.return_value, mock_is_deid_release.return_value = True, False, False, False

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 201, 202, 203, 204, 205],
            'sandboxed_ids': [202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (207,),
                               (208,), (209,), (210,), (211,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 1), (1020, 1)]
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

        # Sandbox for ext and mapping do not exist because it's rdr dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_mapping}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}')
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}')

    @mock_patch_bundle
    def test_no_exclusion(self, mock_is_rdr, mock_is_combined_release,
                          mock_is_deid, mock_is_deid_release):
        """Test to ensure RemediateBasics works as expected when exclude_lookup_table and exclude_lookup_dataset
        are not specified. Testing against COMBINED_RELEASE dataset.

        Mostly same result as test_remediate_basics_combined_release.
        Only the difference is this one includes person_id == 9 (observation_id 901 and 999) (survey_conduct_id 9999).
        person_id==9 is in exclude_lookup_table but this table is not referenced in this test case.
        """
        # Unsetting exclude_lookup_xyz arguments.
        self.kwargs.update({'exclude_lookup_dataset': None})
        self.kwargs.update({'exclude_lookup_table': None})

        # Adding records for person_id==9
        insert_obs = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs}}`
                (observation_id, person_id, observation_concept_id, observation_date,
                 observation_datetime, observation_type_concept_id, observation_source_concept_id,
                 value_source_concept_id, value_source_value, questionnaire_response_id)
            VALUES
                (200, 9, 1586140, date('2022-01-01'), timestamp('2022-01-01 12:34:56'), 45905771, 1586140, 1586144, 'WhatRaceEthnicity_MENA', 9999)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        obs=OBSERVATION)

        insert_obs_mapping = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs_mapping}}`
                (observation_id, src_dataset_id, src_observation_id, src_hpo_id, src_table_id)
            VALUES
                (200, 'dummy_rdr_1', 20, 'rdr', 'observation')
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        obs_mapping=OBS_MAPPING)

        insert_obs_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{obs_ext}}`
                (observation_id, src_id, survey_version_concept_id)
            VALUES
                (200, 'PPI/PM', NULL)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        obs_ext=OBS_EXT)

        insert_pers = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{pers}}`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES
                (9, 1585839, 1995, 1585841, 38003563)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        pers=PERSON)

        insert_pers_ext = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{pers_ext}}`
                (person_id, state_of_residence_concept_id, state_of_residence_source_value, sex_at_birth_source_concept_id)
            VALUES
                (9, 1585266, 'PII State: CA', 1585847)
            """).render(project=self.project_id,
                        dataset=self.dataset_id,
                        pers_ext=PERS_EXT)

        self.load_test_data([
            insert_obs, insert_obs_mapping, insert_obs_ext, insert_pers,
            insert_pers_ext
        ])

        mock_is_rdr.return_value, mock_is_combined_release.return_value, mock_is_deid.return_value, mock_is_deid_release.return_value = False, True, False, False

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs}',
            'loaded_ids': [101, 102, 103, 104, 200, 201, 202, 203, 204, 205],
            'sandboxed_ids': [200, 202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (206,),
                               (207,), (208,), (209,), (210,), (211,), (213,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_MAPPING}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_mapping}',
            'loaded_ids': [101, 102, 103, 104, 200, 201, 202, 203, 204, 205],
            'sandboxed_ids': [200, 202, 203, 204, 205],
            'fields': ['observation_id', 'src_dataset_id'],
            'cleaned_values': [(101, 'dummy_rdr_1'), (102, 'dummy_rdr_1'),
                               (103, 'dummy_rdr_1'), (104, 'dummy_rdr_1'),
                               (201, 'dummy_rdr_1'), (206, 'dummy_rdr_2'),
                               (207, 'dummy_rdr_2'), (208, 'dummy_rdr_2'),
                               (209, 'dummy_rdr_2'), (210, 'dummy_rdr_2'),
                               (211, 'dummy_rdr_2'), (213, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBS_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_obs_ext}',
            'loaded_ids': [101, 102, 103, 104, 200, 201, 202, 203, 204, 205],
            'sandboxed_ids': [200, 202, 203, 204, 205],
            'fields': ['observation_id'],
            'cleaned_values': [(101,), (102,), (103,), (104,), (201,), (206,),
                               (207,), (208,), (209,), (210,), (211,), (213,)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'validated_survey_concept_id'],
            'cleaned_values': [(1001, 0), (1002, 1), (1020, 1), (9999, 0)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_MAPPING}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_mapping}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'src_dataset_id'],
            'cleaned_values': [(1001, 'dummy_rdr_1'), (1002, 'dummy_rdr_2'),
                               (1020, 'dummy_rdr_2'), (9999, 'dummy_rdr_2')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SC_EXT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_sc_ext}',
            'loaded_ids': [1001, 1002],
            'sandboxed_ids': [1002],
            'fields': ['survey_conduct_id', 'language'],
            'cleaned_values': [(1001, 'en'), (1002, 'es'), (1020, 'es'),
                               (9999, 'es')]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.sb_pers}',
            'loaded_ids': [1, 2, 9],
            'sandboxed_ids': [2, 9],
            'fields': ['person_id', 'gender_concept_id'],
            'cleaned_values': [(1, 1585839), (2, 1585839), (9, 1585839)]
        }]

        self.default_test(tables_and_counts)

        # Sandbox for PERSON_EXT does not exist because it's combined release dataset.
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.{self.sb_pers_ext}')

    def tearDown(self):
        self.client.delete_table(self.fq_exclusion_table, not_found_ok=True)
        super().tearDown()