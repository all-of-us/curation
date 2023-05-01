"""
Integration test for BigQuery retraction.
"""

# Python imports
import os
from unittest import mock

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from common import (ACTIVITY_SUMMARY, COMBINED, DEATH, DEID, EHR, FITBIT,
                    JINJA_ENV, MAPPING_PREFIX, OBSERVATION, OBSERVATION_PERIOD,
                    OTHER, PERSON, RDR, UNIONED_EHR)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from retraction.retract_data_bq import (NONE, RETRACTION_ONLY_EHR,
                                        RETRACTION_RDR_EHR, get_datasets_list,
                                        get_primary_key_for_sandbox_table,
                                        run_bq_retraction)
from retraction.retract_utils import get_dataset_type
from constants.retraction.retract_utils import ALL_DATASETS

CREATE_LOOKUP_TMPL = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{dataset}}.pid_rid_to_retract` 
(person_id INT64, research_id INT64)
""")

INSERT_LOOKUP_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.pid_rid_to_retract` 
    (person_id, research_id)
VALUES
    (102, 202), (104, 204)
""")

INSERT_PERS_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}` 
    (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
{% for pers_id in person_ids %}
    ({{pers_id}}, 0, 2001, 0, 0){% if not loop.last -%}, {% endif %}
{% endfor %}
""")

INSERT_OBS_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}` 
    (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
VALUES
{% for pers_id in person_ids %}
    ({{pers_id}}1, {{pers_id}}, 0, date('2021-01-01'), 0),
    ({{pers_id}}2, {{pers_id}}, 0, date('2021-01-01'), 0){% if not loop.last -%}, {% endif %}
{% endfor %}
""")

CREATE_AS_SELECT_TMPL = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{dataset}}.{{table}}` 
AS SELECT * FROM `{{project}}.{{src_dataset}}.{{src_table}}` 
""")

INSERT_MAPPING_OBS_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}._mapping_observation` 
    (observation_id, src_hpo_id)
VALUES
{% for obs_id in observation_ids %}
    {% if 1011 <= obs_id <= 1022 -%}({{obs_id}}, 'rdr')
    {% else %}({{obs_id}}, 'foo'){% endif %}
    {% if not loop.last -%}, {% endif %}
{% endfor %}
""")

INSERT_OBS_EXT_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation_ext` 
    (observation_id, src_id)
VALUES
{% for obs_id in observation_ids %}
    {% if 2011 <= obs_id <= 2022 -%}({{obs_id}}, 'PPI/PM')
    {% else %}({{obs_id}}, 'EHR 999'){% endif %}
    {% if not loop.last -%}, {% endif %}
{% endfor %}
""")

INSERT_ACTIVITY_SUMMARY_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.activity_summary` 
    (person_id)
VALUES
{% for pers_id in person_ids %}
    ({{pers_id}}){% if not loop.last -%}, {% endif %}
{% endfor %}
""")

# test data for retracting sandbox tables
INSERT_SANDBOX_OBS_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.dc111_dc-222_observation` 
    (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
VALUES
    (1000000000000101, 101, 0, date('2021-01-01'), 0),
    (1000000000000102, 102, 0, date('2021-01-01'), 0),
    (2000000000000103, 103, 0, date('2021-01-01'), 0),
    (2000000000000104, 104, 0, date('2021-01-01'), 0),
    (1000000000000201, 201, 0, date('2021-01-01'), 0),
    (1000000000000202, 202, 0, date('2021-01-01'), 0),
    (2000000000000203, 203, 0, date('2021-01-01'), 0),
    (2000000000000204, 204, 0, date('2021-01-01'), 0)
""")
INSERT_SANDBOX_OBS_PERIOD_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc111_dc-222_observation_period` 
AS
SELECT 1000000000000101 AS observation_period_id, 101 AS person_id UNION ALL
SELECT 1000000000000102 AS observation_period_id, 102 AS person_id UNION ALL
SELECT 2000000000000103 AS observation_period_id, 103 AS person_id UNION ALL
SELECT 2000000000000104 AS observation_period_id, 104 AS person_id UNION ALL
SELECT 1000000000000201 AS observation_period_id, 201 AS person_id UNION ALL
SELECT 1000000000000202 AS observation_period_id, 202 AS person_id UNION ALL
SELECT 2000000000000203 AS observation_period_id, 203 AS person_id UNION ALL
SELECT 2000000000000204 AS observation_period_id, 204 AS person_id
""")

PERS_ALL = [101, 102, 103, 104, 201, 202, 203, 204]
PERS_PID_TO_RETRACT = [102, 104]
PERS_RID_TO_RETRACT = [202, 204]
PERS_PID_AFTER_RETRACTION = [101, 103, 201, 202, 203, 204]
PERS_RID_AFTER_RETRACTION = [101, 102, 103, 104, 201, 203]

OBS_ALL = [
    1011, 1012, 1021, 1022, 1031, 1032, 1041, 1042, 2011, 2012, 2021, 2022,
    2031, 2032, 2041, 2042
]
OBS_PID_TO_RETRACT = [1021, 1022, 1041, 1042]
OBS_RID_TO_RETRACT = [2021, 2022, 2041, 2042]
OBS_PID_TO_RETRACT_ONLY_EHR = [1041, 1042]
OBS_RID_TO_RETRACT_ONLY_EHR = [2041, 2042]
OBS_PID_AFTER_RETRACTION = [
    1011, 1012, 1031, 1032, 2011, 2012, 2021, 2022, 2031, 2032, 2041, 2042
]
OBS_RID_AFTER_RETRACTION = [
    1011, 1012, 1021, 1022, 1031, 1032, 1041, 1042, 2011, 2012, 2031, 2032
]
OBS_PID_AFTER_RETRACTION_ONLY_EHR = [
    1011, 1012, 1021, 1022, 1031, 1032, 2011, 2012, 2021, 2022, 2031, 2032,
    2041, 2042
]
OBS_RID_AFTER_RETRACTION_ONLY_EHR = [
    1011, 1012, 1021, 1022, 1031, 1032, 1041, 1042, 2011, 2012, 2021, 2022,
    2031, 2032
]


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
    mock.patch('retraction.retract_data_bq.is_sandbox_dataset'),
    mock.patch('retraction.retract_utils.is_sandbox_dataset'),
    mock.patch('retraction.retract_data_bq.is_fitbit_dataset'),
    mock.patch('retraction.retract_data_bq.is_deid_dataset'),
    mock.patch('retraction.retract_data_bq.is_combined_dataset'),
    mock.patch('retraction.retract_data_bq.is_unioned_dataset'),
    mock.patch('retraction.retract_data_bq.is_ehr_dataset'),
    mock.patch('retraction.retract_data_bq.is_rdr_dataset'),
    mock.patch('retraction.retract_data_bq.get_dataset_type'),
    mock.patch('retraction.retract_utils.get_dataset_type'))


class RetractDataBqTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        sandbox_id = f"{os.environ.get('BIGQUERY_DATASET_ID')}_sandbox"
        cls.sandbox_id = sandbox_id

        cls.rdr_id = os.environ.get('RDR_DATASET_ID')
        cls.ehr_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.unioned_ehr_id = os.environ.get('UNIONED_DATASET_ID')
        cls.combined_id = os.environ.get('COMBINED_DATASET_ID')
        cls.deid_id = os.environ.get('COMBINED_DEID_DATASET_ID')
        cls.fitbit_id = f"{os.environ.get('BIGQUERY_DATASET_ID')}_fitbit"
        cls.other_id = os.environ.get('RDR_DATASET_ID')

        cls.hpo_id = 'fake'
        cls.lookup_table_id = 'pid_rid_to_retract'

        # omop tables
        for dataset in [
                cls.rdr_id, cls.unioned_ehr_id, cls.combined_id, cls.deid_id
        ]:
            cls.fq_table_names.extend([
                f'{project_id}.{dataset}.{PERSON}',
                f'{project_id}.{dataset}.{OBSERVATION}'
            ])
            cls.fq_sandbox_table_names.extend([
                f'{project_id}.{sandbox_id}.retract_{dataset}_{PERSON}',
                f'{project_id}.{sandbox_id}.retract_{dataset}_{OBSERVATION}'
            ])

        # mapping tables/ ext tables
        cls.fq_table_names.extend([
            f'{project_id}.{cls.combined_id}._mapping_{OBSERVATION}',
            f'{project_id}.{cls.deid_id}.{OBSERVATION}_ext'
        ])

        # fitbit tables
        for dataset in [cls.deid_id, cls.fitbit_id]:
            cls.fq_table_names.append(
                f'{project_id}.{dataset}.{ACTIVITY_SUMMARY}')
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.retract_{dataset}_{ACTIVITY_SUMMARY}'
            )

        # tables for EHR dataset and sandbox dataset are in fq_sandbox_table_names
        # because they have a prefix and create_table for fq_table_names do
        # not work for tables with such naming.
        cls.fq_sandbox_table_names.extend([
            f'{project_id}.{cls.ehr_id}.dc111_dc-222_{OBSERVATION}',
            f'{project_id}.{sandbox_id}.retract_{cls.ehr_id}_dc111_dc-222_{OBSERVATION}',
            f'{project_id}.{cls.ehr_id}.dc111_dc-222_{OBSERVATION_PERIOD}',
            f'{project_id}.{sandbox_id}.retract_{cls.ehr_id}_dc111_dc-222_{OBSERVATION_PERIOD}',
        ])
        for hpo in [cls.hpo_id, UNIONED_EHR, 'foo']:
            cls.fq_sandbox_table_names.extend([
                f'{project_id}.{cls.ehr_id}.{hpo}_{PERSON}',
                f'{project_id}.{cls.ehr_id}.{hpo}_{OBSERVATION}',
                f'{project_id}.{sandbox_id}.retract_{cls.ehr_id}_{hpo}_{PERSON}',
                f'{project_id}.{sandbox_id}.retract_{cls.ehr_id}_{hpo}_{OBSERVATION}'
            ])

        # lookup table
        cls.fq_sandbox_table_names.append(
            f'{project_id}.{sandbox_id}.{cls.lookup_table_id}')

        super().setUpClass()

    def setUp(self):
        super().setUp()

        queries = []

        # populate a test table for sandbox table
        create_sb_obs = CREATE_AS_SELECT_TMPL.render(
            project=self.project_id,
            dataset=self.ehr_id,
            table=f'dc111_dc-222_{OBSERVATION}',
            src_dataset=self.rdr_id,
            src_table=OBSERVATION)
        insert_sb_obs = INSERT_SANDBOX_OBS_TMPL.render(project=self.project_id,
                                                       dataset=self.ehr_id)
        insert_sb_obs_period = INSERT_SANDBOX_OBS_PERIOD_TMPL.render(
            project=self.project_id, dataset=self.ehr_id)
        queries.extend([create_sb_obs, insert_sb_obs, insert_sb_obs_period])

        # omop tables
        for dataset in [
                self.rdr_id, self.unioned_ehr_id, self.combined_id, self.deid_id
        ]:
            insert_pers = INSERT_PERS_TMPL.render(project=self.project_id,
                                                  dataset=dataset,
                                                  table=PERSON,
                                                  person_ids=PERS_ALL)
            insert_obs = INSERT_OBS_TMPL.render(project=self.project_id,
                                                dataset=dataset,
                                                table=OBSERVATION,
                                                person_ids=PERS_ALL)
            queries.extend([insert_pers, insert_obs])

        # mapping tables/ ext tables
        insert_map_obs = INSERT_MAPPING_OBS_TMPL.render(
            project=self.project_id,
            dataset=self.combined_id,
            observation_ids=OBS_ALL)
        insert_obs_ext = INSERT_OBS_EXT_TMPL.render(project=self.project_id,
                                                    dataset=self.deid_id,
                                                    observation_ids=OBS_ALL)
        queries.extend([insert_map_obs, insert_obs_ext])

        # fitbit tables
        for dataset in [self.deid_id, self.fitbit_id]:
            insert_activity_summary = INSERT_ACTIVITY_SUMMARY_TMPL.render(
                project=self.project_id, dataset=dataset, person_ids=PERS_ALL)
            queries.extend([insert_activity_summary])

        # tables for EHR dataset
        for hpo in [self.hpo_id, UNIONED_EHR, 'foo']:
            create_pers_ehr = CREATE_AS_SELECT_TMPL.render(
                project=self.project_id,
                dataset=self.ehr_id,
                table=f'{hpo}_{PERSON}',
                src_dataset=self.rdr_id,
                src_table=PERSON)
            create_obs_ehr = CREATE_AS_SELECT_TMPL.render(
                project=self.project_id,
                dataset=self.ehr_id,
                table=f'{hpo}_{OBSERVATION}',
                src_dataset=self.rdr_id,
                src_table=OBSERVATION)
            queries.extend([create_pers_ehr, create_obs_ehr])

        # lookup table
        create_lookup = CREATE_LOOKUP_TMPL.render(project=self.project_id,
                                                  dataset=self.sandbox_id)
        insert_lookup = INSERT_LOOKUP_TMPL.render(project=self.project_id,
                                                  dataset=self.sandbox_id)
        queries.extend([create_lookup, insert_lookup])

        self.load_test_data(queries)

    @mock_patch_bundle
    def test_retract_unioned_ehr_rdr_and_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for unioned ehr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on person_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_unioned.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = UNIONED_EHR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.unioned_ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_unioned_ehr_only_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for unioned ehr dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retracts records based on person_ids in the lookup table.
        * Same result as retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_unioned.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = UNIONED_EHR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.unioned_ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_combined_rdr_and_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for combined dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on person_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_combined.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = COMBINED

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.combined_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_combined_only_ehr(self, mock_ru_get_dataset_type,
                                       mock_rdb_get_dataset_type, mock_is_rdr,
                                       mock_is_ehr, mock_is_unioned,
                                       mock_is_combined, mock_is_deid,
                                       mock_is_fitbit, mock_ru_is_sandbox,
                                       mock_rdb_is_sandbox):
        """
        Test for combined dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retracts records based on person_ids in the lookup table and
        src_hpo_id in the mapping table.
        person table is not retracted because all the data is from RDR.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_combined.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = COMBINED

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.combined_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_ALL)
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.retract_{dataset_id}_{PERSON}'
        )

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'],
                               OBS_PID_AFTER_RETRACTION_ONLY_EHR)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT_ONLY_EHR)

    @mock_patch_bundle
    def test_retract_deid_rdr_and_ehr(self, mock_ru_get_dataset_type,
                                      mock_rdb_get_dataset_type, mock_is_rdr,
                                      mock_is_ehr, mock_is_unioned,
                                      mock_is_combined, mock_is_deid,
                                      mock_is_fitbit, mock_ru_is_sandbox,
                                      mock_rdb_is_sandbox):
        """
        Test for deid dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on research_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_deid.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = DEID

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.deid_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_RID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], PERS_RID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], OBS_RID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_RID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], PERS_RID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}',
            ['person_id'], PERS_RID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_deid_only_ehr(self, mock_ru_get_dataset_type,
                                   mock_rdb_get_dataset_type, mock_is_rdr,
                                   mock_is_ehr, mock_is_unioned,
                                   mock_is_combined, mock_is_deid,
                                   mock_is_fitbit, mock_ru_is_sandbox,
                                   mock_rdb_is_sandbox):
        """
        Test for deid dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retracts records based on research_ids in the lookup table and
        src_id in the ext table.
        person table and fitbit table are not retracted because all the
        data is from RDR.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_deid.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = DEID

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.deid_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_ALL)
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.retract_{dataset_id}_{PERSON}'
        )

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'],
                               OBS_RID_AFTER_RETRACTION_ONLY_EHR)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_RID_TO_RETRACT_ONLY_EHR)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], PERS_ALL)
        self.assertTableDoesNotExist(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}'
        )

    @mock_patch_bundle
    def test_retract_rdr_rdr_and_ehr(self, mock_ru_get_dataset_type,
                                     mock_rdb_get_dataset_type, mock_is_rdr,
                                     mock_is_ehr, mock_is_unioned,
                                     mock_is_combined, mock_is_deid,
                                     mock_is_fitbit, mock_ru_is_sandbox,
                                     mock_rdb_is_sandbox):
        """
        Test for rdr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on person_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_rdr.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = RDR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.rdr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_rdr_only_ehr(self, mock_ru_get_dataset_type,
                                  mock_rdb_get_dataset_type, mock_is_rdr,
                                  mock_is_ehr, mock_is_unioned,
                                  mock_is_combined, mock_is_deid,
                                  mock_is_fitbit, mock_ru_is_sandbox,
                                  mock_rdb_is_sandbox):
        """
        Test for rdr dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retraction is skipped for RDR dataset when 'only_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_rdr.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = RDR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.rdr_id

        with self.assertLogs(level='WARNING') as cm:
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE, [dataset_id], RETRACTION_ONLY_EHR, False,
                              self.client)
        self.assertTrue(f"Skipping retraction" in cm.output[0])

    @mock_patch_bundle
    def test_retract_ehr_rdr_and_ehr(self, mock_ru_get_dataset_type,
                                     mock_rdb_get_dataset_type, mock_is_rdr,
                                     mock_is_ehr, mock_is_unioned,
                                     mock_is_combined, mock_is_deid,
                                     mock_is_fitbit, mock_ru_is_sandbox,
                                     mock_rdb_is_sandbox):
        """
        Test for ehr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Only tables of the specified hpo_id and unioned_ehr are retracted.
        * EHR dataset's table naming convention is different from other datasets.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_ehr.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = EHR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          self.hpo_id, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{PERSON}', ['person_id'],
            PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{PERSON}', ['person_id'],
            PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.foo_{OBSERVATION}',
                               ['observation_id'], OBS_ALL)
        self.assertTableDoesNotExist(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_foo_{OBSERVATION}',
        )

    @mock_patch_bundle
    def test_retract_ehr_only_ehr(self, mock_ru_get_dataset_type,
                                  mock_rdb_get_dataset_type, mock_is_rdr,
                                  mock_is_ehr, mock_is_unioned,
                                  mock_is_combined, mock_is_deid,
                                  mock_is_fitbit, mock_ru_is_sandbox,
                                  mock_rdb_is_sandbox):
        """
        Test for ehr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Only tables of the specified hpo_id and unioned_ehr are retracted.
        * EHR dataset's table naming convention is different from other datasets.
        * Same result as retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_ehr.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = EHR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          self.hpo_id, [dataset_id], RETRACTION_ONLY_EHR, False,
                          self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{PERSON}', ['person_id'],
            PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{PERSON}', ['person_id'],
            PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.foo_{OBSERVATION}',
                               ['observation_id'], OBS_ALL)
        self.assertTableDoesNotExist(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_foo_{OBSERVATION}',
        )

    @mock_patch_bundle
    def test_retract_ehr_without_hpo_id(self, mock_ru_get_dataset_type,
                                        mock_rdb_get_dataset_type, mock_is_rdr,
                                        mock_is_ehr, mock_is_unioned,
                                        mock_is_combined, mock_is_deid,
                                        mock_is_fitbit, mock_ru_is_sandbox,
                                        mock_rdb_is_sandbox):
        """
        Test for ehr dataset.
        Retraction runs against all the tables with person_id in EHR dataset 
        if hpo_id is none.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_ehr.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = EHR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{PERSON}', ['person_id'],
            PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{PERSON}', ['person_id'],
            PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.foo_{PERSON}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_foo_{PERSON}',
            ['person_id'], PERS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.foo_{OBSERVATION}',
                               ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_foo_{OBSERVATION}',
            ['observation_id'], OBS_PID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_fitbit_rdr_and_ehr(self, mock_ru_get_dataset_type,
                                        mock_rdb_get_dataset_type, mock_is_rdr,
                                        mock_is_ehr, mock_is_unioned,
                                        mock_is_combined, mock_is_deid,
                                        mock_is_fitbit, mock_ru_is_sandbox,
                                        mock_rdb_is_sandbox):
        """
        Test for fitbit dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_fitbit.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = FITBIT

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}',
            ['person_id'], PERS_PID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_fitbit_only_ehr(self, mock_ru_get_dataset_type,
                                     mock_rdb_get_dataset_type, mock_is_rdr,
                                     mock_is_ehr, mock_is_unioned,
                                     mock_is_combined, mock_is_deid,
                                     mock_is_fitbit, mock_ru_is_sandbox,
                                     mock_rdb_is_sandbox):
        """
        Test for fitbit dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retraction is skipped for fitbit dataset when 'only_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_fitbit.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = FITBIT

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        with self.assertLogs(level='WARNING') as cm:
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE, [dataset_id], RETRACTION_ONLY_EHR, False,
                              self.client)
        self.assertTrue(f"Skipping retraction" in cm.output[0])

    @mock_patch_bundle
    def test_retract_deid_fitbit_rdr_and_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for fitbit dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_deid.return_value = True
        mock_is_fitbit.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = FITBIT

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], PERS_RID_AFTER_RETRACTION)

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}',
            ['person_id'], PERS_RID_TO_RETRACT)

    @mock_patch_bundle
    def test_retract_deid_fitbit_only_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for deid fitbit dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retraction is skipped for deid fitbit dataset when 'only_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_deid.return_value = True
        mock_is_fitbit.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = FITBIT

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        with self.assertLogs(level='WARNING') as cm:
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE, [dataset_id], RETRACTION_ONLY_EHR, False,
                              self.client)
        self.assertTrue(f"Skipping retraction" in cm.output[0])

    @mock_patch_bundle
    def test_retract_other(self, mock_ru_get_dataset_type,
                           mock_rdb_get_dataset_type, mock_is_rdr, mock_is_ehr,
                           mock_is_unioned, mock_is_combined, mock_is_deid,
                           mock_is_fitbit, mock_ru_is_sandbox,
                           mock_rdb_is_sandbox):
        """
        Test for dataset that is none of the above.
        retract_utils.get_datasets_list() excludes such datasets from retraction
        regardless of `retraction_type`.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = OTHER

        dataset_list = get_datasets_list(self.client, [self.other_id])
        self.assertEqual(dataset_list, [])

    @mock_patch_bundle
    def test_retract_sandbox_rdr_and_ehr(self, mock_ru_get_dataset_type,
                                         mock_rdb_get_dataset_type, mock_is_rdr,
                                         mock_is_ehr, mock_is_unioned,
                                         mock_is_combined, mock_is_deid,
                                         mock_is_fitbit, mock_ru_is_sandbox,
                                         mock_rdb_is_sandbox):
        """
        Test for sandbox datasets.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retraction runs based on person_id, not research_id.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_combined.return_value = True
        mock_rdb_is_sandbox.return_value = mock_ru_is_sandbox.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = COMBINED

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [
                1000000000000101, 2000000000000103, 1000000000000201,
                1000000000000202, 2000000000000203, 2000000000000204
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [1000000000000102, 2000000000000104])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [
                1000000000000101, 2000000000000103, 1000000000000201,
                1000000000000202, 2000000000000203, 2000000000000204
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [1000000000000102, 2000000000000104])

    @mock_patch_bundle
    def test_retract_sandbox_only_ehr(self, mock_ru_get_dataset_type,
                                      mock_rdb_get_dataset_type, mock_is_rdr,
                                      mock_is_ehr, mock_is_unioned,
                                      mock_is_combined, mock_is_deid,
                                      mock_is_fitbit, mock_ru_is_sandbox,
                                      mock_rdb_is_sandbox):
        """
        Test for sandbox datasets.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retraction runs based on {domain}_id and person_id.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_combined.return_value = True
        mock_rdb_is_sandbox.return_value = mock_ru_is_sandbox.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = COMBINED

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [
                1000000000000101, 1000000000000102, 2000000000000103,
                1000000000000201, 1000000000000202, 2000000000000203,
                2000000000000204
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [2000000000000104])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [
                1000000000000101, 1000000000000102, 2000000000000103,
                1000000000000201, 1000000000000202, 2000000000000203,
                2000000000000204
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [2000000000000104])

    @mock_patch_bundle
    def test_retract_deid_sandbox_rdr_and_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for sandbox datasets.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retraction runs based on research_id, not person_id.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_combined.return_value = True
        mock_is_deid.return_value = True
        mock_rdb_is_sandbox.return_value = mock_ru_is_sandbox.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = COMBINED

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, False, self.client)
        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [
                1000000000000101, 1000000000000102, 2000000000000103,
                2000000000000104, 1000000000000201, 2000000000000203
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [1000000000000202, 2000000000000204])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [
                1000000000000101, 1000000000000102, 2000000000000103,
                2000000000000104, 1000000000000201, 2000000000000203
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [1000000000000202, 2000000000000204])

    @mock_patch_bundle
    def test_retract_deid_sandbox_only_ehr(
        self, mock_ru_get_dataset_type, mock_rdb_get_dataset_type, mock_is_rdr,
        mock_is_ehr, mock_is_unioned, mock_is_combined, mock_is_deid,
        mock_is_fitbit, mock_ru_is_sandbox, mock_rdb_is_sandbox):
        """
        Test for sandbox datasets.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retraction runs based on {domain}_id and research_id.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_combined.return_value = True
        mock_is_deid.return_value = True
        mock_rdb_is_sandbox.return_value = mock_ru_is_sandbox.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = COMBINED

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [
                1000000000000101, 1000000000000102, 2000000000000103,
                2000000000000104, 1000000000000201, 1000000000000202,
                2000000000000203
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION}',
            ['observation_id'], [2000000000000204])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [
                1000000000000101, 1000000000000102, 2000000000000103,
                2000000000000104, 1000000000000201, 1000000000000202,
                2000000000000203
            ])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_dc111_dc-222_{OBSERVATION_PERIOD}',
            ['observation_period_id'], [2000000000000204])

    @mock_patch_bundle
    def test_retract_skip_sandboxing(self, mock_ru_get_dataset_type,
                                     mock_rdb_get_dataset_type, mock_is_rdr,
                                     mock_is_ehr, mock_is_unioned,
                                     mock_is_combined, mock_is_deid,
                                     mock_is_fitbit, mock_ru_is_sandbox,
                                     mock_rdb_is_sandbox):
        """
        Test for skip_sandboxing option.
        Everything is same with test_rdr_rdr_and_ehr except skip_sandboxing=True.
        Retraction runs but no sandbox tables are created.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit, mock_ru_is_sandbox,
                mock_rdb_is_sandbox
        ]:
            mock_.return_value = False
        mock_is_rdr.return_value = True
        mock_rdb_get_dataset_type.return_value = mock_ru_get_dataset_type.return_value = RDR

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.rdr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, NONE,
                          [dataset_id], RETRACTION_RDR_EHR, True, self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], PERS_PID_AFTER_RETRACTION)
        self.assertTableDoesNotExist(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}')

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], OBS_PID_AFTER_RETRACTION)
        self.assertTableDoesNotExist(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}')

    def test_retract_all(self):
        """
        When ['all_datasets'] specified for dataset_list, BQ retraction runs against all the datasets
        excluding OTHER datasets.
        """
        actual = set(get_datasets_list(self.client, [ALL_DATASETS]))
        all_datasets = set([
            dataset.dataset_id for dataset in list(self.client.list_datasets())
        ])
        datasets_to_exclude = set([
            dataset for dataset in all_datasets
            if get_dataset_type(dataset) == OTHER
        ])

        self.assertTrue(actual.issubset(all_datasets))
        self.assertTrue(actual.intersection(datasets_to_exclude) == set())
        self.assertTrue(
            len(actual) == len(all_datasets) - len(datasets_to_exclude))

    def test_retract_none(self):
        """
        When ['none'] or [] is specified for dataset_list, all BQ retraction is skipped.
        """
        for dataset_ids in [[NONE], [], None]:
            dataset_list = get_datasets_list(self.client, dataset_ids)
            self.assertEqual(dataset_list, [])


class RetractDataBqGetPKforSBTableTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('BIGQUERY_DATASET_ID')

        cls.fq_table_names.extend(
            [f'{cls.project_id}.{cls.dataset_id}.{PERSON}'])

        # tables for EHR dataset and sandbox dataset are in fq_sandbox_table_names
        # because they have a prefix and create_table for fq_table_names do
        # not work for tables with such naming.
        cls.fq_sandbox_table_names.extend([
            f'{cls.project_id}.{cls.dataset_id}.dc111_dc-111_{PERSON}',
            f'{cls.project_id}.{cls.dataset_id}.dc111_dc-111_{DEATH}',
            f'{cls.project_id}.{cls.dataset_id}.dc999_dc-999_{DEATH}',
            f'{cls.project_id}.{cls.dataset_id}.dc111_dc-111_{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.dc888_dc-888_{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.dc999_dc-999_{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.dc999_dc-999_{MAPPING_PREFIX}{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.dc111_dc-111_{OBSERVATION_PERIOD}',
        ])

        super().setUpClass()

    def setUp(self):
        super().setUp()

        create_sb_pers = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc111_dc-111_person`
        (person_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_death = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc111_dc-111_death`
        (person_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_deat_no_pk = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc999_dc-999_death`
        (dummy_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_obs = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc111_dc-111_observation`
        (observation_id INT64, person_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_obs_no_pk = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc888_dc-888_observation`
        (person_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_obs_no_person_id = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc999_dc-999_observation`
        (observation_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_obs_mapping = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc999_dc-999_mapping_observation`
        (observation_id INT64, person_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        create_sb_obs_period = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.dc111_dc-111_observation_period`
        (observation_period_id INT64, person_id INT64)
        """).render(project=self.project_id, dataset=self.dataset_id)

        self.load_test_data([
            create_sb_pers, create_sb_death, create_sb_deat_no_pk,
            create_sb_obs, create_sb_obs_no_pk, create_sb_obs_no_person_id,
            create_sb_obs_mapping, create_sb_obs_period
        ])

    def test_get_primary_key_for_sandbox_table(self):
        """
        Test for get_primary_key_for_sandbox_table() to confirm it returns
        the primary key column that we can use for `only_ehr` retraction in
        sandbox datasets.
        """
        expected_primary_keys = {
            f"dc111_dc-111_{PERSON}": f"{PERSON}_id",
            f"dc111_dc-111_{DEATH}": f"{PERSON}_id",
            f"dc999_dc-999_{DEATH}": '',
            f"dc111_dc-111_{OBSERVATION}": f"{OBSERVATION}_id",
            f"dc888_dc-888_{OBSERVATION}": '',
            f"dc999_dc-999_{OBSERVATION}": '',
            f"dc999_dc-999_{MAPPING_PREFIX}{OBSERVATION}": '',
            f"dc111_dc-111_{OBSERVATION_PERIOD}": f"{OBSERVATION_PERIOD}_id",
        }

        for table in expected_primary_keys:
            self.assertTrue(
                get_primary_key_for_sandbox_table(self.client, self.dataset_id,
                                                  table) ==
                expected_primary_keys[table])
