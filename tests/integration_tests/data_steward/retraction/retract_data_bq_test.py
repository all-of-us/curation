"""
Integration test for BigQuery retraction.
"""

# Python imports
import os
from unittest import mock

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from common import (ACTIVITY_SUMMARY, COMBINED, DEID, EHR, FITBIT, JINJA_ENV,
                    OBSERVATION, PERSON, RDR, UNIONED_EHR)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from retraction.retract_data_bq import (NONE_STR, RETRACTION_ONLY_EHR,
                                        RETRACTION_RDR_EHR, run_bq_retraction)

CREATE_LOOKUP_TMPL = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{dataset}}.pid_rid_to_retract` 
(person_id INT64, research_id INT64)
""")

LOOKUP_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.pid_rid_to_retract` 
    (person_id, research_id)
VALUES
    (2, 102), (4, 104)
""")

PERSON_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}` 
    (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
    (1, 0, 2001, 0, 0), 
    (2, 0, 2001, 0, 0), 
    (3, 0, 2001, 0, 0), 
    (4, 0, 2001, 0, 0),
    (5, 0, 2001, 0, 0),
    (101, 0, 2001, 0, 0), 
    (102, 0, 2001, 0, 0), 
    (103, 0, 2001, 0, 0), 
    (104, 0, 2001, 0, 0),
    (105, 0, 2001, 0, 0)
""")

OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{table}}` 
    (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
VALUES
    (111, 1, 0, date('2021-01-01'), 0), (112, 1, 0, date('2021-01-01'), 0), 
    (121, 2, 0, date('2021-01-01'), 0), (122, 2, 0, date('2021-01-01'), 0), 
    (131, 3, 0, date('2021-01-01'), 0), (132, 3, 0, date('2021-01-01'), 0), 
    (141, 4, 0, date('2021-01-01'), 0), (142, 4, 0, date('2021-01-01'), 0),
    (151, 5, 0, date('2021-01-01'), 0), (152, 5, 0, date('2021-01-01'), 0),
    (1111, 101, 0, date('2021-01-01'), 0), (1112, 101, 0, date('2021-01-01'), 0),
    (1121, 102, 0, date('2021-01-01'), 0), (1122, 102, 0, date('2021-01-01'), 0),
    (1131, 103, 0, date('2021-01-01'), 0), (1132, 103, 0, date('2021-01-01'), 0), 
    (1141, 104, 0, date('2021-01-01'), 0), (1142, 104, 0, date('2021-01-01'), 0),
    (1151, 105, 0, date('2021-01-01'), 0), (1152, 105, 0, date('2021-01-01'), 0)
""")

EHR_TMPL = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{dataset}}.{{table}}` 
AS SELECT * FROM `{{project}}.{{source_dataset}}.{{source_table}}` 
""")

MAPPING_OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}._mapping_observation` 
    (observation_id, src_hpo_id)
VALUES
    (111, 'rdr'), 
    (112, 'rdr'), 
    (121, 'rdr'), 
    (122, 'rdr'), 
    (131, 'fake'), 
    (132, 'fake'), 
    (141, 'fake'), 
    (142, 'fake'), 
    (151, 'foo'), 
    (152, 'foo')
""")

OBSERVATION_EXT_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation_ext` 
    (observation_id, src_id)
VALUES
    (1111, 'PPI/PM'), 
    (1112, 'PPI/PM'), 
    (1121, 'PPI/PM'), 
    (1122, 'PPI/PM'), 
    (1131, 'EHR 130'), 
    (1132, 'EHR 130'), 
    (1141, 'EHR 130'), 
    (1142, 'EHR 130'),
    (1151, 'EHR 150'), 
    (1152, 'EHR 150')
""")

ACTIVITY_SUMMARY_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.activity_summary` 
    (person_id)
VALUES
    (1), 
    (2), 
    (3), 
    (4), 
    (5), 
    (101), 
    (102),
    (103),
    (104),
    (105)
""")


def mock_patch_decorator_bundle(*decorators):
    """abc
    """

    def _chain(patch):
        for dec in reversed(decorators):
            patch = dec(patch)
        return patch

    return _chain


mock_patch_bundle = mock_patch_decorator_bundle(
    mock.patch('retraction.retract_data_bq.is_fitbit_dataset'),
    mock.patch('retraction.retract_data_bq.is_deid_dataset'),
    mock.patch('retraction.retract_data_bq.is_combined_dataset'),
    mock.patch('retraction.retract_data_bq.is_unioned_dataset'),
    mock.patch('retraction.retract_data_bq.is_ehr_dataset'),
    mock.patch('retraction.retract_data_bq.is_rdr_dataset'),
    mock.patch('retraction.retract_utils.get_dataset_type'))


class RetractDataBqTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.rdr_id = os.environ.get('RDR_DATASET_ID')
        cls.ehr_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.unioned_ehr_id = os.environ.get('UNIONED_DATASET_ID')
        cls.combined_id = os.environ.get('COMBINED_DATASET_ID')
        cls.deid_id = os.environ.get('COMBINED_DEID_DATASET_ID')
        cls.fitbit_id = f"{os.environ.get('BIGQUERY_DATASET_ID')}_fitbit"
        cls.sandbox_id = f"{os.environ.get('BIGQUERY_DATASET_ID')}_sandbox"

        cls.hpo_id = 'fake'
        cls.lookup_table_id = 'pid_rid_to_retract'

        # omop tables (excluding EHR dataset)
        for dataset in [
                cls.rdr_id, cls.unioned_ehr_id, cls.combined_id, cls.deid_id
        ]:
            cls.fq_table_names.extend([
                f'{cls.project_id}.{dataset}.{PERSON}',
                f'{cls.project_id}.{dataset}.{OBSERVATION}'
            ])
            cls.fq_sandbox_table_names.extend([
                f'{cls.project_id}.{cls.sandbox_id}.retract_{dataset}_{PERSON}',
                f'{cls.project_id}.{cls.sandbox_id}.retract_{dataset}_{OBSERVATION}'
            ])

        # mapping tables/ ext tables
        cls.fq_table_names.extend([
            f'{cls.project_id}.{cls.combined_id}._mapping_{OBSERVATION}',
            f'{cls.project_id}.{cls.deid_id}.{OBSERVATION}_ext'
        ])

        # fitbit tables
        for dataset in [cls.deid_id, cls.fitbit_id]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{dataset}.{ACTIVITY_SUMMARY}')
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.retract_{dataset}_{ACTIVITY_SUMMARY}'
            )

        # tables for EHR dataset
        for hpo in [cls.hpo_id, UNIONED_EHR]:
            cls.fq_sandbox_table_names.extend([
                f'{cls.project_id}.{cls.ehr_id}.{hpo}_{PERSON}',
                f'{cls.project_id}.{cls.ehr_id}.{hpo}_{OBSERVATION}',
                f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.ehr_id}_{hpo}_{PERSON}',
                f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.ehr_id}_{hpo}_{OBSERVATION}'
            ])

        # lookup table
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{cls.lookup_table_id}')

        super().setUpClass()

    def setUp(self):
        super().setUp()

        queries = []

        # omop tables (excluding EHR dataset)
        for dataset in [
                self.rdr_id, self.unioned_ehr_id, self.combined_id, self.deid_id
        ]:
            insert_person = PERSON_TMPL.render(project=self.project_id,
                                               dataset=dataset,
                                               table=PERSON)
            insert_observation = OBSERVATION_TMPL.render(
                project=self.project_id, dataset=dataset, table=OBSERVATION)
            queries.extend([insert_person, insert_observation])

        # mapping tables/ ext tables
        insert_mapping_observation = MAPPING_OBSERVATION_TMPL.render(
            project=self.project_id, dataset=self.combined_id)
        insert_observation_ext = OBSERVATION_EXT_TMPL.render(
            project=self.project_id, dataset=self.deid_id)
        queries.extend([insert_mapping_observation, insert_observation_ext])

        # fitbit tables
        for dataset in [self.deid_id, self.fitbit_id]:
            insert_activity_summary = ACTIVITY_SUMMARY_TMPL.render(
                project=self.project_id, dataset=dataset)
            queries.extend([insert_activity_summary])

        # tables for EHR dataset
        for hpo in [self.hpo_id, UNIONED_EHR]:
            create_person_ehr = EHR_TMPL.render(project=self.project_id,
                                                dataset=self.ehr_id,
                                                table=f'{hpo}_{PERSON}',
                                                source_dataset=self.rdr_id,
                                                source_table=PERSON)
            create_observation_ehr = EHR_TMPL.render(
                project=self.project_id,
                dataset=self.ehr_id,
                table=f'{hpo}_{OBSERVATION}',
                source_dataset=self.rdr_id,
                source_table=OBSERVATION)
            queries.extend([create_person_ehr, create_observation_ehr])

        # lookup table
        create_lookup = CREATE_LOOKUP_TMPL.render(project=self.project_id,
                                                  dataset=self.sandbox_id)
        insert_lookup = LOOKUP_TMPL.render(project=self.project_id,
                                           dataset=self.sandbox_id)
        queries.extend([create_lookup, insert_lookup])

        self.load_test_data(queries)

    @mock_patch_bundle
    def test_unioned_ehr_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                                     mock_is_ehr, mock_is_unioned,
                                     mock_is_combined, mock_is_deid,
                                     mock_is_fitbit):
        """
        Test for unioned ehr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on person_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = UNIONED_EHR
        mock_is_unioned.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.unioned_ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'],
                               [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{OBSERVATION}', ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock_patch_bundle
    def test_unioned_ehr_only_ehr(self, mock_get_dataset_type, mock_is_rdr,
                                  mock_is_ehr, mock_is_unioned,
                                  mock_is_combined, mock_is_deid,
                                  mock_is_fitbit):
        """
        Test for unioned ehr dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retracts records based on person_ids in the lookup table.
        * Same result as retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = UNIONED_EHR
        mock_is_unioned.return_value = True
        mock_is_combined.return_value = False
        mock_is_deid.return_value = False
        mock_is_fitbit.return_value = False

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.unioned_ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_ONLY_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'],
                               [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{OBSERVATION}', ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock_patch_bundle
    def test_combined_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                                  mock_is_ehr, mock_is_unioned,
                                  mock_is_combined, mock_is_deid,
                                  mock_is_fitbit):
        """
        Test for combined dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on person_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = COMBINED
        mock_is_combined.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.combined_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'],
                               [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{OBSERVATION}', ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock_patch_bundle
    def test_combined_only_ehr(self, mock_get_dataset_type, mock_is_rdr,
                               mock_is_ehr, mock_is_unioned, mock_is_combined,
                               mock_is_deid, mock_is_fitbit):
        """
        Test for combined dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Retracts records based on person_ids in the lookup table and
        src_hpo_id in the mapping table.
        person table is not retracted because all the data is from RDR.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = COMBINED
        mock_is_combined.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.combined_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_ONLY_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'],
                               [1, 2, 3, 4, 5, 101, 102, 103, 104, 105])
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.retract_{dataset_id}_{PERSON}'
        )

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{OBSERVATION}', ['observation_id'], [
                111, 112, 121, 122, 131, 132, 151, 152, 1111, 1112, 1121, 1122,
                1131, 1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [141, 142])

    @mock_patch_bundle
    def test_deid_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                              mock_is_ehr, mock_is_unioned, mock_is_combined,
                              mock_is_deid, mock_is_fitbit):
        """
        Test for deid dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on research_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = DEID
        mock_is_deid.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.deid_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'], [1, 2, 3, 4, 5, 101, 103, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], [102, 104])

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{OBSERVATION}',
                               ['observation_id'], [
                                   111, 112, 121, 122, 131, 132, 141, 142, 151,
                                   152, 1111, 1112, 1131, 1132, 1151, 1152
                               ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [1121, 1122, 1141, 1142])

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], [1, 2, 3, 4, 5, 101, 103, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}',
            ['person_id'], [102, 104])

    @mock_patch_bundle
    def test_deid_only_ehr(self, mock_get_dataset_type, mock_is_rdr,
                           mock_is_ehr, mock_is_unioned, mock_is_combined,
                           mock_is_deid, mock_is_fitbit):
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
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = DEID
        mock_is_deid.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.deid_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_ONLY_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'],
                               [1, 2, 3, 4, 5, 101, 102, 103, 104, 105])
        self.assertTableDoesNotExist(
            f'{self.project_id}.{self.sandbox_id}.retract_{dataset_id}_{PERSON}'
        )

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{OBSERVATION}', ['observation_id'], [
                111, 112, 121, 122, 131, 132, 141, 142, 151, 152, 1111, 1112,
                1121, 1122, 1131, 1132, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [1141, 1142])

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'],
                               [1, 2, 3, 4, 5, 101, 102, 103, 104, 105])
        self.assertTableDoesNotExist(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}'
        )

    @mock_patch_bundle
    def test_rdr_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                             mock_is_ehr, mock_is_unioned, mock_is_combined,
                             mock_is_deid, mock_is_fitbit):
        """
        Test for rdr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        Retracts records based on person_ids in the lookup table.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = RDR
        mock_is_rdr.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.rdr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{PERSON}',
                               ['person_id'],
                               [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{OBSERVATION}', ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock_patch_bundle
    def test_rdr_only_ehr(self, mock_get_dataset_type, mock_is_rdr, mock_is_ehr,
                          mock_is_unioned, mock_is_combined, mock_is_deid,
                          mock_is_fitbit):
        """
        Test for rdr dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Throws an error since RDR dataset cannot be retracted when 'only_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = RDR
        mock_is_rdr.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.rdr_id

        with self.assertRaises(ValueError):
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE_STR, [dataset_id], RETRACTION_ONLY_EHR,
                              False, self.client)

    @mock_patch_bundle
    def test_ehr_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                             mock_is_ehr, mock_is_unioned, mock_is_combined,
                             mock_is_deid, mock_is_fitbit):
        """
        Test for ehr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        EHR dataset's table naming convention is different from other datasets.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = EHR
        mock_is_ehr.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          self.hpo_id, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{PERSON}', ['person_id'],
            [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{PERSON}', ['person_id'],
            [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock_patch_bundle
    def test_ehr_ehr_only_ehr(self, mock_get_dataset_type, mock_is_rdr,
                              mock_is_ehr, mock_is_unioned, mock_is_combined,
                              mock_is_deid, mock_is_fitbit):
        """
        Test for ehr dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        EHR dataset's table naming convention is different from other datasets.
        * Same result as retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = EHR
        mock_is_ehr.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id, 'fake',
                          [dataset_id], RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{PERSON}', ['person_id'],
            [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{PERSON}', ['person_id'],
            [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{self.hpo_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

        self.assertRowIDsMatch(
            f'{project_id}.{dataset_id}.{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{UNIONED_EHR}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock_patch_bundle
    def test_ehr_without_hpo_id(self, mock_get_dataset_type, mock_is_rdr,
                                mock_is_ehr, mock_is_unioned, mock_is_combined,
                                mock_is_deid, mock_is_fitbit):
        """
        Test for ehr dataset.
        Throws a ValueError when hpo_id is not specified.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = EHR
        mock_is_ehr.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.ehr_id

        with self.assertRaises(ValueError):
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE_STR, [dataset_id], RETRACTION_ONLY_EHR,
                              False, self.client)

    @mock_patch_bundle
    def test_fitbit_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                                mock_is_ehr, mock_is_unioned, mock_is_combined,
                                mock_is_deid, mock_is_fitbit):
        """
        Test for fitbit dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = FITBIT
        mock_is_fitbit.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], [1,3,5, 101, 102,103,104,105])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}',
            ['person_id'], [2,4])

    @mock_patch_bundle
    def test_fitbit_only_ehr(self, mock_get_dataset_type, mock_is_rdr,
                             mock_is_ehr, mock_is_unioned, mock_is_combined,
                             mock_is_deid, mock_is_fitbit):
        """
        Test for fitbit dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Throws an error since fitbit dataset cannot be retracted when 'only_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = FITBIT
        mock_is_fitbit.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        with self.assertRaises(ValueError):
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE_STR, [dataset_id], RETRACTION_ONLY_EHR,
                              False, self.client)

    @mock_patch_bundle
    def test_deid_fitbit_rdr_and_ehr(self, mock_get_dataset_type, mock_is_rdr,
                                     mock_is_ehr, mock_is_unioned,
                                     mock_is_combined, mock_is_deid,
                                     mock_is_fitbit):
        """
        Test for fitbit dataset.
        run_bq_retraction with retraction_type = 'rdr_and_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = FITBIT
        mock_is_deid.return_value = True
        mock_is_fitbit.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                          NONE_STR, [dataset_id], RETRACTION_RDR_EHR, False,
                          self.client)

        self.assertRowIDsMatch(f'{project_id}.{dataset_id}.{ACTIVITY_SUMMARY}',
                               ['person_id'], [1, 2, 3, 4, 5, 101, 103, 105])

        self.assertRowIDsMatch(
            f'{project_id}.{sandbox_id}.retract_{dataset_id}_{ACTIVITY_SUMMARY}',
            ['person_id'], [102, 104])

    @mock_patch_bundle
    def test_deid_fitbit_only_ehr(self, mock_get_dataset_type, mock_is_rdr,
                                  mock_is_ehr, mock_is_unioned,
                                  mock_is_combined, mock_is_deid,
                                  mock_is_fitbit):
        """
        Test for deid fitbit dataset.
        run_bq_retraction with retraction_type = 'only_ehr'.
        Throws an error since fitbit dataset cannot be retracted when 'only_ehr'.
        """
        for mock_ in [
                mock_is_rdr, mock_is_ehr, mock_is_unioned, mock_is_combined,
                mock_is_deid, mock_is_fitbit
        ]:
            mock_.return_value = False

        mock_get_dataset_type.return_value = FITBIT
        mock_is_deid.return_value = True
        mock_is_fitbit.return_value = True

        project_id, sandbox_id, dataset_id = self.project_id, self.sandbox_id, self.fitbit_id

        with self.assertRaises(ValueError):
            run_bq_retraction(project_id, sandbox_id, self.lookup_table_id,
                              NONE_STR, [dataset_id], RETRACTION_ONLY_EHR,
                              False, self.client)
