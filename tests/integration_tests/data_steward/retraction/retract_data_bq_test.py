"""
detail will be added here
"""

# Python imports
import os
from unittest import mock

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from common import (ACTIVITY_SUMMARY, JINJA_ENV, OBSERVATION, PERSON,
                    UNIONED_EHR)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from retraction.retract_data_bq import (NONE_STR, RETRACTION_ONLY_EHR,
                                        RETRACTION_RDR_EHR)
from retraction import retract_data_bq as rbq

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
(1, 0, 2001, 0, 0), (2, 0, 2001, 0, 0), 
(3, 0, 2001, 0, 0), (4, 0, 2001, 0, 0),
(5, 0, 2001, 0, 0),
(101, 0, 2001, 0, 0), (102, 0, 2001, 0, 0), 
(103, 0, 2001, 0, 0), (104, 0, 2001, 0, 0),
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

MAPPING_OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}._mapping_observation` 
(observation_id, src_hpo_id)
VALUES
(111, 'rdr'), (112, 'rdr'), 
(121, 'rdr'), (122, 'rdr'), 
(131, 'fake'), (132, 'fake'), 
(141, 'fake'), (142, 'fake'), 
(151, 'foo'), (152, 'foo')
""")

OBSERVATION_EXT_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation_ext` 
(observation_id, src_id)
VALUES
(1111, 'PPI/PM'), (1112, 'PPI/PM'), 
(1121, 'PPI/PM'), (1122, 'PPI/PM'), 
(1131, 'EHR 130'), (1132, 'EHR 130'), 
(1141, 'EHR 130'), (1142, 'EHR 130'),
(1151, 'EHR 150'), (1152, 'EHR 150')
""")

ACTIVITY_SUMMARY_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.activity_summary` 
(person_id)
VALUES
(1), (2), (3), (4), (5), (101), (102), (103), (104), (105)
""")

# TODO add DEATH table


class RetractDataBqTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.rdr_dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.ehr_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.unioned_ehr_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.combined_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.fitbit_dataset_id = os.environ.get('FITBIT_DATSET_ID')
        cls.deid_dataset_id = os.environ.get('COMBINED_DEID_DATASET_ID')
        cls.sandbox_id = f"{os.environ.get('BIGQUERY_DATASET_ID')}_sandbox"

        cls.lookup_table_id = 'pid_rid_to_retract'
        cls.omop_datasets = [
            cls.rdr_dataset_id, cls.unioned_ehr_dataset_id,
            cls.combined_dataset_id, cls.deid_dataset_id
        ]

        cls.fq_table_names = [
            f'{cls.project_id}.{dataset}.{PERSON}'
            for dataset in cls.omop_datasets
        ] + [
            f'{cls.project_id}.{dataset}.{OBSERVATION}'
            for dataset in cls.omop_datasets
        ] + [
            f'{cls.project_id}.{cls.combined_dataset_id}._mapping_{OBSERVATION}',
            f'{cls.project_id}.{cls.fitbit_dataset_id}.{ACTIVITY_SUMMARY}',
            f'{cls.project_id}.{cls.deid_dataset_id}.{ACTIVITY_SUMMARY}',
            f'{cls.project_id}.{cls.deid_dataset_id}.{OBSERVATION}_ext',
        ]

        cls.fq_sandbox_table_names = [
            #f'{cls.project_id}.{cls.ehr_dataset_id}.fake_{PERSON}',
            #f'{cls.project_id}.{cls.ehr_dataset_id}.fake_{OBSERVATION}',
            #f'{cls.project_id}.{cls.ehr_dataset_id}.unioned_ehr_{PERSON}',
            #f'{cls.project_id}.{cls.ehr_dataset_id}.unioned_ehr_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.lookup_table_id}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.rdr_dataset_id}_{PERSON}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.rdr_dataset_id}_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.unioned_ehr_dataset_id}_{PERSON}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.unioned_ehr_dataset_id}_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.combined_dataset_id}_{PERSON}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.combined_dataset_id}_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.deid_dataset_id}_{PERSON}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.deid_dataset_id}_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.ehr_dataset_id}_fake_{PERSON}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.ehr_dataset_id}_fake_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.ehr_dataset_id}_unioned_ehr_{PERSON}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.ehr_dataset_id}_unioned_ehr_{OBSERVATION}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.deid_dataset_id}_{ACTIVITY_SUMMARY}',
            f'{cls.project_id}.{cls.sandbox_id}.retract_{cls.fitbit_dataset_id}_{ACTIVITY_SUMMARY}',
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        queries = []

        for dataset in self.omop_datasets:
            insert_observation = OBSERVATION_TMPL.render(
                project=self.project_id, dataset=dataset, table=OBSERVATION)
            insert_person = PERSON_TMPL.render(project=self.project_id,
                                               dataset=dataset,
                                               table=PERSON)
            queries.extend([insert_observation, insert_person])

        # insert_observation = OBSERVATION_TMPL.render(
        #     project=self.project_id,
        #     dataset=self.ehr_dataset_id,
        #     table=f'fake_{OBSERVATION}')
        # insert_person = PERSON_TMPL.render(project=self.project_id,
        #                                    dataset=self.ehr_dataset_id,
        #                                    table=f'fake_{PERSON}')
        # queries.extend([insert_observation, insert_person])

        # insert_observation = OBSERVATION_TMPL.render(
        #     project=self.project_id,
        #     dataset=self.ehr_dataset_id,
        #     table=f'unioned_ehr_{OBSERVATION}')
        # insert_person = PERSON_TMPL.render(project=self.project_id,
        #                                    dataset=self.ehr_dataset_id,
        #                                    table=f'unioned_ehr_{PERSON}')
        # queries.extend([insert_observation, insert_person])

        insert_mapping_observation = MAPPING_OBSERVATION_TMPL.render(
            project=self.project_id, dataset=self.combined_dataset_id)
        queries.extend([insert_mapping_observation])

        insert_observation_ext = OBSERVATION_EXT_TMPL.render(
            project=self.project_id, dataset=self.deid_dataset_id)
        queries.extend([insert_observation_ext])

        create_lookup = CREATE_LOOKUP_TMPL.render(project=self.project_id,
                                                  dataset=self.sandbox_id)
        queries.extend([create_lookup])

        insert_lookup = LOOKUP_TMPL.render(project=self.project_id,
                                           dataset=self.sandbox_id)
        queries.extend([insert_lookup])

        insert_activity_summary = ACTIVITY_SUMMARY_TMPL.render(
            project=self.project_id, dataset=self.fitbit_dataset_id)
        queries.extend([insert_activity_summary])

        insert_activity_summary = ACTIVITY_SUMMARY_TMPL.render(
            project=self.project_id, dataset=self.deid_dataset_id)
        queries.extend([insert_activity_summary])

        self.load_test_data(queries)

    @mock.patch('retraction.retract_data_bq.is_unioned_dataset')
    @mock.patch('retraction.retract_utils.get_dataset_type')
    def test_unioned_ehr_rdr_and_ehr(self, mock_get_dataset_type,
                                     mock_is_unioned_dataset):
        """
        a
        """
        mock_get_dataset_type.return_value = UNIONED_EHR
        mock_is_unioned_dataset.return_value = True

        rbq.run_bq_retraction(self.project_id, self.sandbox_id,
                              self.lookup_table_id, NONE_STR,
                              [self.unioned_ehr_dataset_id], RETRACTION_RDR_EHR,
                              False, self.client)

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.unioned_ehr_dataset_id}.{PERSON}',
            ['person_id'], [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.unioned_ehr_dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.unioned_ehr_dataset_id}.{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.unioned_ehr_dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock.patch('retraction.retract_data_bq.is_unioned_dataset')
    @mock.patch('retraction.retract_utils.get_dataset_type')
    def test_unioned_ehr_only_ehr(self, mock_get_dataset_type,
                                  mock_is_unioned_dataset):
        """
        a
        """
        mock_get_dataset_type.return_value = UNIONED_EHR
        mock_is_unioned_dataset.return_value = True

        rbq.run_bq_retraction(self.project_id, self.sandbox_id,
                              self.lookup_table_id, NONE_STR,
                              [self.unioned_ehr_dataset_id],
                              RETRACTION_ONLY_EHR, False, self.client)

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.unioned_ehr_dataset_id}.{PERSON}',
            ['person_id'], [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.unioned_ehr_dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.unioned_ehr_dataset_id}.{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.unioned_ehr_dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock.patch('retraction.retract_data_bq.is_combined_dataset')
    @mock.patch('retraction.retract_utils.get_dataset_type')
    def test_combined_ehr_rdr_and_ehr(self, mock_get_dataset_type,
                                      mock_is_combined_dataset):
        """
        a
        """
        mock_get_dataset_type.return_value = UNIONED_EHR
        mock_is_combined_dataset.return_value = True

        rbq.run_bq_retraction(self.project_id, self.sandbox_id,
                              self.lookup_table_id, NONE_STR,
                              [self.combined_dataset_id], RETRACTION_RDR_EHR,
                              False, self.client)

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.combined_dataset_id}.{PERSON}',
            ['person_id'], [1, 3, 5, 101, 102, 103, 104, 105])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.combined_dataset_id}_{PERSON}',
            ['person_id'], [2, 4])

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.combined_dataset_id}.{OBSERVATION}',
            ['observation_id'], [
                111, 112, 131, 132, 151, 152, 1111, 1112, 1121, 1122, 1131,
                1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.combined_dataset_id}_{OBSERVATION}',
            ['observation_id'], [121, 122, 141, 142])

    @mock.patch('retraction.retract_data_bq.is_combined_dataset')
    @mock.patch('retraction.retract_utils.get_dataset_type')
    def test_combined_ehr_only_ehr(self, mock_get_dataset_type,
                                   mock_is_combined_dataset):
        """
        a
        """
        mock_get_dataset_type.return_value = UNIONED_EHR
        mock_is_combined_dataset.return_value = True

        rbq.run_bq_retraction(self.project_id, self.sandbox_id,
                              self.lookup_table_id, NONE_STR,
                              [self.combined_dataset_id], RETRACTION_ONLY_EHR,
                              False, self.client)

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.combined_dataset_id}.{PERSON}',
            ['person_id'], [1, 2, 3, 4, 5, 101, 102, 103, 104, 105])

        # TODO add table_not_found assertion
        # self.assertRowIDsMatch(
        #     f'{self.project_id}.{self.sandbox_id}.retract_{self.combined_dataset_id}_{PERSON}',
        #     ['person_id'], [])

        self.assertRowIDsMatch(
            f'{self.project_id}.{self.combined_dataset_id}.{OBSERVATION}',
            ['observation_id'], [
                111, 112, 121, 122, 131, 132, 151, 152, 1111, 1112, 1121, 1122,
                1131, 1132, 1141, 1142, 1151, 1152
            ])
        self.assertRowIDsMatch(
            f'{self.project_id}.{self.sandbox_id}.retract_{self.combined_dataset_id}_{OBSERVATION}',
            ['observation_id'], [141, 142])
