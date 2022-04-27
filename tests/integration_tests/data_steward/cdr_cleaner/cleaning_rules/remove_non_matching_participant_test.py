"""Integration test
"""

# Python Imports
import os

# Third party imports
from dateutil.parser import parse
from google.cloud.bigquery import Table, TimePartitioning, TimePartitioningType

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import (
    RemoveNonMatchingParticipant, KEY_FIELDS, TICKET_NUMBER)
from common import JINJA_ENV, IDENTITY_MATCH, LOCATION, OBSERVATION, PARTICIPANT_MATCH, PERSON, PII_ADDRESS
from validation.participants.create_update_drc_id_match_table import (
    create_drc_validation_table, populate_validation_table)
from tests import test_util
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
import resources

# HPO_1 and HPO_2 have both participant_match and identity_match.
# HPO_3 has only identity_match.
# HPO_4 has only participant_match.
HPO_1, HPO_2, HPO_3, HPO_4 = 'fake', 'pitt', 'nyc', 'chs'

POPULATE_STATEMENTS = {
    PERSON:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
        VALUES
        (1, 0, 1991, 0, 0),
        (2, 0, 1992, 0, 0),
        (3, 0, 1993, 0, 0),
        (4, 0, 1994, 0, 0),
        (5, 0, 1995, 0, 0)
        """),
    OBSERVATION:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
        VALUES
        (11, 1, 0, date('2022-01-01'), 0),
        (12, 2, 0, date('2022-01-02'), 0),
        (13, 3, 0, date('2022-01-03'), 0),
        (14, 4, 0, date('2022-01-04'), 0),
        (15, 5, 0, date('2022-01-05'), 0)
        """),
    PII_ADDRESS:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (person_id, location_id)
        VALUES
        (1, 101),
        (2, 102),
        (3, 103),
        (4, 104),
        (5, 105)
        """),
    LOCATION:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (location_id, address_1, address_2, city, state, zip)
        VALUES
        (101, 'xyz', '', 'New York', 'NY', '12345'),
        (102, 'xyz', '', 'New York', 'NY', '12345'),
        (103, 'xyz', '', 'New York', 'NY', '12345'),
        (104, 'xyz', '', 'New York', 'NY', '12345'),
        (105, 'xyz', '', 'New York', 'NY', '12345')
        """),
    PARTICIPANT_MATCH:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}`
        (person_id, algorithm_validation, manual_validation)
        VALUES
        (1, 'yes', 'yes'),
        (2, 'yes', 'yes'),
        (3, 'yes', 'yes'),
        (4, 'yes', 'yes'),
        (5, 'yes', 'yes')
        """),
    IDENTITY_MATCH:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        (person_id, first_name, middle_name, last_name, phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, algorithm)
        VALUES
        (1, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (2, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (3, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (4, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (5, 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'no_match', 'yes')
        """)
}


class RemoveNonMatchingParticipantTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.drc_ops_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {'drc_ops_dataset_id': cls.drc_ops_dataset_id}

        test_util.delete_all_tables(cls.dataset_id)
        test_util.delete_all_tables(cls.drc_ops_dataset_id)

        # Dict that associates tables and CDM tables that are based off of
        cls.fq_table_names_cdm = {
            f'{cls.project_id}.{cls.dataset_id}.{hpo_id}_{cdm_table}': cdm_table
            for hpo_id in [HPO_1, HPO_2, HPO_3, HPO_4]
            for cdm_table in [PERSON, OBSERVATION, PII_ADDRESS, LOCATION]
        }

        cls.fq_table_names = list(cls.fq_table_names_cdm.keys())

        # Set client and create datasets if not exist
        super().setUpClass()

        cls.rule_instance = RemoveNonMatchingParticipant(
            cls.project_id, cls.dataset_id, cls.sandbox_id,
            cls.drc_ops_dataset_id)

        for fq_table_name in cls.fq_table_names:

            schema = resources.fields_for(cls.fq_table_names_cdm[fq_table_name])
            table = Table(fq_table_name, schema=schema)
            table = cls.client.create_table(table, exists_ok=True)

            query = POPULATE_STATEMENTS[
                cls.fq_table_names_cdm[fq_table_name]].render(
                    fq_table_name=fq_table_name)
            job = cls.client.query(query)
            job.result()

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        cls.id_match_table_ids = [
            f'{hpo_id}_{IDENTITY_MATCH}' for hpo_id in [HPO_1, HPO_2, HPO_3]
        ]

        for id_match_table_id in cls.id_match_table_ids:
            create_drc_validation_table(cls.client, id_match_table_id,
                                        cls.drc_ops_dataset_id)

            populate_query = POPULATE_STATEMENTS[IDENTITY_MATCH].render(
                project_id=cls.project_id,
                dataset_id=cls.drc_ops_dataset_id,
                table_id=id_match_table_id)
            job = cls.client.query(populate_query)
            job.result()

        cls.participant_match_table_ids = [
            f'{hpo_id}_{PARTICIPANT_MATCH}' for hpo_id in [HPO_1, HPO_2, HPO_4]
        ]

        schema = resources.fields_for(PARTICIPANT_MATCH)
        for participant_match_table_id in cls.participant_match_table_ids:
            table = Table(
                f'{cls.project_id}.{cls.dataset_id}.{participant_match_table_id}',
                schema=schema)
            table = cls.client.create_table(table, exists_ok=True)

            populate_query = POPULATE_STATEMENTS[PARTICIPANT_MATCH].render(
                project_id=cls.project_id,
                dataset_id=cls.dataset_id,
                table_id=participant_match_table_id)
            job = cls.client.query(populate_query)
            job.result()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        pass

    def test_exist_participant_match(self):
        """_summary_
        """
        self.assertTrue(
            self.rule_instance.exist_participant_match(self.dataset_id, HPO_1))
        self.assertTrue(
            self.rule_instance.exist_participant_match(self.dataset_id, HPO_2))
        self.assertFalse(
            self.rule_instance.exist_participant_match(self.dataset_id, HPO_3))
        self.assertTrue(
            self.rule_instance.exist_participant_match(self.dataset_id, HPO_4))

    def test_exist_identity_match(self):
        """_summary_
        """
        self.assertTrue(
            self.rule_instance.exist_identity_match(
                self.client,
                f'{self.project_id}.{self.dataset_id}.{HPO_1}_{IDENTITY_MATCH}')
        )
        self.assertTrue(
            self.rule_instance.exist_identity_match(
                self.client,
                f'{self.project_id}.{self.dataset_id}.{HPO_2}_{IDENTITY_MATCH}')
        )
        self.assertTrue(
            self.rule_instance.exist_identity_match(
                self.client,
                f'{self.project_id}.{self.dataset_id}.{HPO_3}_{IDENTITY_MATCH}')
        )
        self.assertFalse(
            self.rule_instance.exist_identity_match(
                self.client,
                f'{self.project_id}.{self.dataset_id}.{HPO_4}_{IDENTITY_MATCH}')
        )

    def test_get_missing_criterion(self):
        """_summary_
        """
        self.assertEqual(
            self.rule_instance.get_missing_criterion(KEY_FIELDS),
            "CAST(first_name <> 'match' AS int64) + " +
            "CAST(last_name <> 'match' AS int64) + " +
            "CAST(birth_date <> 'match' AS int64)")

    def test_get_list_non_match_participants(self):
        """_summary_
        """
        self.assertEqual(
            self.rule_instance.get_list_non_match_participants(
                self.client, self.dataset_id, HPO_1), [5])
        self.assertEqual(
            self.rule_instance.get_list_non_match_participants(
                self.client, self.dataset_id, HPO_2), [5])
        self.assertEqual(
            self.rule_instance.get_list_non_match_participants(
                self.client, self.dataset_id, HPO_3), [5])
        self.assertEqual(
            self.rule_instance.get_list_non_match_participants(
                self.client, self.dataset_id, HPO_4), [])

    def test_remove_non_matching_participant(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        tables_and_counts = [
            {
                'name':
                    f'{HPO_1}_{PERSON}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_1}_{PERSON}',
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'race_concept_id', 'ethnicity_concept_id'
                ],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 0, 1991, 0, 0), (2, 0, 1992, 0, 0),
                                   (3, 0, 1993, 0, 0), (4, 0, 1994, 0, 0),
                                   (5, 0, 1995, 0, 0)]
            },
            {
                'name':
                    f'{HPO_2}_{PERSON}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_2}_{PERSON}',
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'race_concept_id', 'ethnicity_concept_id'
                ],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 0, 1991, 0, 0), (2, 0, 1992, 0, 0),
                                   (3, 0, 1993, 0, 0), (4, 0, 1994, 0, 0),
                                   (5, 0, 1995, 0, 0)]
            },
            {
                'name':
                    f'{HPO_3}_{PERSON}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_3}_{PERSON}',
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'race_concept_id', 'ethnicity_concept_id'
                ],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 0, 1991, 0, 0), (2, 0, 1992, 0, 0),
                                   (3, 0, 1993, 0, 0), (4, 0, 1994, 0, 0),
                                   (5, 0, 1995, 0, 0)]
            },
            {
                'name':
                    f'{HPO_4}_{PERSON}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_4}_{PERSON}',
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'race_concept_id', 'ethnicity_concept_id'
                ],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 0, 1991, 0, 0), (2, 0, 1992, 0, 0),
                                   (3, 0, 1993, 0, 0), (4, 0, 1994, 0, 0),
                                   (5, 0, 1995, 0, 0)]
            },
            {
                'name':
                    f'{HPO_1}_{OBSERVATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_1}_{OBSERVATION}',
                'fields': [
                    'observation_id', 'person_id', 'observation_concept_id',
                    'observation_date', 'observation_type_concept_id'
                ],
                'loaded_ids': [11, 12, 13, 14, 15],
                'cleaned_values': [(11, 1, 0, parse('2022-01-01').date(), 0),
                                   (12, 2, 0, parse('2022-01-02').date(), 0),
                                   (13, 3, 0, parse('2022-01-03').date(), 0),
                                   (14, 4, 0, parse('2022-01-04').date(), 0),
                                   (15, 5, 0, parse('2022-01-05').date(), 0)]
            },
            {
                'name':
                    f'{HPO_2}_{OBSERVATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_2}_{OBSERVATION}',
                'fields': [
                    'observation_id', 'person_id', 'observation_concept_id',
                    'observation_date', 'observation_type_concept_id'
                ],
                'loaded_ids': [11, 12, 13, 14, 15],
                'cleaned_values': [(11, 1, 0, parse('2022-01-01').date(), 0),
                                   (12, 2, 0, parse('2022-01-02').date(), 0),
                                   (13, 3, 0, parse('2022-01-03').date(), 0),
                                   (14, 4, 0, parse('2022-01-04').date(), 0),
                                   (15, 5, 0, parse('2022-01-05').date(), 0)]
            },
            {
                'name':
                    f'{HPO_3}_{OBSERVATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_3}_{OBSERVATION}',
                'fields': [
                    'observation_id', 'person_id', 'observation_concept_id',
                    'observation_date', 'observation_type_concept_id'
                ],
                'loaded_ids': [11, 12, 13, 14, 15],
                'cleaned_values': [(11, 1, 0, parse('2022-01-01').date(), 0),
                                   (12, 2, 0, parse('2022-01-02').date(), 0),
                                   (13, 3, 0, parse('2022-01-03').date(), 0),
                                   (14, 4, 0, parse('2022-01-04').date(), 0),
                                   (15, 5, 0, parse('2022-01-05').date(), 0)]
            },
            {
                'name':
                    f'{HPO_4}_{OBSERVATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_4}_{OBSERVATION}',
                'fields': [
                    'observation_id', 'person_id', 'observation_concept_id',
                    'observation_date', 'observation_type_concept_id'
                ],
                'loaded_ids': [11, 12, 13, 14, 15],
                'cleaned_values': [(11, 1, 0, parse('2022-01-01').date(), 0),
                                   (12, 2, 0, parse('2022-01-02').date(), 0),
                                   (13, 3, 0, parse('2022-01-03').date(), 0),
                                   (14, 4, 0, parse('2022-01-04').date(), 0),
                                   (15, 5, 0, parse('2022-01-05').date(), 0)]
            },
            {
                'name':
                    f'{HPO_1}_{PII_ADDRESS}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_1}_{PII_ADDRESS}',
                'fields': ['person_id', 'location_id'],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 101), (2, 102), (3, 103), (4, 104),
                                   (5, 105)]
            },
            {
                'name':
                    f'{HPO_2}_{PII_ADDRESS}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_2}_{PII_ADDRESS}',
                'fields': ['person_id', 'location_id'],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 101), (2, 102), (3, 103), (4, 104),
                                   (5, 105)]
            },
            {
                'name':
                    f'{HPO_3}_{PII_ADDRESS}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_3}_{PII_ADDRESS}',
                'fields': ['person_id', 'location_id'],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 101), (2, 102), (3, 103), (4, 104),
                                   (5, 105)]
            },
            {
                'name':
                    f'{HPO_4}_{PII_ADDRESS}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_4}_{PII_ADDRESS}',
                'fields': ['person_id', 'location_id'],
                'loaded_ids': [1, 2, 3, 4, 5],
                'cleaned_values': [(1, 101), (2, 102), (3, 103), (4, 104),
                                   (5, 105)]
            },
            {
                'name':
                    f'{HPO_1}_{LOCATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_1}_{LOCATION}',
                'fields': [
                    'location_id', 'address_1', 'address_2', 'city', 'state',
                    'zip'
                ],
                'loaded_ids': [101, 102, 103, 104, 105],
                'cleaned_values': [(101, 'xyz', '', 'New York', 'NY', '12345'),
                                   (102, 'xyz', '', 'New York', 'NY', '12345'),
                                   (103, 'xyz', '', 'New York', 'NY', '12345'),
                                   (104, 'xyz', '', 'New York', 'NY', '12345'),
                                   (105, 'xyz', '', 'New York', 'NY', '12345')]
            },
            {
                'name':
                    f'{HPO_2}_{LOCATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_2}_{LOCATION}',
                'fields': [
                    'location_id', 'address_1', 'address_2', 'city', 'state',
                    'zip'
                ],
                'loaded_ids': [101, 102, 103, 104, 105],
                'cleaned_values': [(101, 'xyz', '', 'New York', 'NY', '12345'),
                                   (102, 'xyz', '', 'New York', 'NY', '12345'),
                                   (103, 'xyz', '', 'New York', 'NY', '12345'),
                                   (104, 'xyz', '', 'New York', 'NY', '12345'),
                                   (105, 'xyz', '', 'New York', 'NY', '12345')]
            },
            {
                'name':
                    f'{HPO_3}_{LOCATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_3}_{LOCATION}',
                'fields': [
                    'location_id', 'address_1', 'address_2', 'city', 'state',
                    'zip'
                ],
                'loaded_ids': [101, 102, 103, 104, 105],
                'cleaned_values': [(101, 'xyz', '', 'New York', 'NY', '12345'),
                                   (102, 'xyz', '', 'New York', 'NY', '12345'),
                                   (103, 'xyz', '', 'New York', 'NY', '12345'),
                                   (104, 'xyz', '', 'New York', 'NY', '12345'),
                                   (105, 'xyz', '', 'New York', 'NY', '12345')]
            },
            {
                'name':
                    f'{HPO_4}_{LOCATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{HPO_4}_{LOCATION}',
                'fields': [
                    'location_id', 'address_1', 'address_2', 'city', 'state',
                    'zip'
                ],
                'loaded_ids': [101, 102, 103, 104, 105],
                'cleaned_values': [(101, 'xyz', '', 'New York', 'NY', '12345'),
                                   (102, 'xyz', '', 'New York', 'NY', '12345'),
                                   (103, 'xyz', '', 'New York', 'NY', '12345'),
                                   (104, 'xyz', '', 'New York', 'NY', '12345'),
                                   (105, 'xyz', '', 'New York', 'NY', '12345')]
            },
        ]

        self.default_test(tables_and_counts)

    def tearDown(self):
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        test_util.delete_all_tables(cls.dataset_id)
        test_util.delete_all_tables(cls.drc_ops_dataset_id)
