"""Integration test for RemoveNonMatchingParticipant
"""

# Python Imports
import os

# Third party imports
from dateutil.parser import parse
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import (
    RemoveNonMatchingParticipant, KEY_FIELDS)
from common import JINJA_ENV, IDENTITY_MATCH, OBSERVATION, PARTICIPANT_MATCH, PERSON
from validation.participants.create_update_drc_id_match_table import create_drc_validation_table
from tests import test_util
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
import resources

HPO_1, HPO_2, HPO_3, HPO_4 = 'fake', 'pitt', 'nyc', 'chs'

POPULATE_STATEMENTS = {
    PERSON:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
        VALUES
        (101, 0, 1991, 0, 0),
        (102, 0, 1992, 0, 0),
        (103, 0, 1993, 0, 0),
        (104, 0, 1994, 0, 0),
        (201, 0, 1991, 0, 0),
        (202, 0, 1992, 0, 0),
        (203, 0, 1993, 0, 0),
        (204, 0, 1994, 0, 0),
        (301, 0, 1991, 0, 0),
        (302, 0, 1992, 0, 0),
        (303, 0, 1993, 0, 0),
        (304, 0, 1994, 0, 0),
        (401, 0, 1991, 0, 0),
        (402, 0, 1992, 0, 0),
        (403, 0, 1993, 0, 0),
        (404, 0, 1994, 0, 0)
        """),
    OBSERVATION:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
        VALUES
        (1001, 101, 0, date('2022-01-01'), 0),
        (1002, 102, 0, date('2022-01-02'), 0),
        (1003, 103, 0, date('2022-01-03'), 0),
        (1004, 104, 0, date('2022-01-04'), 0),
        (2001, 201, 0, date('2022-01-01'), 0),
        (2002, 202, 0, date('2022-01-02'), 0),
        (2003, 203, 0, date('2022-01-03'), 0),
        (2004, 204, 0, date('2022-01-04'), 0),
        (3001, 301, 0, date('2022-01-01'), 0),
        (3002, 302, 0, date('2022-01-02'), 0),
        (3003, 303, 0, date('2022-01-03'), 0),
        (3004, 304, 0, date('2022-01-04'), 0),
        (4001, 401, 0, date('2022-01-01'), 0),
        (4002, 402, 0, date('2022-01-02'), 0),
        (4003, 403, 0, date('2022-01-03'), 0),
        (4004, 404, 0, date('2022-01-04'), 0)
        """),
    f'{HPO_1}_{IDENTITY_MATCH}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{validation_dataset_id}}.{{table_id}}` 
        (person_id, first_name, middle_name, last_name, phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, algorithm)
        VALUES
        (101, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (102, 'missing', 'match', 'match', 'missing', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (103, 'missing', 'match', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (104, 'missing', 'match', 'match', 'missing', 'missing', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'yes')
        """),
    f'{HPO_2}_{IDENTITY_MATCH}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{validation_dataset_id}}.{{table_id}}` 
        (person_id, first_name, middle_name, last_name, phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, algorithm)
        VALUES
        (201, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (202, 'missing', 'match', 'match', 'missing', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (203, 'missing', 'match', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (204, 'missing', 'match', 'match', 'missing', 'missing', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'yes')
        """),
    f'{HPO_3}_{IDENTITY_MATCH}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{validation_dataset_id}}.{{table_id}}` 
        (person_id, first_name, middle_name, last_name, phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, algorithm)
        VALUES
        (301, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (302, 'missing', 'match', 'match', 'missing', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (303, 'missing', 'match', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (304, 'missing', 'match', 'match', 'missing', 'missing', 'missing', 'match', 'match', 'match', 'match', 'match', 'match', 'yes')
        """),
    f'{HPO_1}_{PARTICIPANT_MATCH}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{ehr_dataset_id}}.{{table_id}}`
        (person_id, algorithm_validation, manual_validation)
        VALUES
        (101, 'yes', 'no'),
        (102, 'no', 'yes'),
        (103, 'yes', 'no'),
        (104, 'no', 'yes')
        """),
    f'{HPO_2}_{PARTICIPANT_MATCH}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{ehr_dataset_id}}.{{table_id}}`
        (person_id, algorithm_validation, manual_validation)
        VALUES
        (201, 'yes', 'yes'),
        (202, 'yes', 'no'),
        (203, 'no', 'yes'),
        (204, 'no', 'no')
        """),
    f'{HPO_4}_{PARTICIPANT_MATCH}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{ehr_dataset_id}}.{{table_id}}`
        (person_id, algorithm_validation, manual_validation)
        VALUES
        (401, 'no', 'no'),
        (402, 'no', 'no'),
        (403, 'no', 'no'),
        (404, 'no', 'no')
        """),
}


class RemoveNonMatchingParticipantTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.ehr_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.validation_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {
            'validation_dataset_id': cls.validation_dataset_id,
            'ehr_dataset_id': cls.ehr_dataset_id
        }

        cls.fq_table_names = []
        for cdm_table in [PERSON, OBSERVATION]:
            fq_table_name = f'{cls.project_id}.{cls.dataset_id}.{cdm_table}'
            cls.fq_table_names.append(fq_table_name)

        # Set client and create datasets if not exist
        super().setUpClass()

        test_util.delete_all_tables(cls.dataset_id)
        test_util.delete_all_tables(cls.ehr_dataset_id)
        test_util.delete_all_tables(cls.validation_dataset_id)
        test_util.delete_all_tables(cls.sandbox_id)

        for cdm_table in [PERSON, OBSERVATION]:

            fq_table_name = f'{cls.project_id}.{cls.dataset_id}.{cdm_table}'

            schema = resources.fields_for(cdm_table)
            table = Table(fq_table_name, schema=schema)
            table = cls.client.create_table(table, exists_ok=True)

            query = POPULATE_STATEMENTS[cdm_table].render(
                fq_table_name=fq_table_name)
            job = cls.client.query(query)
            job.result()

        cls.rule_instance = RemoveNonMatchingParticipant(
            cls.project_id, cls.dataset_id, cls.sandbox_id, cls.ehr_dataset_id,
            cls.validation_dataset_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        for hpo_id in [HPO_1, HPO_2, HPO_3]:
            id_match_table_id = f'{hpo_id}_{IDENTITY_MATCH}'

            create_drc_validation_table(cls.client, id_match_table_id,
                                        cls.validation_dataset_id)

            populate_query = POPULATE_STATEMENTS[
                f'{hpo_id}_{IDENTITY_MATCH}'].render(
                    project_id=cls.project_id,
                    validation_dataset_id=cls.validation_dataset_id,
                    table_id=id_match_table_id)
            job = cls.client.query(populate_query)
            job.result()

        schema = resources.fields_for(PARTICIPANT_MATCH)
        for hpo_id in [HPO_1, HPO_2, HPO_4]:
            participant_match_table_id = f'{hpo_id}_{PARTICIPANT_MATCH}'

            table = Table(
                f'{cls.project_id}.{cls.ehr_dataset_id}.{participant_match_table_id}',
                schema=schema)
            table = cls.client.create_table(table, exists_ok=True)

            populate_query = POPULATE_STATEMENTS[
                f'{hpo_id}_{PARTICIPANT_MATCH}'].render(
                    project_id=cls.project_id,
                    ehr_dataset_id=cls.ehr_dataset_id,
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
        """
        Test for exist_participant_match(). Only HPO_3 should return False
        as it does not have a participant match table. 
        """
        self.assertTrue(
            self.rule_instance.exist_participant_match(self.ehr_dataset_id,
                                                       HPO_1))
        self.assertTrue(
            self.rule_instance.exist_participant_match(self.ehr_dataset_id,
                                                       HPO_2))
        self.assertFalse(
            self.rule_instance.exist_participant_match(self.ehr_dataset_id,
                                                       HPO_3))
        self.assertTrue(
            self.rule_instance.exist_participant_match(self.ehr_dataset_id,
                                                       HPO_4))

    def test_exist_identity_match(self):
        """
        Test for exist_identity_match(). Only HPO_4 should return False
        as it does not have a identity match table. 
        """
        self.assertTrue(
            self.rule_instance.exist_identity_match(
                f'{self.project_id}.{self.validation_dataset_id}.{HPO_1}_{IDENTITY_MATCH}'
            ))
        self.assertTrue(
            self.rule_instance.exist_identity_match(
                f'{self.project_id}.{self.validation_dataset_id}.{HPO_2}_{IDENTITY_MATCH}'
            ))
        self.assertTrue(
            self.rule_instance.exist_identity_match(
                f'{self.project_id}.{self.validation_dataset_id}.{HPO_3}_{IDENTITY_MATCH}'
            ))
        self.assertFalse(
            self.rule_instance.exist_identity_match(
                f'{self.project_id}.{self.validation_dataset_id}.{HPO_4}_{IDENTITY_MATCH}'
            ))

    def test_get_missing_criterion(self):
        """Test for get_missing_criterion().
        """
        self.assertEqual(
            self.rule_instance.get_missing_criterion(KEY_FIELDS),
            "CAST(first_name <> 'match' AS int64) + " +
            "CAST(last_name <> 'match' AS int64) + " +
            "CAST(birth_date <> 'match' AS int64)")

    def test_get_not_validated_participants(self):
        """Test for get_not_validated_participants(). Test for HPO_3 does not exist as 
        get_not_validated_participants() does not run for HPO sites without a participant match table.
        """
        self.assertCountEqual(
            self.rule_instance.get_not_validated_participants(
                self.ehr_dataset_id, HPO_1), [])
        self.assertCountEqual(
            self.rule_instance.get_not_validated_participants(
                self.ehr_dataset_id, HPO_2), [204])
        self.assertCountEqual(
            self.rule_instance.get_not_validated_participants(
                self.ehr_dataset_id, HPO_4), [401, 402, 403, 404])

    def test_get_non_match_participants(self):
        """Test for get_non_match_participants(). Only the participants who are
        not validated AND non-matching are returned. Test for HPO_1 does not exist as 
        get_non_match_participants() runs only for the HPO sites that (1) have not validated participants,
        or (2) does not have a participant match table. HPO_4 returns [] because it does not have
        an identity match table.
        """
        self.assertCountEqual(
            self.rule_instance.get_non_match_participants(
                self.validation_dataset_id,
                HPO_2,
                pids=self.rule_instance.get_not_validated_participants(
                    self.ehr_dataset_id, HPO_2)), [204])
        self.assertCountEqual(
            self.rule_instance.get_non_match_participants(
                self.validation_dataset_id, HPO_3), [303, 304])
        self.assertCountEqual(
            self.rule_instance.get_non_match_participants(
                self.validation_dataset_id,
                HPO_4,
                pids=self.rule_instance.get_not_validated_participants(
                    self.ehr_dataset_id, HPO_4)), [])

    def test_remove_non_matching_participant(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.

        Test HPO sites...
        HPO_1: have both participant_match and identity_match. And all the person_ids are validated by the site.
               -> CR skips all the person_ids from HPO_1.
        HPO_2: have both participant_match and identity_match. But person_id 204 is not validated by the site.
               -> CR runs again only 204 from HPO_1.
        HPO_3: has identity_match but not participant_match.
               -> CR runs through all the person_ids from HPO_3.
        HPO_4: has participant_match but not identity_match.
               -> CR skips all the person_ids from HPO_4.

        Test person_ids...
        X01: All "match" -> None of X01s should be removed.
        X02: Some "missing" but not exceeding NUM_OF_MISSING_[KEY,ALL]_FIELDS -> None of X02s should be removed.
        X03: "missing" exceeding NUM_OF_MISSING_KEY_FIELDS -> Only 303 should be removed. 103 and 203 are 
             validated, and 4XX is skipped because of missing identity_match table.
        X04: "missing" exceeding NUM_OF_MISSING_ALL_FIELDS -> 204 and 304 should be removed. 104 is validated,
             and 4XX is skipped because of missing identity_match table.
        """
        tables_and_counts = [
            {
                'name':
                    f'{PERSON}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{PERSON}',
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'race_concept_id', 'ethnicity_concept_id'
                ],
                'loaded_ids': [
                    101, 102, 103, 104, 201, 202, 203, 204, 301, 302, 303, 304,
                    401, 402, 403, 404
                ],
                'cleaned_values': [
                    (101, 0, 1991, 0, 0),
                    (102, 0, 1992, 0, 0),
                    (103, 0, 1993, 0, 0),
                    (104, 0, 1994, 0, 0),
                    (201, 0, 1991, 0, 0),
                    (202, 0, 1992, 0, 0),
                    (203, 0, 1993, 0, 0),
                    (301, 0, 1991, 0, 0),
                    (302, 0, 1992, 0, 0),
                    (401, 0, 1991, 0, 0),
                    (402, 0, 1992, 0, 0),
                    (403, 0, 1993, 0, 0),
                    (404, 0, 1994, 0, 0),
                ]
            },
            {
                'name':
                    f'{OBSERVATION}',
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
                'fields': [
                    'observation_id', 'person_id', 'observation_concept_id',
                    'observation_date', 'observation_type_concept_id'
                ],
                'loaded_ids': [
                    1001, 1002, 1003, 1004, 2001, 2002, 2003, 2004, 3001, 3002,
                    3003, 3004, 4001, 4002, 4003, 4004
                ],
                'cleaned_values': [
                    (1001, 101, 0, parse('2022-01-01').date(), 0),
                    (1002, 102, 0, parse('2022-01-02').date(), 0),
                    (1003, 103, 0, parse('2022-01-03').date(), 0),
                    (1004, 104, 0, parse('2022-01-04').date(), 0),
                    (2001, 201, 0, parse('2022-01-01').date(), 0),
                    (2002, 202, 0, parse('2022-01-02').date(), 0),
                    (2003, 203, 0, parse('2022-01-03').date(), 0),
                    (3001, 301, 0, parse('2022-01-01').date(), 0),
                    (3002, 302, 0, parse('2022-01-02').date(), 0),
                    (4001, 401, 0, parse('2022-01-01').date(), 0),
                    (4002, 402, 0, parse('2022-01-02').date(), 0),
                    (4003, 403, 0, parse('2022-01-03').date(), 0),
                    (4004, 404, 0, parse('2022-01-04').date(), 0),
                ]
            },
        ]

        self.default_test(tables_and_counts)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        test_util.delete_all_tables(cls.dataset_id)
        test_util.delete_all_tables(cls.ehr_dataset_id)
        test_util.delete_all_tables(cls.validation_dataset_id)
        test_util.delete_all_tables(cls.sandbox_id)
