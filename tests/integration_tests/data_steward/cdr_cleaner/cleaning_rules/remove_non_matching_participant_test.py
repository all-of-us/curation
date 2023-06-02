"""Integration test for RemoveNonMatchingParticipant
"""

# Python Imports
import os

# Third party imports

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import (
    RemoveNonMatchingParticipant, NOT_MATCH_TABLE)
from common import (AOU_DEATH, JINJA_ENV, IDENTITY_MATCH, OBSERVATION,
                    PARTICIPANT_MATCH, PERSON)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

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
        (10101, 101, 0, date('2022-01-01'), 0),
        (10102, 101, 0, date('2022-01-01'), 0),
        (10201, 102, 0, date('2022-01-02'), 0),
        (10202, 102, 0, date('2022-01-02'), 0),
        (10301, 103, 0, date('2022-01-03'), 0),
        (10302, 103, 0, date('2022-01-03'), 0),
        (10401, 104, 0, date('2022-01-04'), 0),
        (10402, 104, 0, date('2022-01-04'), 0),
        (20101, 201, 0, date('2022-01-01'), 0),
        (20102, 201, 0, date('2022-01-01'), 0),
        (20201, 202, 0, date('2022-01-02'), 0),
        (20202, 202, 0, date('2022-01-02'), 0),
        (20301, 203, 0, date('2022-01-03'), 0),
        (20302, 203, 0, date('2022-01-03'), 0),
        (20401, 204, 0, date('2022-01-04'), 0),
        (20402, 204, 0, date('2022-01-04'), 0),
        (30101, 301, 0, date('2022-01-01'), 0),
        (30102, 301, 0, date('2022-01-01'), 0),
        (30201, 302, 0, date('2022-01-02'), 0),
        (30202, 302, 0, date('2022-01-02'), 0),
        (30301, 303, 0, date('2022-01-03'), 0),
        (30302, 303, 0, date('2022-01-03'), 0),
        (30401, 304, 0, date('2022-01-04'), 0),
        (30402, 304, 0, date('2022-01-04'), 0),
        (40101, 401, 0, date('2022-01-01'), 0),
        (40102, 401, 0, date('2022-01-01'), 0),
        (40201, 402, 0, date('2022-01-02'), 0),
        (40202, 402, 0, date('2022-01-02'), 0),
        (40301, 403, 0, date('2022-01-03'), 0),
        (40302, 403, 0, date('2022-01-03'), 0),
        (40401, 404, 0, date('2022-01-04'), 0),
        (40402, 404, 0, date('2022-01-04'), 0)
        """),
    f'_mapping_{OBSERVATION}':
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (observation_id, src_dataset_id, src_observation_id, src_hpo_id, src_table_id)
        VALUES
        (10101, '{{rdr_dataset_id}}', 111, 'rdr', 'observation'),
        (10102, '{{ehr_dataset_id}}', 112, '{{hpo_1}}', 'observation'),
        (10201, '{{rdr_dataset_id}}', 121, 'rdr', 'observation'),
        (10202, '{{ehr_dataset_id}}', 122, '{{hpo_1}}', 'observation'),
        (10301, '{{rdr_dataset_id}}', 131, 'rdr', 'observation'),
        (10302, '{{ehr_dataset_id}}', 132, '{{hpo_1}}', 'observation'),
        (10401, '{{rdr_dataset_id}}', 141, 'rdr', 'observation'),
        (10402, '{{ehr_dataset_id}}', 142, '{{hpo_1}}', 'observation'),
        (20101, '{{rdr_dataset_id}}', 211, 'rdr', 'observation'),
        (20102, '{{ehr_dataset_id}}', 212, '{{hpo_2}}', 'observation'),
        (20201, '{{rdr_dataset_id}}', 221, 'rdr', 'observation'),
        (20202, '{{ehr_dataset_id}}', 222, '{{hpo_2}}', 'observation'),
        (20301, '{{rdr_dataset_id}}', 231, 'rdr', 'observation'),
        (20302, '{{ehr_dataset_id}}', 232, '{{hpo_2}}', 'observation'),
        (20401, '{{rdr_dataset_id}}', 241, 'rdr', 'observation'),
        (20402, '{{ehr_dataset_id}}', 242, '{{hpo_2}}', 'observation'),
        (30101, '{{rdr_dataset_id}}', 311, 'rdr', 'observation'),
        (30102, '{{ehr_dataset_id}}', 312, '{{hpo_3}}', 'observation'),
        (30201, '{{rdr_dataset_id}}', 321, 'rdr', 'observation'),
        (30202, '{{ehr_dataset_id}}', 322, '{{hpo_3}}', 'observation'),
        (30301, '{{rdr_dataset_id}}', 331, 'rdr', 'observation'),
        (30302, '{{ehr_dataset_id}}', 332, '{{hpo_3}}', 'observation'),
        (30401, '{{rdr_dataset_id}}', 341, 'rdr', 'observation'),
        (30402, '{{ehr_dataset_id}}', 342, '{{hpo_3}}', 'observation'),
        (40101, '{{rdr_dataset_id}}', 411, 'rdr', 'observation'),
        (40102, '{{ehr_dataset_id}}', 412, '{{hpo_4}}', 'observation'),
        (40201, '{{rdr_dataset_id}}', 421, 'rdr', 'observation'),
        (40202, '{{ehr_dataset_id}}', 422, '{{hpo_4}}', 'observation'),
        (40301, '{{rdr_dataset_id}}', 431, 'rdr', 'observation'),
        (40302, '{{ehr_dataset_id}}', 432, '{{hpo_4}}', 'observation'),
        (40401, '{{rdr_dataset_id}}', 441, 'rdr', 'observation'),
        (40402, '{{ehr_dataset_id}}', 442, '{{hpo_4}}', 'observation')
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
    AOU_DEATH:
        JINJA_ENV.from_string("""
        INSERT INTO `{{fq_table_name}}` 
        (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
        VALUES
        ('a10101', 101, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a10102', 101, date('2020-05-05'), 0, 0, 0, '{{hpo_1}}', False),
        ('a10201', 102, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a10202', 102, date('2020-05-05'), 0, 0, 0, '{{hpo_1}}', False),
        ('a10301', 103, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a10302', 103, date('2020-05-05'), 0, 0, 0, '{{hpo_1}}', False),
        ('a10401', 104, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a10402', 104, date('2020-05-05'), 0, 0, 0, '{{hpo_1}}', False),
        ('a20101', 201, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a20102', 201, date('2020-05-05'), 0, 0, 0, '{{hpo_2}}', False),
        ('a20201', 202, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a20202', 202, date('2020-05-05'), 0, 0, 0, '{{hpo_2}}', False),
        ('a20301', 203, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a20302', 203, date('2020-05-05'), 0, 0, 0, '{{hpo_2}}', False),
        ('a20401', 204, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a20402', 204, date('2020-05-05'), 0, 0, 0, '{{hpo_2}}', False),
        ('a30101', 301, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a30102', 301, date('2020-05-05'), 0, 0, 0, '{{hpo_3}}', False),
        ('a30201', 302, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a30202', 302, date('2020-05-05'), 0, 0, 0, '{{hpo_3}}', False),
        ('a30301', 303, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a30302', 303, date('2020-05-05'), 0, 0, 0, '{{hpo_3}}', False),
        ('a30401', 304, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a30402', 304, date('2020-05-05'), 0, 0, 0, '{{hpo_3}}', False),
        ('a40101', 401, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a40102', 401, date('2020-05-05'), 0, 0, 0, '{{hpo_4}}', False),
        ('a40201', 402, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a40202', 402, date('2020-05-05'), 0, 0, 0, '{{hpo_4}}', False),
        ('a40301', 403, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a40302', 403, date('2020-05-05'), 0, 0, 0, '{{hpo_4}}', False),
        ('a40401', 404, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
        ('a40402', 404, date('2020-05-05'), 0, 0, 0, '{{hpo_4}}', False)
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
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.ehr_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.rdr_dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.validation_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {
            'validation_dataset_id': cls.validation_dataset_id,
            'ehr_dataset_id': cls.ehr_dataset_id
        }

        cls.rule_instance = RemoveNonMatchingParticipant(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
            ehr_dataset_id=cls.ehr_dataset_id,
            validation_dataset_id=cls.validation_dataset_id)

        cls.fq_table_names = []
        for cdm_table in [
                PERSON, OBSERVATION, '_mapping_observation', AOU_DEATH
        ]:
            fq_table_name = f'{cls.project_id}.{cls.dataset_id}.{cdm_table}'
            cls.fq_table_names.append(fq_table_name)

        # Overwriting affected_tables, as only PERSON, OBSERVATION, and AOU_DEATH are prepared for this test.
        cls.rule_instance.affected_tables = [OBSERVATION, AOU_DEATH]

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names + [NOT_MATCH_TABLE]:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        for hpo_id in [HPO_1, HPO_2, HPO_3]:
            fq_table_name = f'{cls.project_id}.{cls.validation_dataset_id}.{hpo_id}_{IDENTITY_MATCH}'
            cls.fq_table_names.append(fq_table_name)

        for hpo_id in [HPO_1, HPO_2, HPO_4]:
            fq_table_name = f'{cls.project_id}.{cls.ehr_dataset_id}.{hpo_id}_{PARTICIPANT_MATCH}'
            cls.fq_table_names.append(fq_table_name)

        # Set client and create datasets if not exist
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """

        load_statements = []

        for cdm_table in self.fq_table_names:
            table_id = cdm_table.split('.')[-1]
            query = POPULATE_STATEMENTS[table_id].render(
                fq_table_name=cdm_table,
                project_id=self.project_id,
                validation_dataset_id=self.validation_dataset_id,
                table_id=table_id,
                rdr_dataset_id=self.rdr_dataset_id,
                ehr_dataset_id=self.ehr_dataset_id,
                hpo_1=HPO_1,
                hpo_2=HPO_2,
                hpo_3=HPO_3,
                hpo_4=HPO_4)
            load_statements.append(query)

        for hpo_id in [HPO_1, HPO_2, HPO_3]:
            id_match_table_id = f'{hpo_id}_{IDENTITY_MATCH}'

            id_match = POPULATE_STATEMENTS[id_match_table_id].render(
                project_id=self.project_id,
                validation_dataset_id=self.validation_dataset_id,
                table_id=id_match_table_id)
            load_statements.append(id_match)

        for hpo_id in [HPO_1, HPO_2, HPO_4]:
            participant_match_table_id = f'{hpo_id}_{PARTICIPANT_MATCH}'
            query = POPULATE_STATEMENTS[participant_match_table_id].render(
                project_id=self.project_id,
                ehr_dataset_id=self.ehr_dataset_id,
                table_id=participant_match_table_id)
            load_statements.append(query)

        super().setUp()
        self.load_test_data(load_statements)

    def test_remove_non_matching_participant(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.

        EHR validation(participant_match) and DRC matching algorithm(identity_match)...
            HPO_1: participant_match exists? -> Yes. identity_match exists? -> Yes.
            HPO_2: participant_match exists? -> Yes. identity_match exists? -> Yes.
            HPO_3: participant_match exists? -> No.  identity_match exists? -> Yes.
            HPO_4: participant_match exists? -> Yes. identity_match exists? -> No.

        For each person_id, is it validated by EHR sites?
            HPO_1: 101...Yes, 102...Yes, 103...Yes, 104...Yes
            HPO_2: 201...Yes, 202...Yes, 203...Yes, 204...NO
            HPO_3: 301...NO,  302...NO,  303...NO,  304...NO (all NO because no participant_match table exists)
            HPO_4: 401...NO,  402...NO,  403...NO,  404...NO

        For each person_id, what is the result of the DRC matching algorithm?
            101, 201, 301 ... All "match", so it's OK
            102, 202, 302 ... Some "missing" but not exceeding NUM_OF_MISSING_[KEY,ALL]_FIELDS so it's OK
            103, 203, 303 ... "missing" exceeding NUM_OF_MISSING_KEY_FIELDS, so it's NOT OK
            104, 204, 304 ... "missing" exceeding NUM_OF_MISSING_ALL_FIELDS, so it's NOT OK
            401, 402, 403, 404 ... Not validated (no identity_match table exists). This CR skips them.

        Test Observation IDs...
            XXX01 ... Records from RDR
            XXX02 ... Records from EHR

        The records that meet all the following criteria are removed by this CR:
            1. Not validated by the EHR site,
            2. Fails DRC matching algorithm, and
            3. From EHR.
            * Person table is not affected by this CR since all the data is from RDR.

        The test records that meet all the criteria are [20402, 30302, 30402] in observation
        and ['a20402', 'a30302', 'a30402'] in aou_death. 
        """
        tables_and_counts = [
            {
                'name':
                    PERSON,
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{PERSON}',
                'fields': ['person_id'],
                'loaded_ids': [
                    101, 102, 103, 104, 201, 202, 203, 204, 301, 302, 303, 304,
                    401, 402, 403, 404
                ],
                'cleaned_values': [
                    (101,), (102,), (103,), (104,), (201,), (202,), (203,),
                    (204,), (301,), (302,), (303,), (304,), (401,), (402,),
                    (403,), (404,)
                ]
            },
            {
                'name':
                    OBSERVATION,
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
                'fq_sandbox_table_name': [
                    table for table in self.fq_sandbox_table_names
                    if OBSERVATION in table
                ][0],
                'fields': ['observation_id', 'person_id'],
                'loaded_ids': [
                    10101, 10102, 10201, 10202, 10301, 10302, 10401, 10402,
                    20101, 20102, 20201, 20202, 20301, 20302, 20401, 20402,
                    30101, 30102, 30201, 30202, 30301, 30302, 30401, 30402,
                    40101, 40102, 40201, 40202, 40301, 40302, 40401, 40402
                ],
                'sandboxed_ids': [20402, 30302, 30402],
                'cleaned_values': [(10101, 101), (10102, 101), (10201, 102),
                                   (10202, 102), (10301, 103), (10302, 103),
                                   (10401, 104), (10402, 104), (20101, 201),
                                   (20102, 201), (20201, 202), (20202, 202),
                                   (20301, 203), (20302, 203), (20401, 204),
                                   (30101, 301), (30102, 301), (30201, 302),
                                   (30202, 302), (30301, 303), (30401, 304),
                                   (40101, 401), (40102, 401), (40201, 402),
                                   (40202, 402), (40301, 403), (40302, 403),
                                   (40401, 404), (40402, 404)]
            },
            {
                'name':
                    AOU_DEATH,
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
                'fq_sandbox_table_name': [
                    table for table in self.fq_sandbox_table_names
                    if AOU_DEATH in table
                ][0],
                'fields': ['aou_death_id', 'person_id'],
                'loaded_ids': [
                    'a10101', 'a10102', 'a10201', 'a10202', 'a10301', 'a10302',
                    'a10401', 'a10402', 'a20101', 'a20102', 'a20201', 'a20202',
                    'a20301', 'a20302', 'a20401', 'a20402', 'a30101', 'a30102',
                    'a30201', 'a30202', 'a30301', 'a30302', 'a30401', 'a30402',
                    'a40101', 'a40102', 'a40201', 'a40202', 'a40301', 'a40302',
                    'a40401', 'a40402'
                ],
                'sandboxed_ids': ['a20402', 'a30302', 'a30402'],
                'cleaned_values': [('a10101', 101), ('a10102', 101),
                                   ('a10201', 102), ('a10202', 102),
                                   ('a10301', 103), ('a10302', 103),
                                   ('a10401', 104), ('a10402', 104),
                                   ('a20101', 201), ('a20102', 201),
                                   ('a20201', 202), ('a20202', 202),
                                   ('a20301', 203), ('a20302', 203),
                                   ('a20401', 204), ('a30101', 301),
                                   ('a30102', 301), ('a30201', 302),
                                   ('a30202', 302), ('a30301', 303),
                                   ('a30401', 304), ('a40101', 401),
                                   ('a40102', 401), ('a40201', 402),
                                   ('a40202', 402), ('a40301', 403),
                                   ('a40302', 403), ('a40401', 404),
                                   ('a40402', 404)]
            },
        ]

        self.default_test(tables_and_counts)
