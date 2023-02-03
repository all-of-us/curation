"""
Integration test for repopulate_person_controlled_tier module

Original Issues: DC-1439, DC-2273

The intent is to repopulate the person table using the PPI responses based on the controlled tier
privacy requirements.
"""

# Python Imports
import os
from dateutil import parser

# Project Imports
from common import PERSON, OBSERVATION, VOCABULARY_TABLES
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.repopulate_person_controlled_tier import \
    RepopulatePersonControlledTier, \
    NONE_OF_THESE_CONCEPT_ID, NONE_OF_THESE_CONCEPT_SOURCE_VALUE, SKIP_CONCEPT_SOURCE_VALUE, \
    GENERALIZED_RACE_CONCEPT_ID, GENERALIZED_RACE_SOURCE_VALUE, \
    GENERALIZED_GENDER_IDENTITY_CONCEPT_ID, GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE, \
    NON_HISPANIC_LATINO_CONCEPT_ID, SKIP_CONCEPT_ID, NON_HISPANIC_LATINO_CONCEPT_SOURCE_VALUE, \
    HISPANIC_LATINO_STANDARD_CONCEPT_ID, HISPANIC_LATINO_STANDARD_SOURCE_VALUE, \
    PNA_CONCEPT_ID, PNA_CONCEPT_SOURCE_VALUE, NO_MATCHING_CONCEPT_ID, NO_MATCHING_SOURCE_VALUE
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

AOU_NONE_INDICATED_CONCEPT_ID = 2100000001


class RepopulatePersonControlledTierTestBase(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = RepopulatePersonControlledTier(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [PERSON, OBSERVATION] + VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for sandbox_table_name in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{sandbox_table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

        # Copy vocab tables over to the test dataset
        for src_table in cls.client.list_tables(cls.vocabulary_id):
            destination = f'{cls.project_id}.{cls.dataset_id}.{src_table.table_id}'
            cls.client.copy_table(src_table, destination)

    def setUp(self):
        """
        Setting up tables before running the tests.
        """
        super().setUp()

        person_data_template = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.person`
            (
                person_id,
                gender_concept_id,
                year_of_birth,
                month_of_birth,
                day_of_birth,
                birth_datetime,
                race_concept_id,
                ethnicity_concept_id,
                location_id,
                provider_id,
                care_site_id,
                person_source_value,
                gender_source_value,
                gender_source_concept_id,
                race_source_value,
                race_source_concept_id,
                ethnicity_source_value,
                ethnicity_source_concept_id
            )
            VALUES
            (1, 0, 1990, 1, 1, '1990-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (2, 0, 1980, 1, 1, '1980-01-01T00:00:01', 0, 0, 1, 1, 1, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (3, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (4, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (5, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (6, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (7, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (8, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0),
            (9, 0, 1970, 1, 1, '1970-01-01T00:00:01', 0, 0, NULL, NULL, NULL, 'person_source_value', 'gender_source_value', 0, 'race_source_value', 0, 'ethnicity', 0)
       """)

        observation_data_template = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (
                observation_id,
                person_id,
                value_as_concept_id,
                observation_source_concept_id,
                value_source_concept_id,
                value_source_value,
                observation_date,
                observation_concept_id,
                observation_type_concept_id
            )
            VALUES
                (11, 1, 45877987, 1586140, 1586146, 'WhatRaceEthnicity_White', '2020-01-01', 0, 0),
                (12, 1, 1586143, 1586140, 1586143, 'WhatRaceEthnicity_Black', '2020-01-01', 0, 0),
                (13, 1, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (14, 1, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (21, 2, 45879439, 1586140, 1586142, 'WhatRaceEthnicity_Asian', '2020-01-01', 0, 0),
                (22, 2, 1586147, 1586140, 1586147, 'WhatRaceEthnicity_Hispanic', '2020-01-01', 0, 0),
                (23, 2, 45880669, 1585838, 1585839, 'GenderIdentity_Man', '2020-01-01', 0, 0),
                (24, 2, 1585841, 1585838, 1585841, 'GenderIdentity_NonBinary', '2020-01-01', 0, 0),
                (25, 2, 45880669, 1585845, 1585846, 'SexAtBirth_Male', '2020-01-01', 0, 0),
                (31, 3, 903096, 1586140, 903096, 'PMI_Skip', '2020-01-01', 0, 0),
                (32, 3, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (33, 3, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (41, 4, 903079, 1586140, 903079, 'PMI_PreferNotToAnswer', '2020-01-01', 0, 0),
                (42, 4, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (43, 4, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (51, 5, 1586148, 1586140, 1586148, 'WhatRaceEthnicity_RaceEthnicityNoneOfThese', '2020-01-01', 0, 0),
                (52, 5, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (53, 5, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (61, 6, 45877987, 1586140, 1586146, 'WhatRaceEthnicity_White', '2020-01-01', 0, 0),
                (62, 6, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (63, 6, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (71, 7, 0, 1586140, 0, NULL, '2020-01-01', 0, 0),
                (72, 7, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (73, 7, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (82, 8, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (83, 8, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0),
                (92, 9, 45878463, 1585838, 1585840, 'GenderIdentity_Woman', '2020-01-01', 0, 0),
                (93, 9, 45878463, 1585845, 1585847, 'SexAtBirth_Female', '2020-01-01', 0, 0)
        """)

        insert_person_query = person_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data(
            [f'{insert_person_query}', f'{insert_observation_query}'])

    def test_repopulate_person_controlled_tier(self):
        """
        Test cases
        1 - Race: White and Black,     Ethnicity: Non-hispanic, Gender Identity: Woman,              Sex at Birth: Female,
        2 - Race: Asian,               Ethnicity: Hispanic,     Gender Identity: Man and Non-binary, Sex at Birth: Male,
        3 - Race and Ethnicity: Skipped,                        Gender Identity: Woman,              Sex at Birth: Female,
        4 - Race and Ethnicity: Prefer not to answer,           Gender Identity: Woman,              Sex at Birth: Female,
        5 - Race and Ethnicity: None of these,                  Gender Identity: Woman,              Sex at Birth: Female,
        6 - Race: White,               Ethnicity: Non-hispanic, Gender Identity: Woman,              Sex at Birth: Female,
        7 - Race and Ethnicity: No matching concept,            Gender Identity: Woman,              Sex at Birth: Female,
        8 - Race and Ethnicity: No record,                      Gender Identity: Woman,              Sex at Birth: Female,
        9 - Race concept AoU none indicated,                    Race source concept: AoU none indicated
        """

        self.maxDiff = None

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.person',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'month_of_birth', 'day_of_birth', 'birth_datetime',
                'race_concept_id', 'ethnicity_concept_id', 'location_id',
                'provider_id', 'care_site_id', 'person_source_value',
                'gender_source_value', 'gender_source_concept_id',
                'race_source_value', 'race_source_concept_id',
                'ethnicity_source_value', 'ethnicity_source_concept_id'
            ],
            'cleaned_values': [
                (1, 45878463, 1990, None, None,
                 parser.parse('1990-06-15 00:00:00 UTC'),
                 GENERALIZED_RACE_CONCEPT_ID, NON_HISPANIC_LATINO_CONCEPT_ID,
                 None, None, None, 'person_source_value',
                 'GenderIdentity_Woman', 1585840, GENERALIZED_RACE_SOURCE_VALUE,
                 GENERALIZED_RACE_CONCEPT_ID,
                 NON_HISPANIC_LATINO_CONCEPT_SOURCE_VALUE,
                 NON_HISPANIC_LATINO_CONCEPT_ID),
                (2, GENERALIZED_GENDER_IDENTITY_CONCEPT_ID, 1980, None, None,
                 parser.parse('1980-06-15 00:00:00 UTC'), 8515,
                 HISPANIC_LATINO_STANDARD_CONCEPT_ID, 1, 1, 1,
                 'person_source_value',
                 GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE,
                 GENERALIZED_GENDER_IDENTITY_CONCEPT_ID,
                 'WhatRaceEthnicity_Asian', 1586142,
                 HISPANIC_LATINO_STANDARD_SOURCE_VALUE,
                 HISPANIC_LATINO_STANDARD_CONCEPT_ID),
                (3, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'), SKIP_CONCEPT_ID,
                 SKIP_CONCEPT_ID, None, None, None, 'person_source_value',
                 'GenderIdentity_Woman', 1585840, SKIP_CONCEPT_SOURCE_VALUE,
                 SKIP_CONCEPT_ID, SKIP_CONCEPT_SOURCE_VALUE, SKIP_CONCEPT_ID),
                (4, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'), PNA_CONCEPT_ID,
                 PNA_CONCEPT_ID, None, None, None, 'person_source_value',
                 'GenderIdentity_Woman', 1585840, PNA_CONCEPT_SOURCE_VALUE,
                 PNA_CONCEPT_ID, PNA_CONCEPT_SOURCE_VALUE, PNA_CONCEPT_ID),
                (5, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'),
                 NONE_OF_THESE_CONCEPT_ID, NONE_OF_THESE_CONCEPT_ID, None, None,
                 None, 'person_source_value', 'GenderIdentity_Woman', 1585840,
                 NONE_OF_THESE_CONCEPT_SOURCE_VALUE, NONE_OF_THESE_CONCEPT_ID,
                 NONE_OF_THESE_CONCEPT_SOURCE_VALUE, NONE_OF_THESE_CONCEPT_ID),
                (6, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'), 8527,
                 NON_HISPANIC_LATINO_CONCEPT_ID, None, None, None,
                 'person_source_value', 'GenderIdentity_Woman', 1585840,
                 'WhatRaceEthnicity_White', 1586146,
                 NON_HISPANIC_LATINO_CONCEPT_SOURCE_VALUE,
                 NON_HISPANIC_LATINO_CONCEPT_ID),
                (7, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'), 2100000001,
                 NO_MATCHING_CONCEPT_ID, None, None, None,
                 'person_source_value', 'GenderIdentity_Woman', 1585840,
                 'AoUDRC_NoneIndicated', NO_MATCHING_CONCEPT_ID,
                 NO_MATCHING_SOURCE_VALUE, NO_MATCHING_CONCEPT_ID),
                (8, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'), 2100000001,
                 NO_MATCHING_CONCEPT_ID, None, None, None,
                 'person_source_value', 'GenderIdentity_Woman', 1585840,
                 'AoUDRC_NoneIndicated', 2100000001, NO_MATCHING_SOURCE_VALUE,
                 NO_MATCHING_CONCEPT_ID),
                (9, 45878463, 1970, None, None,
                 parser.parse('1970-06-15 00:00:00 UTC'), 2100000001,
                 NO_MATCHING_CONCEPT_ID, None, None, None,
                 'person_source_value', 'GenderIdentity_Woman', 1585840,
                 'AoUDRC_NoneIndicated', 2100000001, NO_MATCHING_SOURCE_VALUE,
                 NO_MATCHING_CONCEPT_ID)
            ]
        }]

        self.default_test(tables_and_counts)
