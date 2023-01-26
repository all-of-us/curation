"""
Integration test for repopulate_person_post_deid module

The de-id scripts removes all fields in the person table except for the person_id and the birthdate_time field.
Before CDR handoff to the Workbench team, we need to repopulate the following fields with demographic information
from the observation table.

These are the following fields in the person table will be repopulated:

gender_concept_id
year_of_birth
month_of_birth
day_of_birth
race_concept_id
ethnicity_concept_id
gender_source_value
gender_source_concept_id
race_source_value
race_source_concept_id
ethnicity_source_value
ethnicity_source_concept_id

Original Issue: DC-516
"""

# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.repopulate_person_post_deid import (
    RepopulatePersonPostDeid, GENDER_CONCEPT_ID)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import PERSON, OBSERVATION, VOCABULARY_TABLES

GENDER_NONBINARY_CONCEPT_ID = 1585841
GENDER_NONBINARY_SOURCE_CONCEPT_ID = 123
SEX_FEMALE_CONCEPT_ID = 1585847
SEX_FEMALE_SOURCE_CONCEPT_ID = 45878463
ETHNICITY_NONEOFTHESE_CONCEPT_ID = 1586148
ETHNICITY_NONEOFTHESE_CONCEPT_CODE = "WhatRaceEthnicity_RaceEthnicityNoneOfThese"
ETHNICITY_PREFER_NOT_TO_ANSWER_CONCEPT_ID = 903079
ETHNICITY_NOT_HISPANIC_CONCEPT_ID = 38003564
ETHNICITY_NOT_HISPANIC_CONCEPT_CODE = "Not Hispanic"
ETHNICITY_HISPANIC_CONCEPT_ID = 1586147
ETHNICITY_HISPANIC_CONCEPT_CODE = "WhatRaceEthnicity_Hispanic"


class RepopulatePersonPostDeidTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        cls.vocabulary_dataset = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = RepopulatePersonPostDeid(project_id, dataset_id,
                                                     sandbox_id)

        table_names = [PERSON, OBSERVATION] + VOCABULARY_TABLES

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{table_name}'
            for table_name in table_names
        ]

        cls.fq_sandbox_table_names += [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_name)}'
            for table_name in table_names
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parser.parse('2020-05-05').date()

        self.copy_vocab_tables(self.vocabulary_dataset)
        super().setUp()

    def test_repopulate_person_post_deid(self):
        """
        Tests that the specifications for queries perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        self.maxDiff = None

        create_persons_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.person` (person_id, gender_concept_id, birth_datetime, year_of_birth,
            race_concept_id, race_source_concept_id, race_source_value, ethnicity_concept_id, ethnicity_source_concept_id,
            ethnicity_source_value, gender_source_value, gender_source_concept_id)
            INSERT INTO `{{fq_dataset_name}}.person` (person_id, gender_concept_id, birth_datetime, year_of_birth, 
            race_concept_id, race_source_concept_id, race_source_value, ethnicity_concept_id, ethnicity_source_concept_id, 
            ethnicity_source_value, gender_source_value, gender_source_concept_id)
            VALUES
                (1, 1, timestamp('1991-05-05'), 1991, 1, 1, "race_source_value", 1, 2, "ethnicity source_value", "gender_source_value", 1),
                (2, 2, timestamp('1976-05-05'), 1976, 2, 2, "race_source_value", 1, 2, "ethnicity source_value", "gender_source_value", 2),
                (3, 2, timestamp('1945-05-05'), 1945, 2, 3, "race_source_value", 1, 2, "ethnicity source_value", "gender_source_value", 2),
                (4, 2, '1900-01-01', 1900, 2, 3, "race_source_value", 1, 2, "ethnicity source_value", "gender_source_value", 2),
                (5, 2, '1900-01-01', 1900, 2, 3, "race_source_value", 1, 2, "ethnicity source_value", "gender_source_value", 2),
                (6, 2,  timestamp('1954-03-21'), 1954, 2100000001, 2100000001, "race_source_value", 1, 2, "ethnicity_source_value", "gender_source_value", 2)

        """).render(fq_dataset_name=self.fq_dataset_name)

        create_observations_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation` (person_id, observation_id, observation_source_concept_id,
             value_as_concept_id, value_source_concept_id, observation_date, observation_type_concept_id, observation_concept_id)
            VALUES
                (1, 100, {{gender_concept_id}}, {{gender_nonbinary_concept_id}}, {{gender_nonbinary_source_concept_id}}, date('2020-05-05'), 1, 1),
                --What race ethnicity? White --
                (1, 101, 1586140, 45877987, 1586146, '2020-01-01', 1, 1586140),
                --What race ethnicity? Black --
                (2, 102, 1586140, 45876489, 1586143, '2020-01-01', 1, 1586140),
                -- What race ethnicity? Ethnicity NoneofThese --
                (3, 103, 1586140, {{ethnicity_noneofthese_concept_id}}, {{ethnicity_noneofthese_concept_id}}, '2020-01-01', 1, 1586140),
                -- What race ethnicity?  Hispanic or Latino --
                (4, 104, 1586140, {{ethnicity_hispanic_concept_id}}, {{ethnicity_hispanic_concept_id}}, '2020-01-01', 1, 1586140),
                (4, 106, 1586140, 1586146, 1586146, '2020-01-01', 1, 1586140),
                 --What race ethnicity? Prefer not to answer --
                (5, 107, 1586140,{{ethnicity_pna_concept_id}}, {{ethnicity_pna_concept_id}}, '2020-01-01', 1,903079)
                
        """).render(
            fq_dataset_name=self.fq_dataset_name,
            gender_concept_id=GENDER_CONCEPT_ID,
            gender_nonbinary_concept_id=GENDER_NONBINARY_CONCEPT_ID,
            gender_nonbinary_source_concept_id=
            GENDER_NONBINARY_SOURCE_CONCEPT_ID,
            sex_female_concept_id=SEX_FEMALE_CONCEPT_ID,
            sex_female_source_concept_id=SEX_FEMALE_SOURCE_CONCEPT_ID,
            ethnicity_noneofthese_concept_id=ETHNICITY_NONEOFTHESE_CONCEPT_ID,
            ethnicity_pna_concept_id=ETHNICITY_PREFER_NOT_TO_ANSWER_CONCEPT_ID,
            ethnicity_hispanic_concept_id=ETHNICITY_HISPANIC_CONCEPT_ID)

        queries = [create_persons_query, create_observations_query]
        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'person']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'sandboxed_ids': [],
            'fields': [
                'person_id',
                'gender_concept_id',
                'year_of_birth',
                'race_concept_id',
                'race_source_concept_id',
                'race_source_value',
                'ethnicity_concept_id',
                'ethnicity_source_concept_id',
                'ethnicity_source_value',
                'gender_source_value',
                'gender_source_concept_id',
            ],
            'cleaned_values': [(
                1,
                1585841,
                1991,
                8527,
                1586146,
                'No matching concept',
                38003564,
                38003564,
                "Not Hispanic",
                "No matching concept",
                123,
            ),
                               (
                                   2,
                                   0,
                                   1976,
                                   8516,
                                   1586143,
                                   'No matching concept',
                                   38003564,
                                   38003564,
                                   "Not Hispanic",
                                   "No matching concept",
                                   0,
                               ),
                               (
                                   3,
                                   0,
                                   1945,
                                   1586148,
                                   1586148,
                                   'No matching concept',
                                   1586148,
                                   1586148,
                                   "WhatRaceEthnicity_RaceEthnicityNoneOfThese",
                                   "No matching concept",
                                   0,
                               ),
                               (
                                   4,
                                   0,
                                   1900,
                                   8527,
                                   1586146,
                                   "No matching concept",
                                   38003563,
                                   38003563,
                                   'Hispanic',
                                   "No matching concept",
                                   0,
                               ),
                               (
                                   5,
                                   0,
                                   1900,
                                   1177221,
                                   903079,
                                   'PMI_PreferNotToAnswer',
                                   903079,
                                   903079,
                                   'PMI_PreferNotToAnswer',
                                   "No matching concept",
                                   0,
                               ),
                               (
                                   6,
                                   0,
                                   1954,
                                   1177221,
                                   903079,
                                   'PMI_PreferNotToAnswer',
                                   903079,
                                   903079,
                                   'PMI_PreferNotToAnswer',
                                   "No matching concept",
                                   0,
                               )]
        }]

        self.default_test(tables_and_counts)