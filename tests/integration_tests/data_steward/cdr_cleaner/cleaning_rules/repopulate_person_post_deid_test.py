# Python imports
from datetime import date
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.repopulate_person_post_deid import (
    RepopulatePersonPostDeid, GENDER_CONCEPT_ID, SEX_AT_BIRTH_CONCEPT_ID)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import PERSON, OBSERVATION, CONCEPT

GENDER_NONBINARY_CONCEPT_ID = 1585841
GENDER_NONBINARY_SOURCE_CONCEPT_ID = 123
SEX_FEMALE_CONCEPT_ID = 1585847
SEX_FEMALE_SOURCE_CONCEPT_ID = 45878463


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

        cls.rule_instance = RepopulatePersonPostDeid(project_id, dataset_id,
                                                     sandbox_id)

        table_names = [PERSON, OBSERVATION, CONCEPT]

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

        super().setUp()

    def test_repopulate_person_post_deid(self):
        """
        Tests that the specifications for queries perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        queries = []

        insert_concepts_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept` (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
            VALUES
                ({{gender_concept_id}}, "some text", "some text", "some text", "some text", "gender", date('2020-05-05'), date('2020-05-05')),
                ({{gender_nonbinary_concept_id}}, "some text", "some text", "some text", "some text", "nonbinary", date('2020-05-05'), date('2020-05-05')),
                ({{gender_nonbinary_source_concept_id}}, "some text", "some text", "some text", "some text", "nonbinary_src", date('2020-05-05'), date('2020-05-05')),
                ({{sex_at_birth_concept_id}}, "some text", "some text", "some text", "some text", "sex", date('2020-05-05'), date('2020-05-05')),
                ({{sex_female_concept_id}}, "some text", "some text", "some text", "some text", "female", date('2020-05-05'), date('2020-05-05')),
                ({{sex_female_source_concept_id}}, "some text", "some text", "some text", "some text", "female_src", date('2020-05-05'), date('2020-05-05'))
        """).render(fq_dataset_name=self.fq_dataset_name,
                    gender_concept_id=GENDER_CONCEPT_ID,
                    gender_nonbinary_concept_id=GENDER_NONBINARY_CONCEPT_ID,
                    gender_nonbinary_source_concept_id=
                    GENDER_NONBINARY_SOURCE_CONCEPT_ID,
                    sex_at_birth_concept_id=SEX_AT_BIRTH_CONCEPT_ID,
                    sex_female_concept_id=SEX_FEMALE_CONCEPT_ID,
                    sex_female_source_concept_id=SEX_FEMALE_SOURCE_CONCEPT_ID)

        create_persons_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.person` (person_id, gender_concept_id, birth_datetime, year_of_birth, race_concept_id, ethnicity_concept_id, gender_source_value, gender_source_concept_id)
            VALUES
                (1, 1, timestamp('1991-05-05'), 1991, 1, 1, "some text", 1),
                (2, 2, timestamp('1976-05-05'), 1976, 2, 2, "some text", 2)
        """).render(fq_dataset_name=self.fq_dataset_name)

        create_observations_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation` (person_id, observation_id, observation_source_concept_id, value_as_concept_id, value_source_concept_id,
                observation_date, observation_type_concept_id, observation_concept_id)
            VALUES
                (1, 100, {{gender_concept_id}}, {{gender_nonbinary_concept_id}}, {{gender_nonbinary_source_concept_id}}, date('2020-05-05'), 1, 1),
                (1, 101, {{sex_at_birth_concept_id}}, {{sex_female_concept_id}}, {{sex_female_source_concept_id}}, date('2020-05-05'), 2, 2)
        """).render(fq_dataset_name=self.fq_dataset_name,
                    gender_concept_id=GENDER_CONCEPT_ID,
                    gender_nonbinary_concept_id=GENDER_NONBINARY_CONCEPT_ID,
                    gender_nonbinary_source_concept_id=
                    GENDER_NONBINARY_SOURCE_CONCEPT_ID,
                    sex_at_birth_concept_id=SEX_AT_BIRTH_CONCEPT_ID,
                    sex_female_concept_id=SEX_FEMALE_CONCEPT_ID,
                    sex_female_source_concept_id=SEX_FEMALE_SOURCE_CONCEPT_ID)

        queries = [
            insert_concepts_query, create_persons_query,
            create_observations_query
        ]
        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'person']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [1, 2],
            'sandboxed_ids': [],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'race_concept_id', 'ethnicity_concept_id',
                'gender_source_value', 'gender_source_concept_id',
                'sex_at_birth_concept_id', 'sex_at_birth_source_concept_id',
                'sex_at_birth_source_value'
            ],
            'cleaned_values': [(1, 1585841, 1991, 0, 38003564, "nonbinary_src",
                                123, SEX_FEMALE_CONCEPT_ID,
                                SEX_FEMALE_SOURCE_CONCEPT_ID, "female_src"),
                               (2, 0, 1976, 0, 38003564, "No matching concept",
                                0, 0, 0, "No matching concept")]
        }]

        self.default_test(tables_and_counts)
