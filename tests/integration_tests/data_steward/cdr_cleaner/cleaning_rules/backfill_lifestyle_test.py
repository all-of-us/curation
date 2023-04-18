"""
Integration test for BackfillLifestyle.
"""
# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.backfill_lifestyle import BackfillLifestyle
from common import JINJA_ENV, OBSERVATION, PERSON
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`
(observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id, observation_source_concept_id, questionnaire_response_id)
VALUES
    -- person_id=1 has all Lifestyle records. Nothing happens. --
    (101, 1, 40766306, date('2020-01-01'), 45905771, 1585857, 111),
    (102, 1, 1585636, date('2020-01-01'), 45905771, 1585636, 111),
    (103, 1, 1586166, date('2020-01-01'), 45905771, 1586166, 111),
    (104, 1, 1586174, date('2020-01-01'), 45905771, 1586174, 111),
    (105, 1, 1586182, date('2020-01-01'), 45905771, 1586182, 111),
    (106, 1, 1586190, date('2020-01-01'), 45905771, 1586190, 111),
    (107, 1, 40766357, date('2020-01-01'), 45905771, 1586198, 111),
    -- person_id=2 has NO Lifestyle records. Nothing happens. --
    (201, 2, 9999999, date('2020-01-01'), 99999999, 9999999, 222),
    -- person_id=3 has some missing Lifestyle records. Backfill happens. --
    -- Backfilled skip records have the MAX date ('2022-01-01') as its date. --
    (301, 3, 40766306, date('2020-01-01'), 45905771, 1585857, 333),
    (302, 3, 1585636, date('2021-01-01'), 45905771, 1585636, 333),
    (303, 3, 1586166, date('2022-01-01'), 45905771, 1586166, 333)
""")

PERSON_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
    (1, 8532, 2001, 999, 99999),
    (2, 8532, 2001, 999, 99999),
    (3, 8532, 2001, 999, 99999)
""")


class BackfillLifestyleTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = BackfillLifestyle(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id)

        cls.fq_sandbox_table_names = []

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.{PERSON}',
        ]

        super().setUpClass()

    def setUp(self):
        self.date_2020 = parser.parse('2020-01-01').date()
        self.date_2021 = parser.parse('2021-01-01').date()
        self.date_2022 = parser.parse('2022-01-01').date()

        super().setUp()

        insert_observation = OBSERVATION_TMPL.render(project=self.project_id,
                                                     dataset=self.dataset_id)
        insert_person = PERSON_TMPL.render(project=self.project_id,
                                           dataset=self.dataset_id)

        queries = [insert_observation, insert_person]
        self.load_test_data(queries)

    def test_backfill_lifestyle(self):
        """
        Test cases:
        person_id = 1:
            It has all the Lifestyle records. Nothing happens.
        person_id = 2:
            It has NO Lifestyle records. Nothing happens.
        person_id = 3:
            It has some missing Lifestyle records. Backfill happens.
            Backfilled skip records have this participant's MAX observation_date ('2022-01-01') as its date.
            Backfilled skip records have the newly assigned observation_ids.
        """
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                None,
            'loaded_ids': [
                101, 102, 103, 104, 105, 106, 107, 201, 301, 302, 303
            ],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_concept_id', 'questionnaire_response_id'
            ],
            'cleaned_values': [
                (101, 1, 40766306, self.date_2020, 45905771, 1585857, 111),
                (102, 1, 1585636, self.date_2020, 45905771, 1585636, 111),
                (103, 1, 1586166, self.date_2020, 45905771, 1586166, 111),
                (104, 1, 1586174, self.date_2020, 45905771, 1586174, 111),
                (105, 1, 1586182, self.date_2020, 45905771, 1586182, 111),
                (106, 1, 1586190, self.date_2020, 45905771, 1586190, 111),
                (107, 1, 40766357, self.date_2020, 45905771, 1586198, 111),
                (201, 2, 9999999, self.date_2020, 99999999, 9999999, 222),
                (301, 3, 40766306, self.date_2020, 45905771, 1585857, 333),
                (302, 3, 1585636, self.date_2021, 45905771, 1585636, 333),
                (303, 3, 1586166, self.date_2022, 45905771, 1586166, 333),
                (304, 3, 1586174, self.date_2022, 45905771, 1586174, None),
                (305, 3, 1586182, self.date_2022, 45905771, 1586182, None),
                (306, 3, 1586190, self.date_2022, 45905771, 1586190, None),
                (307, 3, 40766357, self.date_2022, 45905771, 1586198, None)
            ]
        }]

        self.default_test(tables_and_counts)