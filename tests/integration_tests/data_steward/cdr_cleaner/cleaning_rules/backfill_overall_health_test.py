"""
Integration test for BackfillOverallHealth.
"""
# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.backfill_overall_health import BackfillOverallHealth
from common import JINJA_ENV, OBSERVATION, PERSON
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`
(observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id, observation_source_concept_id, questionnaire_response_id)
VALUES
    -- person_id=1 has all OverallHealth records. Nothing happens. --
    (101, 1, 40764338, date('2020-01-01'), 45905771, 1585711, 111),
    (102, 1, 40764339, date('2020-01-01'), 45905771, 1585717, 111),
    (103, 1, 40764340, date('2020-01-01'), 45905771, 1585723, 111),
    (104, 1, 40764341, date('2020-01-01'), 45905771, 1585729, 111),
    (105, 1, 40764342, date('2020-01-01'), 45905771, 1585735, 111),
    (106, 1, 40764343, date('2020-01-01'), 45905771, 1585741, 111),
    (107, 1, 40764344, date('2020-01-01'), 45905771, 1585747, 111),
    (108, 1, 40764345, date('2020-01-01'), 45905771, 1585748, 111),
    (109, 1, 40764346, date('2020-01-01'), 45905771, 1585754, 111),
    (110, 1, 40764347, date('2020-01-01'), 45905771, 1585760, 111),
    (111, 1, 1585766, date('2020-01-01'), 45905771, 1585766, 111),
    (112, 1, 1585772, date('2020-01-01'), 45905771, 1585772, 111),
    (113, 1, 1585778, date('2020-01-01'), 45905771, 1585778, 111),
    (114, 1, 40767407, date('2020-01-01'), 45905771, 1585784, 111),
    (115, 1, 1585803, date('2020-01-01'), 45905771, 1585803, 111),
    (116, 1, 1585815, date('2020-01-01'), 45905771, 1585815, 111),
    -- person_id=2 has NO OverallHealth records. Nothing happens. --
    (201, 2, 9999999, date('2020-01-01'), 99999999, 9999999, 222),
    -- person_id=3 has some missing OverallHealth records. Backfill happens. --
    -- FEMALE participant (gender_concept_id==8532) --
    -- Backfilled skip records have the MAX date ('2022-01-01') as its date. --
    (301, 3, 40764338, date('2020-01-01'), 45905771, 1585711, 333),
    (302, 3, 40764339, date('2021-01-01'), 45905771, 1585717, 333),
    (303, 3, 40764340, date('2022-01-01'), 45905771, 1585723, 333),
    -- person_id=4 has some missing OverallHealth records. Backfill happens. --
    -- NOT-FEMALE participant (gender_concept_id!=8532) --
    -- Backfilled skip records have the MAX date ('2021-01-01') as its date. --
    (401, 4, 40764338, date('2020-01-01'), 45905771, 1585711, 444),
    (402, 4, 40764339, date('2021-01-01'), 45905771, 1585717, 444),
    (403, 4, 40764340, date('2021-01-01'), 45905771, 1585723, 444)
""")

PERSON_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
    (1, 8532, 2001, 999, 99999),
    (2, 8532, 2001, 999, 99999),
    (3, 8532, 2001, 999, 99999),
    (4, 8507, 2001, 999, 99999)
""")


class BackfillOverallHealthTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = BackfillOverallHealth(cls.project_id,
                                                  cls.dataset_id,
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

    def test_backfill_overall_health(self):
        """
        Test cases:
        person_id = 1:
            It has all the OverallHealth records. Nothing happens.
        person_id = 2:
            It has NO OverallHealth records. Nothing happens.
        person_id = 3:
            It has some missing OverallHealth records. Backfill happens.
            FEMALE participant (gender_concept_id==8532), 1585784 will be backfilled.
            Backfilled skip records have this participant's MAX observation_date ('2022-01-01') as its date.
            Backfilled skip records have the newly assigned observation_ids.
        person_id = 4:
            It has some missing OverallHealth records. Backfill happens.
            NOT-FEMALE participant (gender_concept_id!=8532), 1585784 will NOT be backfilled.
            Backfilled skip records have this participant's MAX observation_date ('2021-01-01') as its date.
            Backfilled skip records have the newly assigned observation_ids.
        """
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                None,
            'loaded_ids': [
                101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
                114, 115, 116, 201, 301, 302, 303, 401, 402, 403
            ],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_concept_id', 'questionnaire_response_id'
            ],
            'cleaned_values': [
                (101, 1, 40764338, self.date_2020, 45905771, 1585711, 111),
                (102, 1, 40764339, self.date_2020, 45905771, 1585717, 111),
                (103, 1, 40764340, self.date_2020, 45905771, 1585723, 111),
                (104, 1, 40764341, self.date_2020, 45905771, 1585729, 111),
                (105, 1, 40764342, self.date_2020, 45905771, 1585735, 111),
                (106, 1, 40764343, self.date_2020, 45905771, 1585741, 111),
                (107, 1, 40764344, self.date_2020, 45905771, 1585747, 111),
                (108, 1, 40764345, self.date_2020, 45905771, 1585748, 111),
                (109, 1, 40764346, self.date_2020, 45905771, 1585754, 111),
                (110, 1, 40764347, self.date_2020, 45905771, 1585760, 111),
                (111, 1, 1585766, self.date_2020, 45905771, 1585766, 111),
                (112, 1, 1585772, self.date_2020, 45905771, 1585772, 111),
                (113, 1, 1585778, self.date_2020, 45905771, 1585778, 111),
                (114, 1, 40767407, self.date_2020, 45905771, 1585784, 111),
                (115, 1, 1585803, self.date_2020, 45905771, 1585803, 111),
                (116, 1, 1585815, self.date_2020, 45905771, 1585815, 111),
                (201, 2, 9999999, self.date_2020, 99999999, 9999999, 222),
                (301, 3, 40764338, self.date_2020, 45905771, 1585711, 333),
                (302, 3, 40764339, self.date_2021, 45905771, 1585717, 333),
                (303, 3, 40764340, self.date_2022, 45905771, 1585723, 333),
                (401, 4, 40764338, self.date_2020, 45905771, 1585711, 444),
                (402, 4, 40764339, self.date_2021, 45905771, 1585717, 444),
                (403, 4, 40764340, self.date_2021, 45905771, 1585723, 444),
                (404, 3, 40764341, self.date_2022, 45905771, 1585729, None),
                (405, 3, 40764342, self.date_2022, 45905771, 1585735, None),
                (406, 3, 40764343, self.date_2022, 45905771, 1585741, None),
                (407, 3, 40764344, self.date_2022, 45905771, 1585747, None),
                (408, 3, 40764345, self.date_2022, 45905771, 1585748, None),
                (409, 3, 40764346, self.date_2022, 45905771, 1585754, None),
                (410, 3, 40764347, self.date_2022, 45905771, 1585760, None),
                (411, 3, 1585766, self.date_2022, 45905771, 1585766, None),
                (412, 3, 1585772, self.date_2022, 45905771, 1585772, None),
                (413, 3, 1585778, self.date_2022, 45905771, 1585778, None),
                (414, 3, 40767407, self.date_2022, 45905771, 1585784, None),
                (415, 3, 1585803, self.date_2022, 45905771, 1585803, None),
                (416, 3, 1585815, self.date_2022, 45905771, 1585815, None),
                (417, 4, 40764341, self.date_2021, 45905771, 1585729, None),
                (418, 4, 40764342, self.date_2021, 45905771, 1585735, None),
                (419, 4, 40764343, self.date_2021, 45905771, 1585741, None),
                (420, 4, 40764344, self.date_2021, 45905771, 1585747, None),
                (421, 4, 40764345, self.date_2021, 45905771, 1585748, None),
                (422, 4, 40764346, self.date_2021, 45905771, 1585754, None),
                (423, 4, 40764347, self.date_2021, 45905771, 1585760, None),
                (424, 4, 1585766, self.date_2021, 45905771, 1585766, None),
                (425, 4, 1585772, self.date_2021, 45905771, 1585772, None),
                (426, 4, 1585778, self.date_2021, 45905771, 1585778, None),
                (427, 4, 1585803, self.date_2021, 45905771, 1585803, None),
                (428, 4, 1585815, self.date_2021, 45905771, 1585815, None)
            ]
        }]

        self.default_test(tables_and_counts)