"""
Integration test for BackfillTheBasics.
"""
# Python Imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.backfill_the_basics import BackfillTheBasics
from common import JINJA_ENV, OBSERVATION, PERSON, MAPPING_PREFIX
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`
(observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id, observation_source_concept_id)
VALUES
    -- person_id=1 has all TheBasics records. Nothing happens. --
    (101, 1, 1585370, date('2020-01-01'), 45905771, 1585370),
    (102, 1, 46235933, date('2020-01-01'), 45905771, 1585375),
    (103, 1, 40766240, date('2020-01-01'), 45905771, 1585386),
    (104, 1, 1585389, date('2020-01-01'), 45905771, 1585389),
    (105, 1, 1585838, date('2020-01-01'), 45905771, 1585838),
    (106, 1, 1585879, date('2020-01-01'), 45905771, 1585879),
    (107, 1, 1585886, date('2020-01-01'), 45905771, 1585886),
    (108, 1, 1585889, date('2020-01-01'), 45905771, 1585889),
    (109, 1, 1585890, date('2020-01-01'), 45905771, 1585890),
    (110, 1, 3046344, date('2020-01-01'), 45905771, 1585892),
    (111, 1, 1585899, date('2020-01-01'), 45905771, 1585899),
    (112, 1, 40771091, date('2020-01-01'), 45905771, 1585940),
    (113, 1, 40771090, date('2020-01-01'), 45905771, 1585952),
    (114, 1, 3005917, date('2020-01-01'), 45905771, 1586135),
    (115, 1, 1586140, date('2020-01-01'), 45905771, 1586140),
    -- person_id=2 has NO TheBasics records. Nothing happens. --
    (201, 2, 9999999, date('2020-01-01'), 99999999, 9999999),
    -- person_id=3 has some missing TheBasics records. Backfill happens. --
    -- Backfilled skip records have the MAX date ('2022-01-01') as its date. --
    (301, 3, 1585370, date('2020-01-01'), 45905771, 1585370),
    (302, 3, 46235933, date('2021-01-01'), 45905771, 1585375),
    (303, 3, 40766240, date('2022-01-01'), 45905771, 1585386)
""")

PERSON_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
    (1, 8532, 2001, 999, 99999),
    (2, 8532, 2001, 999, 99999),
    (3, 8532, 2001, 999, 99999)
""")

MAPPING_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{dataset}}._mapping_observation`
    (observation_id INT64, src_id STRING)
    ;
INSERT INTO `{{project}}.{{dataset}}._mapping_observation`
(observation_id, src_id)
VALUES
    (101, 'src_1'),
    (102, 'src_1'),
    (103, 'src_1'),
    (104, 'src_1'),
    (105, 'src_1'),
    (106, 'src_1'),
    (107, 'src_1'),
    (108, 'src_1'),
    (109, 'src_1'),
    (110, 'src_1'),
    (111, 'src_1'),
    (112, 'src_1'),
    (113, 'src_1'),
    (114, 'src_1'),
    (115, 'src_1'),
    (301, 'src_2'),
    (302, 'src_2'),
    (303, 'src_2')
""")

class BackfillTheBasicsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = BackfillTheBasics(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id)

        cls.fq_sandbox_table_names = []

        # NOTE _mapping_observation is not in cls.fq_table_names because its columns are different from the ones
        # defined in the resource_files folder. It has the columns defined in `create_rdr_snapshot.py` instead.
        cls.fq_mapping_table_name = f'{cls.project_id}.{cls.dataset_id}.{MAPPING_PREFIX}{OBSERVATION}'

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

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
        insert_mapping = MAPPING_TMPL.render(project=self.project_id,
                                             dataset=self.dataset_id)

        queries = [insert_observation, insert_person, insert_mapping]
        self.load_test_data(queries)

    def test_backfill_the_basics(self):
        """
        Test cases:
        person_id = 1:
            It has all TheBasics records. Nothing happens.
        person_id = 2:
            It has NO TheBasics records. Nothing happens.
        person_id = 3:
            It has some missing TheBasics records. Backfill happens.
            Backfilled skip records have this participant's MAX observation_date ('2022-01-01') as its date.
            Backfilled skip records have the newly assigned observation_ids.
        """
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
                114, 115, 201, 301, 302, 303
            ],
            'sandboxed_ids': [304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_concept_id'
            ],
            'cleaned_values': [
                (101, 1, 1585370, self.date_2020, 45905771, 1585370),
                (102, 1, 46235933, self.date_2020, 45905771, 1585375),
                (103, 1, 40766240, self.date_2020, 45905771, 1585386),
                (104, 1, 1585389, self.date_2020, 45905771, 1585389),
                (105, 1, 1585838, self.date_2020, 45905771, 1585838),
                (106, 1, 1585879, self.date_2020, 45905771, 1585879),
                (107, 1, 1585886, self.date_2020, 45905771, 1585886),
                (108, 1, 1585889, self.date_2020, 45905771, 1585889),
                (109, 1, 1585890, self.date_2020, 45905771, 1585890),
                (110, 1, 3046344, self.date_2020, 45905771, 1585892),
                (111, 1, 1585899, self.date_2020, 45905771, 1585899),
                (112, 1, 40771091, self.date_2020, 45905771, 1585940),
                (113, 1, 40771090, self.date_2020, 45905771, 1585952),
                (114, 1, 3005917, self.date_2020, 45905771, 1586135),
                (115, 1, 1586140, self.date_2020, 45905771, 1586140),
                (201, 2, 9999999, self.date_2020, 99999999, 9999999),
                (301, 3, 1585370, self.date_2020, 45905771, 1585370),
                (302, 3, 46235933, self.date_2021, 45905771, 1585375),
                (303, 3, 40766240, self.date_2022, 45905771, 1585386),
                (304, 3, 1585389, self.date_2022, 45905771, 1585389),
                (305, 3, 1585838, self.date_2022, 45905771, 1585838),
                (306, 3, 1585879, self.date_2022, 45905771, 1585879),
                (307, 3, 1585886, self.date_2022, 45905771, 1585886),
                (308, 3, 1585889, self.date_2022, 45905771, 1585889),
                (309, 3, 1585890, self.date_2022, 45905771, 1585890),
                (310, 3, 3046344, self.date_2022, 45905771, 1585892),
                (311, 3, 1585899, self.date_2022, 45905771, 1585899),
                (312, 3, 40771091, self.date_2022, 45905771, 1585940),
                (313, 3, 40771090, self.date_2022, 45905771, 1585952),
                (314, 3, 3005917, self.date_2022, 45905771, 1586135),
                (315, 3, 1586140, self.date_2022, 45905771, 1586140)
            ]
        }, {
            'fq_table_name':
                self.fq_mapping_table_name,
            'loaded_ids': [
                101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 301, 302, 303
            ],
            'fields': ['observation_id', 'src_id'],
            'cleaned_values': [(101, 'src_1'), (102, 'src_1'), (103, 'src_1'),
                               (104, 'src_1'), (105, 'src_1'), (106, 'src_1'),
                               (107, 'src_1'), (108, 'src_1'), (109, 'src_1'),
                               (110, 'src_1'), (111, 'src_1'), (112, 'src_1'),
                               (113, 'src_1'), (114, 'src_1'), (115, 'src_1'),
                               (301, 'src_2'), (302, 'src_2'), (303, 'src_2'),
                               (304, 'src_2'), (305, 'src_2'), (306, 'src_2'),
                               (307, 'src_2'), (308, 'src_2'), (309, 'src_2'),
                               (310, 'src_2'), (311, 'src_2'), (312, 'src_2'),
                               (313, 'src_2'), (314, 'src_2'), (315, 'src_2'),
                               ]
        }]

        self.default_test(tables_and_counts)

    def tearDown(self):
        self.client.delete_table(self.fq_mapping_table_name, not_found_ok=True)
        super().tearDown()