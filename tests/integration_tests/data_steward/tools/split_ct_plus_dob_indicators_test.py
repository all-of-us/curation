"""
Integration test for split_ct_plus_dob_indicators.

Original Issues: DL-2495

Participant 1 is eligible. The clean fixture carries one birth row in
condition_occurrence and one in observation, with its _ext row, beside rows carrying
no birth concept. Each refusal test adds one row the split must not publish: an AIAN
participant's, a pediatric participant's, one not keyed to a CT+ research ID, and one
in death, which the add-on cannot carry.
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import (AIAN_LIST, CONDITION_OCCURRENCE, DEATH, JINJA_ENV,
                    OBSERVATION, PERSON, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from tools import split_ct_plus_dob_indicators as dob

IDS_VIEW = 'research_ids'

# O99.814, filed under liveborn
LIVEBORN = 45582600
# The PPI date-of-birth question
PPI_DATE_OF_BIRTH = 1585259

INPUT_TABLES = [
    PERSON, CONDITION_OCCURRENCE, OBSERVATION, f'{OBSERVATION}_ext', DEATH
]
OUTPUT_TABLES = [CONDITION_OCCURRENCE, OBSERVATION, f'{OBSERVATION}_ext']

IDS_SCHEMA = [
    {
        "type": "integer",
        "name": name,
        "mode": "nullable"
    } for name in
    ['participant_id', 'controlled_tier_id', 'controlled_tier_plus_id']
]

LOOKUP_SCHEMA = [{"type": "integer", "name": "person_id", "mode": "nullable"}]

# Participant IDs 900x, CT IDs 800x, CT+ IDs x. Participant 9003 is AIAN and 9005 is
# pediatric.
LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{input}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES (1, 0, 1980, 0, 0), (3, 0, 1975, 0, 0), (5, 0, 2021, 0, 0);

INSERT INTO `{{project}}.{{input}}.condition_occurrence`
(condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
 condition_start_datetime, condition_type_concept_id, condition_source_concept_id)
VALUES
  (1, 1, 0, '2010-01-01', '2010-01-01 00:00:00', 0, {{liveborn}}),
  (2, 1, 320128, '2010-01-01', '2010-01-01 00:00:00', 0, 0);

INSERT INTO `{{project}}.{{input}}.observation`
(observation_id, person_id, observation_concept_id, observation_date,
 observation_type_concept_id)
VALUES (11, 1, {{dob}}, '2010-01-01', 0), (12, 1, 0, '2010-01-01', 0);

INSERT INTO `{{project}}.{{input}}.observation_ext` (observation_id, src_id)
VALUES (11, 'PPI/PM'), (12, 'PPI/PM');

INSERT INTO `{{project}}.{{sandbox}}.{{ids_view}}`
(participant_id, controlled_tier_id, controlled_tier_plus_id)
VALUES (9001, 8001, 1), (9003, 8003, 3), (9005, 8005, 5);

INSERT INTO `{{project}}.{{sandbox}}.{{aian_list}}` (person_id) VALUES (9003);

INSERT INTO `{{project}}.{{sandbox}}.{{under18_table}}` (person_id) VALUES (9005)
""")

BAD_ROWS = {
    'aian_rows':
        f"INSERT INTO `{{input}}.condition_occurrence` "
        f"(condition_occurrence_id, person_id, condition_concept_id, "
        f"condition_start_date, condition_start_datetime, "
        f"condition_type_concept_id, condition_source_concept_id) "
        f"VALUES (3, 3, 0, '2010-01-01', '2010-01-01 00:00:00', 0, {LIVEBORN})",
    'pediatric_rows':
        f"INSERT INTO `{{input}}.condition_occurrence` "
        f"(condition_occurrence_id, person_id, condition_concept_id, "
        f"condition_start_date, condition_start_datetime, "
        f"condition_type_concept_id, condition_source_concept_id) "
        f"VALUES (3, 5, 0, '2022-01-01', '2022-01-01 00:00:00', 0, {LIVEBORN})",
    'non_ct_plus_rows':
        f"INSERT INTO `{{input}}.condition_occurrence` "
        f"(condition_occurrence_id, person_id, condition_concept_id, "
        f"condition_start_date, condition_start_datetime, "
        f"condition_type_concept_id, condition_source_concept_id) "
        f"VALUES (3, 8001, 0, '2010-01-01', '2010-01-01 00:00:00', 0, "
        f"{LIVEBORN})",
    'uncarriable':
        f"INSERT INTO `{{input}}.death` "
        f"(person_id, death_date, death_type_concept_id, cause_concept_id) "
        f"VALUES (1, '2020-01-01', 0, {LIVEBORN})",
}

REMOVE_BAD_ROWS = [
    "DELETE FROM `{input}.condition_occurrence` WHERE condition_occurrence_id = 3",
    "DELETE FROM `{input}.death` WHERE TRUE",
]


class SplitCtPlusDobIndicatorsTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        # Dedicated datasets: the split reads every person-keyed table it finds, so
        # a shared dataset would pull in other tests' tables.
        base = os.environ.get('COMBINED_DATASET_ID')
        cls.input_dataset_id = f'{base}_dob_split_input'
        cls.sandbox_dataset_id = f'{base}_dob_split_sandbox'
        cls.output_dataset_id = f'{base}_dob_split_output'

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.input_dataset_id}.{table}'
            for table in INPUT_TABLES
        ]
        cls.extra_tables = {
            f'{cls.project_id}.{cls.sandbox_dataset_id}.{IDS_VIEW}':
                IDS_SCHEMA,
            f'{cls.project_id}.{cls.sandbox_dataset_id}.{AIAN_LIST}':
                LOOKUP_SCHEMA,
            f'{cls.project_id}.{cls.sandbox_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}':
                LOOKUP_SCHEMA,
        }
        cls.fq_sandbox_table_names = list(cls.extra_tables) + [
            f'{cls.project_id}.{cls.output_dataset_id}.{table}'
            for table in OUTPUT_TABLES
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        for fq_table, schema in self.extra_tables.items():
            self.client.create_table(Table(fq_table, schema), exists_ok=True)
        self.load_test_data([
            LOAD_DATA.render(project=self.project_id,
                             input=self.input_dataset_id,
                             sandbox=self.sandbox_dataset_id,
                             ids_view=IDS_VIEW,
                             aian_list=AIAN_LIST,
                             under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                             liveborn=LIVEBORN,
                             dob=PPI_DATE_OF_BIRTH)
        ])

    def _classify(self):
        return dob.classify_tables(self.client, self.project_id,
                                   self.input_dataset_id)

    def _assert(self, carried, uncarriable, concept_ids):
        # The fixture keeps both lookups and the ids stand-in in one dataset
        dob.assert_split_is_publishable(self.client, self.project_id,
                                        self.input_dataset_id,
                                        self.sandbox_dataset_id,
                                        self.sandbox_dataset_id,
                                        self.sandbox_dataset_id, IDS_VIEW,
                                        carried, uncarriable, concept_ids)

    def test_birth_rows_are_split_with_their_ext_rows(self):
        carried, uncarriable = self._classify()
        concept_ids = dob.get_dob_indicator_concept_ids()
        self.assertIn(CONDITION_OCCURRENCE, carried)
        self.assertIn(DEATH, uncarriable)

        self._assert(carried, uncarriable, concept_ids)
        self.client.create_dataset(self.output_dataset_id, exists_ok=True)
        dob.split(self.client, self.project_id, self.input_dataset_id,
                  self.output_dataset_id, carried, concept_ids)

        output = f'{self.project_id}.{self.output_dataset_id}'
        self.assertTableValuesMatch(f'{output}.{CONDITION_OCCURRENCE}',
                                    ['condition_occurrence_id', 'person_id'],
                                    [(1, 1)])
        self.assertTableValuesMatch(f'{output}.{OBSERVATION}',
                                    ['observation_id', 'person_id'], [(11, 1)])
        self.assertTableValuesMatch(f'{output}.{OBSERVATION}_ext',
                                    ['observation_id', 'src_id'],
                                    [(11, 'PPI/PM')])

    def test_each_unpublishable_row_is_refused(self):
        carried, uncarriable = self._classify()
        concept_ids = dob.get_dob_indicator_concept_ids()
        fq_input = f'{self.project_id}.{self.input_dataset_id}'

        for check, insert in BAD_ROWS.items():
            with self.subTest(check=check):
                self.client.query(insert.format(input=fq_input)).result()

                with self.assertRaises(RuntimeError):
                    self._assert(carried, uncarriable, concept_ids)

                for delete in REMOVE_BAD_ROWS:
                    self.client.query(delete.format(input=fq_input)).result()
