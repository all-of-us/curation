"""
Integration test for split_ct_plus_expanded_concepts.

Original Issues: DL-2538

Participant 1 is eligible. The clean fixture carries a CT-suppressed row, a
single-element expanded row, a two-element expanded row and their _ext rows, plus a
pediatric participant's expanded row, which no element dataset claims. Each refusal test
adds one row the split must not publish: an AIAN participant's, one not keyed to a CT+
research ID, and one in visit_occurrence, which an add-on cannot carry.

The concept set is the shipped one. The vocabulary tables hold one concept for each
vehicle rule, so the vehicular list's table scope is exercised too.
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.ct_plus_expanded_concepts import (
    VEHICLE_LOOKUPS, build_expanded_lookup)
from common import (AIAN_LIST, CONCEPT, CONCEPT_ANCESTOR, CONCEPT_RELATIONSHIP,
                    CONDITION_OCCURRENCE, CT_PLUS_EXPANDED_CONCEPTS,
                    CT_PLUS_PEDIATRIC_COHORT, JINJA_ENV, OBSERVATION, PERSON,
                    VISIT_OCCURRENCE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from tools import split_ct_plus_expanded_concepts as split

IDS_VIEW = 'research_ids'

# Filed under suicide.csv only
SUICIDE = 432507
# Filed under both assault.csv and a suffocation label
ASSAULT_SUFFOCATION = 19192
# Filed under location, which stays suppressed in CT+
LOCATION = 2721594
# A descendant of the vehicular rule's ancestor concept, on no privacy CSV
VEHICULAR_ANCESTOR = 4054924
VEHICULAR_ONLY = 999000001

INPUT_TABLES = [
    PERSON, CONDITION_OCCURRENCE, OBSERVATION, f'{OBSERVATION}_ext',
    VISIT_OCCURRENCE, CONCEPT, CONCEPT_ANCESTOR, CONCEPT_RELATIONSHIP
]
OUTPUT_TABLES = [
    PERSON, CONDITION_OCCURRENCE, OBSERVATION, f'{OBSERVATION}_ext',
    VISIT_OCCURRENCE
]
DATA_ELEMENTS = [
    'assault_homicide', 'motor_vehicle_accident', 'suffocation', 'suicide'
]

IDS_SCHEMA = [
    {
        "type": "integer",
        "name": name,
        "mode": "nullable"
    } for name in
    ['participant_id', 'controlled_tier_id', 'controlled_tier_plus_id']
]

LOOKUP_SCHEMA = [{"type": "integer", "name": "person_id", "mode": "nullable"}]

# Participant IDs 900x, CT IDs 800x, CT+ IDs x. Participant 9003 is AIAN and 5 is the
# pediatric cohort, which the cohort table holds by CT+ ID.
#   condition 1: single-element expanded row
#   condition 2: two-element expanded row
#   condition 3: suppressed concept, which no upstream rule removed in this fixture
#   condition 4: pediatric participant's expanded row
#   condition 5: reached by the vehicular rule, which covers this table
#   observation 11: expanded, with its _ext row
#   observation 12: the vehicular concept, in a table that rule does not reach
LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{input}}.concept_ancestor`
(ancestor_concept_id, descendant_concept_id, min_levels_of_separation,
 max_levels_of_separation)
VALUES ({{vehicular_ancestor}}, {{vehicular_only}}, 1, 1);

INSERT INTO `{{project}}.{{input}}.concept`
(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code,
 valid_start_date, valid_end_date)
VALUES ({{vehicular_only}}, 'test', 'Condition', 'SNOMED', 'Clinical Finding', 'test',
        '2000-01-01', '2099-12-31');

INSERT INTO `{{project}}.{{input}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES (1, 0, 1980, 0, 0), (3, 0, 1975, 0, 0), (5, 0, 2021, 0, 0);

INSERT INTO `{{project}}.{{input}}.condition_occurrence`
(condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
 condition_start_datetime, condition_type_concept_id)
VALUES
  (1, 1, {{suicide}}, '2010-01-01', '2010-01-01 00:00:00', 0),
  (2, 1, {{assault_suffocation}}, '2010-01-01', '2010-01-01 00:00:00', 0),
  (3, 1, {{location}}, '2010-01-01', '2010-01-01 00:00:00', 0),
  (4, 5, {{suicide}}, '2022-01-01', '2022-01-01 00:00:00', 0),
  (5, 1, {{vehicular_only}}, '2010-01-01', '2010-01-01 00:00:00', 0);

INSERT INTO `{{project}}.{{input}}.observation`
(observation_id, person_id, observation_concept_id, observation_date,
 observation_type_concept_id)
VALUES
  (11, 1, {{suicide}}, '2010-01-01', 0),
  (12, 1, {{vehicular_only}}, '2010-01-01', 0);

INSERT INTO `{{project}}.{{input}}.observation_ext` (observation_id, src_id)
VALUES (11, 'EHR site 1'), (12, 'EHR site 1');

INSERT INTO `{{project}}.{{sandbox}}.{{ids_view}}`
(participant_id, controlled_tier_id, controlled_tier_plus_id)
VALUES (9001, 8001, 1), (9003, 8003, 3), (9005, 8005, 5);

INSERT INTO `{{project}}.{{sandbox}}.{{aian_list}}` (person_id) VALUES (9003);

INSERT INTO `{{project}}.{{sandbox}}.{{cohort}}` (person_id) VALUES (5)
""")

CONDITION_INSERT = (
    "INSERT INTO `{{input}}.condition_occurrence` "
    "(condition_occurrence_id, person_id, condition_concept_id, "
    "condition_start_date, condition_start_datetime, condition_type_concept_id) "
    "VALUES (6, {person_id}, {concept}, '2010-01-01', '2010-01-01 00:00:00', 0)"
)

BAD_ROWS = {
    'aian_rows':
        CONDITION_INSERT.format(person_id=3, concept=SUICIDE),
    'non_ct_plus_rows':
        CONDITION_INSERT.format(person_id=8001, concept=SUICIDE),
    'uncarriable':
        f"INSERT INTO `{{input}}.visit_occurrence` "
        f"(visit_occurrence_id, person_id, visit_concept_id, visit_start_date, "
        f"visit_end_date, visit_type_concept_id, visit_source_concept_id) "
        f"VALUES (1, 1, 0, '2010-01-01', '2010-01-01', 0, {SUICIDE})",
}

REMOVE_BAD_ROWS = [
    "DELETE FROM `{input}.condition_occurrence` WHERE condition_occurrence_id = 6",
    "DELETE FROM `{input}.visit_occurrence` WHERE TRUE",
]


class SplitCtPlusExpandedConceptsTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        # Dedicated datasets: the split reads every table it finds, so a shared dataset
        # would pull in other tests' tables.
        base = os.environ.get('COMBINED_DATASET_ID')
        cls.input_dataset_id = f'{base}_expanded_split_input'
        cls.sandbox_dataset_id = f'{base}_expanded_split_sandbox'
        cls.output_dataset_ids = {
            element: f'{base}_expanded_split_{element}'
            for element in DATA_ELEMENTS
        }

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.input_dataset_id}.{table}'
            for table in INPUT_TABLES
        ]
        cls.extra_tables = {
            f'{cls.project_id}.{cls.sandbox_dataset_id}.{IDS_VIEW}':
                IDS_SCHEMA,
            f'{cls.project_id}.{cls.sandbox_dataset_id}.{AIAN_LIST}':
                LOOKUP_SCHEMA,
            f'{cls.project_id}.{cls.sandbox_dataset_id}.{CT_PLUS_PEDIATRIC_COHORT}':
                LOOKUP_SCHEMA,
        }
        cls.fq_sandbox_table_names = list(cls.extra_tables) + [
            f'{cls.project_id}.{cls.sandbox_dataset_id}.{table}'
            for table in [CT_PLUS_EXPANDED_CONCEPTS, *VEHICLE_LOOKUPS.values()]
        ] + [
            f'{cls.project_id}.{dataset}.{table}'
            for dataset in cls.output_dataset_ids.values()
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
                             cohort=CT_PLUS_PEDIATRIC_COHORT,
                             suicide=SUICIDE,
                             assault_suffocation=ASSAULT_SUFFOCATION,
                             location=LOCATION,
                             vehicular_ancestor=VEHICULAR_ANCESTOR,
                             vehicular_only=VEHICULAR_ONLY)
        ])
        build_expanded_lookup(self.client, self.project_id,
                              self.input_dataset_id, self.sandbox_dataset_id)

    def _classify(self):
        columns = split.get_table_columns(self.client, self.project_id,
                                          self.input_dataset_id)
        views = split.get_views(self.client, self.project_id,
                                self.input_dataset_id)
        carried, uncarriable = split.classify_tables(columns, views)
        return carried, uncarriable, split.get_shape_tables(columns, views)

    def _assert(self, carried, uncarriable):
        # The fixture keeps the lookups and the ids stand-in in one dataset
        split.assert_split_is_publishable(self.client, self.project_id,
                                          self.input_dataset_id,
                                          self.sandbox_dataset_id,
                                          self.sandbox_dataset_id,
                                          self.sandbox_dataset_id, IDS_VIEW,
                                          carried, uncarriable)

    def _write(self, carried, shape_tables, elements):
        for element in elements:
            dataset = self.output_dataset_ids[element]
            self.client.create_dataset(dataset, exists_ok=True)
            split.write_element(self.client, self.project_id,
                                self.input_dataset_id, self.sandbox_dataset_id,
                                dataset, element, carried, shape_tables)

    def _output(self, element, table):
        return (f'{self.project_id}.{self.output_dataset_ids[element]}.{table}')

    def test_each_element_holds_its_rows_with_ids_unchanged(self):
        carried, uncarriable, shape_tables = self._classify()
        self.assertIn(CONDITION_OCCURRENCE, carried)
        self.assertIn(VISIT_OCCURRENCE, uncarriable)

        self._assert(carried, uncarriable)
        self._write(carried, shape_tables, DATA_ELEMENTS)
        split.assert_nothing_left_behind(self.client, self.project_id,
                                         self.input_dataset_id,
                                         self.sandbox_dataset_id,
                                         list(self.output_dataset_ids.values()),
                                         carried)

        condition = ['condition_occurrence_id', 'person_id']
        self.assertTableValuesMatch(
            self._output('suicide', CONDITION_OCCURRENCE), condition, [(1, 1)])
        self.assertTableValuesMatch(self._output('suicide', OBSERVATION),
                                    ['observation_id', 'person_id'], [(11, 1)])
        self.assertTableValuesMatch(
            self._output('suicide', f'{OBSERVATION}_ext'),
            ['observation_id', 'src_id'], [(11, 'EHR site 1')])

        # A two-element concept reaches both datasets
        for element in ['assault_homicide', 'suffocation']:
            self.assertTableValuesMatch(
                self._output(element, CONDITION_OCCURRENCE), condition,
                [(2, 1)])

        # The vehicular list reaches condition_occurrence, not observation
        self.assertTableValuesMatch(
            self._output('motor_vehicle_accident', CONDITION_OCCURRENCE),
            condition, [(5, 1)])
        self.assertTableValuesMatch(
            self._output('motor_vehicle_accident', OBSERVATION),
            ['observation_id', 'person_id'], [])

        # Full table shape, with no parent rows
        for table in [PERSON, VISIT_OCCURRENCE]:
            self.assertTableValuesMatch(self._output('suicide', table),
                                        ['person_id'], [])

    def test_an_unwritten_element_is_caught(self):
        carried, _, shape_tables = self._classify()
        self._write(carried, shape_tables, ['suicide'])

        with self.assertRaises(RuntimeError) as ctx:
            split.assert_nothing_left_behind(
                self.client, self.project_id, self.input_dataset_id,
                self.sandbox_dataset_id, [self.output_dataset_ids['suicide']],
                carried)

        self.assertIn(CONDITION_OCCURRENCE, str(ctx.exception))

    def test_a_pediatric_parent_row_is_left_to_the_pediatrics_add_on(self):
        carried, uncarriable, _ = self._classify()
        fq_input = f'{self.project_id}.{self.input_dataset_id}'
        self.client.query(
            f"INSERT INTO `{fq_input}.visit_occurrence` "
            f"(visit_occurrence_id, person_id, visit_concept_id, visit_start_date, "
            f"visit_end_date, visit_type_concept_id, visit_source_concept_id) "
            f"VALUES (2, 5, 0, '2022-01-01', '2022-01-01', 0, {SUICIDE})"
        ).result()

        self._assert(carried, uncarriable)

        self.client.query(
            f"DELETE FROM `{fq_input}.visit_occurrence` WHERE TRUE").result()

    def test_each_unpublishable_row_is_refused(self):
        carried, uncarriable, _ = self._classify()
        fq_input = f'{self.project_id}.{self.input_dataset_id}'

        for check, insert in BAD_ROWS.items():
            with self.subTest(check=check):
                self.client.query(insert.format(input=fq_input)).result()

                with self.assertRaises(RuntimeError):
                    self._assert(carried, uncarriable)

                for delete in REMOVE_BAD_ROWS:
                    self.client.query(delete.format(input=fq_input)).result()
