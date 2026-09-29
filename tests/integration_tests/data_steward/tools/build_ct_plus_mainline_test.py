"""
Integration test for build_ct_plus_mainline.

Original Issues: DL2534

Participant 1 is an adult and participant 5 is pediatric. The input holds four
observation rows, each with an _ext row: 11 is adult base content, 12 an adult expanded
row the delta claims, 13 an adult indicators-of-birth row the date-of-birth dataset
claims, and 51 the pediatric participant's row, which the pediatrics dataset claims
with person 5. care_site 1 is referenced by adult and pediatric rows alike and must
survive whole. pediatric_relationship_ext and zip3_ses_map are dropped, and
flanker_task_view, a view over observation, must come out reading the mainline.
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import (CARE_SITE, JINJA_ENV, OBSERVATION,
                    PEDIATRIC_RELATIONSHIP_EXT, PERSON, ZIP3_SES_MAP)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from tools import build_ct_plus_mainline as mainline

OBSERVATION_EXT = f'{OBSERVATION}_ext'
VIEW = 'flanker_task_view'


def _schema(*columns):
    return [{
        "type": data_type,
        "name": name,
        "mode": "nullable"
    } for name, data_type in columns]


EXT_SCHEMA = _schema(('observation_id', 'integer'), ('src_id', 'string'))
RELATIONSHIP_SCHEMA = _schema(('subject_person_id', 'integer'),
                              ('related_person_id', 'integer'))
# An add-on table is read only for its key, so the fixtures carry nothing else.
PERSON_KEY_SCHEMA = _schema(('person_id', 'integer'))
OBSERVATION_KEY_SCHEMA = _schema(('observation_id', 'integer'))
DOB_SCHEMA = _schema(('person_id', 'integer'), ('birth_datetime', 'datetime'))

LOAD_INPUT = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{input}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id,
  care_site_id)
VALUES (1, 0, 1985, 0, 0, 1), (5, 0, 2021, 0, 0, 1);

INSERT INTO `{{project}}.{{input}}.observation`
(observation_id, person_id, observation_concept_id, observation_date,
  observation_type_concept_id)
VALUES (11, 1, 0, '2024-01-01', 0), (12, 1, 0, '2024-01-01', 0),
  (13, 1, 0, '2024-01-01', 0), (51, 5, 0, '2024-01-01', 0);

INSERT INTO `{{project}}.{{input}}.observation_ext` (observation_id, src_id)
VALUES (11, 'EHR 1'), (12, 'EHR 1'), (13, 'EHR 1'), (51, 'EHR 1');

INSERT INTO `{{project}}.{{input}}.care_site` (care_site_id) VALUES (1);

INSERT INTO `{{project}}.{{input}}.{{relationship}}`
(subject_person_id, related_person_id) VALUES (5, 1);

INSERT INTO `{{project}}.{{pediatrics}}.person` (person_id) VALUES (5);
INSERT INTO `{{project}}.{{pediatrics}}.observation` (observation_id) VALUES (51);
INSERT INTO `{{project}}.{{pediatrics}}.observation_ext` (observation_id) VALUES (51);
INSERT INTO `{{project}}.{{pediatrics}}.{{relationship}}`
(subject_person_id, related_person_id) VALUES (5, 1);

INSERT INTO `{{project}}.{{delta}}.observation` (observation_id) VALUES (12);
INSERT INTO `{{project}}.{{delta}}.observation_ext` (observation_id) VALUES (12);

INSERT INTO `{{project}}.{{dob}}.observation` (observation_id) VALUES (13);
INSERT INTO `{{project}}.{{dob}}.observation_ext` (observation_id) VALUES (13);
INSERT INTO `{{project}}.{{dob}}.dob` (person_id, birth_datetime)
VALUES (1, '1985-04-12 00:00:00')
""")


class BuildCtPlusMainlineTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        base = os.environ.get('COMBINED_DATASET_ID')
        cls.input_dataset_id = f'{base}_mainline_input'
        cls.pediatrics_dataset_id = f'{base}_mainline_pediatrics'
        cls.delta_dataset_id = f'{base}_mainline_delta'
        cls.dob_dataset_id = f'{base}_mainline_dob'
        cls.output_dataset_id = f'{base}_mainline_output'
        cls.addon_dataset_ids = [
            cls.pediatrics_dataset_id, cls.delta_dataset_id, cls.dob_dataset_id
        ]

        def fq(dataset, table):
            return f'{cls.project_id}.{dataset}.{table}'

        cls.fq_table_names = [
            fq(cls.input_dataset_id, table)
            for table in [PERSON, OBSERVATION, CARE_SITE]
        ]
        cls.created = {
            fq(cls.input_dataset_id, OBSERVATION_EXT):
                EXT_SCHEMA,
            fq(cls.input_dataset_id, PEDIATRIC_RELATIONSHIP_EXT):
                RELATIONSHIP_SCHEMA,
            fq(cls.pediatrics_dataset_id, PERSON):
                PERSON_KEY_SCHEMA,
            fq(cls.pediatrics_dataset_id, OBSERVATION):
                OBSERVATION_KEY_SCHEMA,
            fq(cls.pediatrics_dataset_id, OBSERVATION_EXT):
                OBSERVATION_KEY_SCHEMA,
            fq(cls.pediatrics_dataset_id, PEDIATRIC_RELATIONSHIP_EXT):
                RELATIONSHIP_SCHEMA,
            fq(cls.delta_dataset_id, OBSERVATION):
                OBSERVATION_KEY_SCHEMA,
            fq(cls.delta_dataset_id, OBSERVATION_EXT):
                OBSERVATION_KEY_SCHEMA,
            fq(cls.dob_dataset_id, OBSERVATION):
                OBSERVATION_KEY_SCHEMA,
            fq(cls.dob_dataset_id, OBSERVATION_EXT):
                OBSERVATION_KEY_SCHEMA,
            fq(cls.dob_dataset_id, 'dob'):
                DOB_SCHEMA,
            fq(cls.input_dataset_id, ZIP3_SES_MAP):
                _schema(('zip3', 'integer')),
        }
        cls.fq_input_view = fq(cls.input_dataset_id, VIEW)
        cls.fq_undeclared = fq(cls.input_dataset_id, 'scratch_counts')
        cls.fq_output = {
            table: fq(cls.output_dataset_id, table) for table in [
                PERSON, OBSERVATION, OBSERVATION_EXT, CARE_SITE,
                PEDIATRIC_RELATIONSHIP_EXT, ZIP3_SES_MAP, VIEW
            ]
        }
        cls.fq_sandbox_table_names = (list(cls.created) +
                                      [cls.fq_undeclared, cls.fq_input_view] +
                                      list(cls.fq_output.values()))

        super().setUpClass()

    def setUp(self):
        super().setUp()

        for fq_table, schema in self.created.items():
            self.client.create_table(Table(fq_table, schema))
        view = Table(self.fq_input_view)
        view.view_query = (f'SELECT observation_id FROM '
                           f'`{self.project_id}.{self.input_dataset_id}.'
                           f'{OBSERVATION}`')
        self.client.create_table(view)
        self.load_test_data([
            LOAD_INPUT.render(project=self.project_id,
                              input=self.input_dataset_id,
                              pediatrics=self.pediatrics_dataset_id,
                              delta=self.delta_dataset_id,
                              dob=self.dob_dataset_id,
                              relationship=PEDIATRIC_RELATIONSHIP_EXT)
        ])

    def _plan(self, addon_dataset_ids=None):
        return mainline.plan(self.client, self.project_id,
                             self.input_dataset_id, addon_dataset_ids or
                             self.addon_dataset_ids, self.pediatrics_dataset_id)

    def test_mainline_is_the_input_minus_every_claimed_row(self):
        input_columns, claims, views = self._plan()
        self.assertEqual(views, {VIEW})
        counts = mainline.check_claims(self.client, self.project_id,
                                       self.input_dataset_id, input_columns,
                                       claims)
        self.assertEqual(counts[OBSERVATION], (4, 3))
        view_queries = mainline.get_view_queries(self.client, self.project_id,
                                                 self.input_dataset_id,
                                                 self.output_dataset_id, views)

        mainline.write_mainline(self.client, self.project_id,
                                self.input_dataset_id, self.output_dataset_id,
                                input_columns, claims, counts, views)
        mainline.recreate_views(self.client, self.project_id,
                                self.output_dataset_id, view_queries)

        self.assertTableValuesMatch(self.fq_output[PERSON], ['person_id'],
                                    [(1,)])
        self.assertTableValuesMatch(self.fq_output[OBSERVATION],
                                    ['observation_id', 'person_id'], [(11, 1)])
        self.assertTableValuesMatch(self.fq_output[OBSERVATION_EXT],
                                    ['observation_id'], [(11,)])
        self.assertTableValuesMatch(self.fq_output[CARE_SITE], ['care_site_id'],
                                    [(1,)])
        self.assertTableDoesNotExist(self.fq_output[PEDIATRIC_RELATIONSHIP_EXT])
        self.assertTableDoesNotExist(self.fq_output[ZIP3_SES_MAP])
        self.assertTableValuesMatch(self.fq_output[VIEW], ['observation_id'],
                                    [(11,)])

    def test_an_add_on_row_matching_nothing_in_the_input_is_refused(self):
        self.client.query(
            f'INSERT INTO `{self.project_id}.{self.delta_dataset_id}.'
            f'{OBSERVATION}` (observation_id) VALUES (99)').result()
        input_columns, claims, _ = self._plan()

        with self.assertRaises(RuntimeError) as ctx:
            mainline.check_claims(self.client, self.project_id,
                                  self.input_dataset_id, input_columns, claims)

        self.assertIn('hold 4 rows', str(ctx.exception))

    def test_an_undeclared_input_table_is_refused(self):
        self.client.create_table(
            Table(self.fq_undeclared, _schema(('person_id', 'integer'))))

        with self.assertRaises(RuntimeError) as ctx:
            self._plan()

        self.assertIn("['scratch_counts']", str(ctx.exception))

    def test_a_named_add_on_dataset_that_does_not_exist_is_refused(self):
        absent = f'{self.input_dataset_id}_absent'

        with self.assertRaises(RuntimeError) as ctx:
            self._plan(self.addon_dataset_ids + [absent])

        self.assertIn(f'{absent} does not exist', str(ctx.exception))
