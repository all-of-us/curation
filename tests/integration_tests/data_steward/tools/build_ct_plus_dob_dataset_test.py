"""
Integration test for build_ct_plus_dob_dataset.

Original Issues: DL2535

Participants 1 and 2 are in the input. The clean fixture captures both, and the output
dataset already holds one indicators-of-birth table, standing in for the split. Each
refusal test adds one row the tool must not publish: a participant absent from the
input, a repeated participant, a row still on its CT research ID, an AIAN participant,
a pediatric participant, and a row with no birth_datetime.
"""
# Python imports
import os
from datetime import datetime

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import (AIAN_LIST, CT_PLUS_BIRTHDATE_TABLE, CT_PLUS_DOB_INDICATORS,
                    JINJA_ENV, OBSERVATION, PERSON,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from tools import build_ct_plus_dob_dataset as dob

IDS_VIEW = 'research_ids'

CAPTURE_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}, {
    "type": "datetime",
    "name": "birth_datetime",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "month_of_birth",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "day_of_birth",
    "mode": "nullable"
}]

IDS_SCHEMA = [
    {
        "type": "integer",
        "name": name,
        "mode": "nullable"
    } for name in
    ['participant_id', 'controlled_tier_id', 'controlled_tier_plus_id']
]

# Both lookups are keyed by participant ID; the tool reads only person_id from each.
LOOKUP_SCHEMA = [{"type": "integer", "name": "person_id", "mode": "nullable"}]

# The split's event tables keep their CDM schema; the tool only needs one to exist.
EVENT_SCHEMA = [{
    "type": "integer",
    "name": "observation_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}]

# Participant IDs 900x, CT IDs 800x, CT+ IDs x. Participant 9003 is AIAN and 9005 is
# pediatric. Both are in the input, and would only reach the capture if the capture's
# exclusion regressed.
LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{input}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES (1, 0, 1985, 0, 0), (2, 0, 1990, 0, 0), (3, 0, 1970, 0, 0),
  (5, 0, 2021, 0, 0);

INSERT INTO `{{project}}.{{ids_dataset}}.{{ids_view}}`
(participant_id, controlled_tier_id, controlled_tier_plus_id)
VALUES (9001, 8001, 1), (9002, 8002, 2), (9003, 8003, 3), (9004, 8004, 4),
  (9005, 8005, 5);

INSERT INTO `{{project}}.{{rdr_sandbox}}.{{aian_list}}` (person_id)
VALUES (9003);

INSERT INTO `{{project}}.{{rdr_sandbox}}.{{under18_table}}` (person_id)
VALUES (9005);

INSERT INTO `{{project}}.{{capture}}.{{capture_table}}`
(person_id, birth_datetime, month_of_birth, day_of_birth)
VALUES
  (1, '1985-04-12 00:00:00', 4, 12),
  (2, '1990-11-03 00:00:00', 11, 3);

INSERT INTO `{{project}}.{{output}}.{{event_table}}` (observation_id, person_id)
VALUES (101, 1), (102, 2)
""")

BAD_ROWS = {
    'unresolved_rows': (4, "'1960-01-01 00:00:00'"),
    'duplicate_rows': (1, "'1985-04-13 00:00:00'"),
    'non_ct_plus_rows': (8002, "'1990-11-03 00:00:00'"),
    'aian_rows': (3, "'1970-06-01 00:00:00'"),
    'pediatric_rows': (5, "'2021-02-01 00:00:00'"),
    'missing_birth_datetime_rows': (2, 'NULL'),
}

# month_of_birth 13 marks the added row so it can be removed again.
ADD_ROW = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{capture}}.{{capture_table}}`
(person_id, birth_datetime, month_of_birth, day_of_birth)
VALUES ({{person_id}}, {{birth_datetime}}, 13, 1)
""")


class BuildCtPlusDobDatasetTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        # Dedicated datasets, so the resolve check reads only this fixture's person
        # rows and the output holds only the stand-in event table and what the tool
        # writes.
        base = os.environ.get('COMBINED_DATASET_ID')
        cls.input_dataset_id = f'{base}_dob_input'
        cls.capture_dataset_id = f'{base}_dob_sandbox'
        cls.output_dataset_id = f'{base}_dob_output'

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.input_dataset_id}.{PERSON}'
        ]
        cls.fq_capture = (f'{cls.project_id}.{cls.capture_dataset_id}.'
                          f'{CT_PLUS_BIRTHDATE_TABLE}')
        cls.fq_ids = f'{cls.project_id}.{cls.capture_dataset_id}.{IDS_VIEW}'
        cls.fq_aian = f'{cls.project_id}.{cls.capture_dataset_id}.{AIAN_LIST}'
        cls.fq_under18 = (f'{cls.project_id}.{cls.capture_dataset_id}.'
                          f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}')
        cls.fq_event = (f'{cls.project_id}.{cls.output_dataset_id}.'
                        f'{OBSERVATION}')
        cls.fq_output = (f'{cls.project_id}.{cls.output_dataset_id}.'
                         f'{dob.DELIVERED_TABLE}')
        cls.fq_sandbox_table_names = [
            cls.fq_capture, cls.fq_ids, cls.fq_aian, cls.fq_under18,
            cls.fq_event, cls.fq_output
        ]

        super().setUpClass()

        # The split labels the dataset it creates, and the tool checks the label.
        dataset = cls.client.get_dataset(
            f'{cls.project_id}.{cls.output_dataset_id}')
        dataset.labels = dict(dataset.labels or {},
                              ct_plus_component=CT_PLUS_DOB_INDICATORS)
        cls.client.update_dataset(dataset, ['labels'])

    def setUp(self):
        super().setUp()

        self.client.create_table(Table(self.fq_capture, CAPTURE_SCHEMA))
        self.client.create_table(Table(self.fq_ids, IDS_SCHEMA))
        self.client.create_table(Table(self.fq_aian, LOOKUP_SCHEMA))
        self.client.create_table(Table(self.fq_under18, LOOKUP_SCHEMA))
        self.client.create_table(Table(self.fq_event, EVENT_SCHEMA))
        self.load_test_data([
            LOAD_DATA.render(project=self.project_id,
                             input=self.input_dataset_id,
                             ids_dataset=self.capture_dataset_id,
                             ids_view=IDS_VIEW,
                             rdr_sandbox=self.capture_dataset_id,
                             aian_list=AIAN_LIST,
                             under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                             capture=self.capture_dataset_id,
                             capture_table=CT_PLUS_BIRTHDATE_TABLE,
                             output=self.output_dataset_id,
                             event_table=OBSERVATION)
        ])

    def _assert_capture(self):
        # The fixture keeps the capture, both lookups and the ids stand-in in one
        # dataset.
        return dob.assert_capture_is_publishable(
            self.client, self.project_id, self.input_dataset_id,
            self.capture_dataset_id, self.capture_dataset_id,
            self.capture_dataset_id, self.capture_dataset_id, IDS_VIEW)

    def test_clean_capture_is_added_beside_the_event_tables(self):
        dob.assert_output_dataset_is_ready(self.client, self.project_id,
                                           self.output_dataset_id)
        self.assertEqual(self._assert_capture(), 2)

        dob.write_delivered_table(self.client, self.project_id,
                                  self.capture_dataset_id,
                                  self.output_dataset_id)

        self.assertTableValuesMatch(self.fq_output, dob.DELIVERED_COLUMNS,
                                    [(1, datetime(1985, 4, 12), 4, 12),
                                     (2, datetime(1990, 11, 3), 11, 3)])
        self.assertTableValuesMatch(self.fq_event,
                                    ['observation_id', 'person_id'], [(101, 1),
                                                                      (102, 2)])
        self.assertEqual(
            {
                table.table_id for table in self.client.list_tables(
                    f'{self.project_id}.{self.output_dataset_id}')
            }, {OBSERVATION, dob.DELIVERED_TABLE})

        with self.assertRaises(RuntimeError) as ctx:
            dob.assert_output_dataset_is_ready(self.client, self.project_id,
                                               self.output_dataset_id)
        self.assertIn('already exists', str(ctx.exception))

    def test_dataset_without_event_tables_is_refused(self):
        self.client.delete_table(self.fq_event)

        with self.assertRaises(RuntimeError) as ctx:
            dob.assert_output_dataset_is_ready(self.client, self.project_id,
                                               self.output_dataset_id)
        self.assertIn('holds no tables', str(ctx.exception))

    def test_each_unpublishable_row_is_refused(self):
        for check, (person_id, birth_datetime) in BAD_ROWS.items():
            with self.subTest(check=check):
                self.load_test_data([
                    ADD_ROW.render(project=self.project_id,
                                   capture=self.capture_dataset_id,
                                   capture_table=CT_PLUS_BIRTHDATE_TABLE,
                                   person_id=person_id,
                                   birth_datetime=birth_datetime)
                ])

                row = list(
                    self.client.query(
                        dob.CAPTURE_CHECKS.render(
                            project=self.project_id,
                            input_dataset=self.input_dataset_id,
                            capture_dataset=self.capture_dataset_id,
                            capture_table=CT_PLUS_BIRTHDATE_TABLE,
                            rdr_sandbox=self.capture_dataset_id,
                            aian_list=AIAN_LIST,
                            under18_dataset=self.capture_dataset_id,
                            under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                            ids_dataset=self.capture_dataset_id,
                            ids_view=IDS_VIEW,
                            person=PERSON)).result())[0]
                self.assertEqual(row[check], 1)
                with self.assertRaises(RuntimeError):
                    self._assert_capture()

                self.client.query(
                    f'DELETE FROM `{self.fq_capture}` WHERE month_of_birth = 13'
                ).result()
