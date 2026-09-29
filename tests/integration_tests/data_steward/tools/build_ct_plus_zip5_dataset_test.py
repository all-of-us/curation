"""
Integration test for build_ct_plus_zip5_dataset.

Original Issues: DL2536

Participants 1 and 2 are in the input. The clean fixture captures both; each refusal
test adds one row the tool must not publish: a participant absent from the input, a
repeated participant, a row still on its CT research ID, an AIAN participant, and a
generalized three-digit value.
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import AIAN_LIST, CT_PLUS_ZIP5_TABLE, JINJA_ENV, PERSON
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from tools import build_ct_plus_zip5_dataset as zip5

IDS_VIEW = 'research_ids'

CAPTURE_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "observation_source_concept_id",
    "mode": "nullable"
}, {
    "type": "datetime",
    "name": "observation_datetime",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "value_as_string",
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

AIAN_SCHEMA = [{"type": "integer", "name": "person_id", "mode": "nullable"}]

# Participant IDs 900x, CT IDs 800x, CT+ IDs x. Participant 9003 is AIAN, is in the
# input, and would only reach the capture if the capture's exclusion regressed.
LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{input}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES (1, 0, 1985, 0, 0), (2, 0, 1990, 0, 0), (3, 0, 1970, 0, 0);

INSERT INTO `{{project}}.{{ids_dataset}}.{{ids_view}}`
(participant_id, controlled_tier_id, controlled_tier_plus_id)
VALUES (9001, 8001, 1), (9002, 8002, 2), (9003, 8003, 3), (9004, 8004, 4);

INSERT INTO `{{project}}.{{rdr_sandbox}}.{{aian_list}}` (person_id)
VALUES (9003);

INSERT INTO `{{project}}.{{capture}}.{{capture_table}}`
(person_id, observation_source_concept_id, observation_datetime, value_as_string)
VALUES
  (1, 1585250, '2024-01-01 00:00:00', '35401'),
  (2, 1585250, '2024-02-01 00:00:00', '10001')
""")

BAD_ROWS = {
    'unresolved_rows': (4, '60601'),
    'duplicate_rows': (1, '35402'),
    'non_ct_plus_rows': (8002, '10002'),
    'aian_rows': (3, '87501'),
    'not_five_digit_rows': (2, '354**'),
}

ADD_ROW = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{capture}}.{{capture_table}}`
(person_id, observation_source_concept_id, observation_datetime, value_as_string)
VALUES ({{person_id}}, 1585250, '2024-03-01 00:00:00', '{{value}}')
""")


class BuildCtPlusZip5DatasetTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        # Dedicated datasets, so the resolve check reads only this fixture's person
        # rows and the output holds only what the tool writes.
        base = os.environ.get('COMBINED_DATASET_ID')
        cls.input_dataset_id = f'{base}_zip5_input'
        cls.capture_dataset_id = f'{base}_zip5_sandbox'
        cls.output_dataset_id = f'{base}_zip5_output'

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.input_dataset_id}.{PERSON}'
        ]
        cls.fq_capture = (f'{cls.project_id}.{cls.capture_dataset_id}.'
                          f'{CT_PLUS_ZIP5_TABLE}')
        cls.fq_ids = f'{cls.project_id}.{cls.capture_dataset_id}.{IDS_VIEW}'
        cls.fq_aian = f'{cls.project_id}.{cls.capture_dataset_id}.{AIAN_LIST}'
        cls.fq_output = (f'{cls.project_id}.{cls.output_dataset_id}.'
                         f'{zip5.DELIVERED_TABLE}')
        cls.fq_sandbox_table_names = [
            cls.fq_capture, cls.fq_ids, cls.fq_aian, cls.fq_output
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        self.client.create_table(Table(self.fq_capture, CAPTURE_SCHEMA))
        self.client.create_table(Table(self.fq_ids, IDS_SCHEMA))
        self.client.create_table(Table(self.fq_aian, AIAN_SCHEMA))
        self.load_test_data([
            LOAD_DATA.render(project=self.project_id,
                             input=self.input_dataset_id,
                             ids_dataset=self.capture_dataset_id,
                             ids_view=IDS_VIEW,
                             rdr_sandbox=self.capture_dataset_id,
                             aian_list=AIAN_LIST,
                             capture=self.capture_dataset_id,
                             capture_table=CT_PLUS_ZIP5_TABLE)
        ])

    def _assert_capture(self):
        return zip5.assert_capture_is_publishable(self.client, self.project_id,
                                                  self.input_dataset_id,
                                                  self.capture_dataset_id,
                                                  self.capture_dataset_id,
                                                  self.capture_dataset_id,
                                                  IDS_VIEW)

    def test_clean_capture_is_promoted(self):
        self.assertEqual(self._assert_capture(), 2)

        zip5.write_delivered_table(self.client, self.project_id,
                                   self.capture_dataset_id,
                                   self.output_dataset_id)

        self.assertTableValuesMatch(self.fq_output,
                                    ['person_id', 'value_as_string'],
                                    [(1, '35401'), (2, '10001')])

    def test_each_unpublishable_row_is_refused(self):
        for check, (person_id, value) in BAD_ROWS.items():
            with self.subTest(check=check):
                self.load_test_data([
                    ADD_ROW.render(project=self.project_id,
                                   capture=self.capture_dataset_id,
                                   capture_table=CT_PLUS_ZIP5_TABLE,
                                   person_id=person_id,
                                   value=value)
                ])

                row = list(
                    self.client.query(
                        zip5.CAPTURE_CHECKS.render(
                            project=self.project_id,
                            input_dataset=self.input_dataset_id,
                            capture_dataset=self.capture_dataset_id,
                            capture_table=CT_PLUS_ZIP5_TABLE,
                            rdr_sandbox=self.capture_dataset_id,
                            aian_list=AIAN_LIST,
                            ids_dataset=self.capture_dataset_id,
                            ids_view=IDS_VIEW,
                            person=PERSON)).result())[0]
                self.assertEqual(row[check], 1)
                with self.assertRaises(RuntimeError):
                    self._assert_capture()

                self.client.query(
                    f'DELETE FROM `{self.fq_capture}` '
                    f"WHERE observation_datetime = '2024-03-01 00:00:00'"
                ).result()
