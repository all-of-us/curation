"""
Integration test for capture_ct_plus_birthdate module

Original Issues: DL2436

Runs the capture, NullPersonBirthdate, the prune and the conversion in their
pipeline order against one fixture, with person_id already holding CT research
ids, as it does after RtCtPIDtoRID.
"""

# Python Imports
import os
from unittest import mock

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from app_identity import PROJECT_ID
from common import (AIAN_LIST, CT_PLUS_BIRTHDATE, DEID_MAP, JINJA_ENV, PERSON,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.capture_ct_plus_birthdate import (
    CaptureCtPlusBirthdate, ConvertCtPlusBirthdateIds, PruneCtPlusBirthdate)
from cdr_cleaner.cleaning_rules.null_person_birthdate import NullPersonBirthdate
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

CT_PLUS_IDS_VIEW = 'ct_plus_birthdate_test_ids'

PERSON_ID_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}]

UNDER18_LOOKUP_SCHEMA = PERSON_ID_SCHEMA + [{
    "type": "integer",
    "name": "age_at_consent",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "age_band",
    "mode": "nullable"
}]

DEID_MAP_SCHEMA = PERSON_ID_SCHEMA + [{
    "type": "integer",
    "name": "research_id",
    "mode": "nullable"
}]

IDS_VIEW_SCHEMA = [
    {
        "type": "integer",
        "name": column,
        "mode": "nullable"
    } for column in
    ['participant_id', 'controlled_tier_id', 'controlled_tier_plus_id']
]

# Participant ids are 10x, CT research ids x, CT+ research ids 300000000000 + x.
#   1: kept
#   3: AIAN, excluded
#   4: pediatric, excluded
#   5: no birth_datetime, so no row
#   6: captured, then removed from person, so pruned
#   7: kept
PERSON_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
 birth_datetime, race_concept_id, ethnicity_concept_id)
VALUES
  (1, 0, 1980, 5, 17, '1980-05-17 00:00:00', 0, 0),
  (3, 0, 1975, 2, 3, '1975-02-03 00:00:00', 0, 0),
  (4, 0, 2022, 8, 9, '2022-08-09 00:00:00', 0, 0),
  (5, 0, 1990, NULL, NULL, NULL, 0, 0),
  (6, 0, 1985, 11, 30, '1985-11-30 00:00:00', 0, 0),
  (7, 0, 1970, 1, 2, '1970-01-02 00:00:00', 0, 0)
""")

AIAN_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{fq_table}}` (person_id) VALUES (103)
""")

UNDER18_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{fq_table}}` (person_id, age_at_consent, age_band)
VALUES (104, 2, '0-6')
""")

DEID_MAP_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{fq_table}}` (person_id, research_id)
VALUES (101, 1), (103, 3), (104, 4), (105, 5), (106, 6), (107, 7)
""")

IDS_VIEW_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{fq_table}}`
(participant_id, controlled_tier_id, controlled_tier_plus_id)
VALUES
  (101, 1, 300000000001), (103, 3, 300000000003), (104, 4, 300000000004),
  (105, 5, 300000000005), (106, 6, 300000000006), (107, 7, 300000000007)
""")


@mock.patch('cdr_cleaner.cleaning_rules.ct_plus_side_tables.PIPELINE_TABLES',
            os.environ.get('RDR_DATASET_ID'))
class CaptureCtPlusBirthdateTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        # The lookups, and the ids view standing in for pipeline_tables, share
        # the RDR dataset.
        cls.rdr_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {
            'rdr_sandbox_id': cls.rdr_dataset_id,
            'under18_lookup_dataset_id': cls.rdr_dataset_id,
            'ct_plus_ids_view': CT_PLUS_IDS_VIEW
        }

        cls.fq_person_table = f'{cls.project_id}.{cls.dataset_id}.{PERSON}'
        cls.fq_table_names.append(cls.fq_person_table)
        cls.fq_birthdate_table = (f'{cls.project_id}.{cls.sandbox_id}.'
                                  f'{CT_PLUS_BIRTHDATE}')
        cls.fq_sandbox_table_names.append(cls.fq_birthdate_table)

        # Not via fq_table_names, whose tables are created from schema files by
        # name. setUp creates these and tearDown drops them.
        cls.extra_tables = {
            f'{cls.project_id}.{cls.rdr_dataset_id}.{AIAN_LIST}':
                (PERSON_ID_SCHEMA, AIAN_DATA),
            f'{cls.project_id}.{cls.rdr_dataset_id}.{UNDER18_PARTICIPANTS_LOOKUP_TABLE}':
                (UNDER18_LOOKUP_SCHEMA, UNDER18_DATA),
            f'{cls.project_id}.{cls.sandbox_id}.{DEID_MAP}':
                (DEID_MAP_SCHEMA, DEID_MAP_DATA),
            f'{cls.project_id}.{cls.rdr_dataset_id}.{CT_PLUS_IDS_VIEW}':
                (IDS_VIEW_SCHEMA, IDS_VIEW_DATA),
        }

        super().setUpClass()

    def setUp(self):
        super().setUp()

        queries = [
            PERSON_DATA.render(project_id=self.project_id,
                               dataset_id=self.dataset_id)
        ]
        for fq_table, (schema, data) in self.extra_tables.items():
            self.client.create_table(Table(fq_table, schema))
            queries.append(data.render(fq_table=fq_table))

        self.load_test_data(queries)

    def tearDown(self):
        for fq_table in self.extra_tables:
            self.client.delete_table(fq_table, not_found_ok=True)

        super().tearDown()

    def _run(self, rule_class):
        """
        Run a rule the way the engine does: setup_rule, then each query.
        """
        rule = rule_class(
            self.project_id, self.dataset_id, self.sandbox_id,
            **clean_engine.get_custom_kwargs(rule_class, **self.kwargs))
        rule.setup_rule(self.client)
        for spec in rule.get_query_specs():
            self.client.query(spec[cdr_consts.QUERY]).result()

    def test_capture_survives_nulling_then_prune_and_convert(self):
        """
        The capture keeps full dates of birth for eligible participants,
        NullPersonBirthdate still nulls person, the prune drops participant 6
        once person loses them, and the conversion re-keys the rest.
        """
        fields = [
            'person_id', 'birth_datetime', 'month_of_birth', 'day_of_birth'
        ]

        self._run(CaptureCtPlusBirthdate)
        self._run(NullPersonBirthdate)

        # Asserted before the prune so the later steps cannot pass on an
        # empty table.
        captured = [(1, '1980-05-17 00:00:00', 5, 17),
                    (6, '1985-11-30 00:00:00', 11, 30),
                    (7, '1970-01-02 00:00:00', 1, 2)]
        self.assertTableValuesMatch(self.fq_birthdate_table, [
            'person_id', "FORMAT_TIMESTAMP('%F %T', birth_datetime)",
            'month_of_birth', 'day_of_birth'
        ], captured)
        self.assertTableValuesMatch(
            self.fq_person_table, fields,
            [(p, None, None, None) for p in [1, 3, 4, 5, 6, 7]])

        # Stands in for DropOrphanedPIDS and the other removal rules.
        self.client.query(f'DELETE FROM `{self.fq_person_table}` '
                          f'WHERE person_id = 6').result()

        self._run(PruneCtPlusBirthdate)
        self._run(ConvertCtPlusBirthdateIds)

        self.assertTableValuesMatch(self.fq_birthdate_table, [
            'person_id', "FORMAT_TIMESTAMP('%F %T', birth_datetime)",
            'month_of_birth', 'day_of_birth'
        ], [(300000000001, '1980-05-17 00:00:00', 5, 17),
            (300000000007, '1970-01-02 00:00:00', 1, 2)])
