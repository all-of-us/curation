"""
Integration test for capture_ct_plus_zip5 module

Original Issues: DL2437

Runs the three rules in their pipeline order against one fixture, with
person_id already holding CT research ids, as it does after RtCtPIDtoRID.
"""

# Python Imports
import os
from unittest import mock

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from app_identity import PROJECT_ID
from common import (AIAN_LIST, CT_PLUS_ZIP5, DEID_MAP, JINJA_ENV, OBSERVATION,
                    PERSON, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.capture_ct_plus_zip5 import (
    CaptureCtPlusZip5, ConvertCtPlusZip5Ids, PruneCtPlusZip5)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

CT_PLUS_IDS_VIEW = 'ct_plus_zip5_test_ids'

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
#   1: two zips on different days, the later one wins, cut to five digits
#   2: two zips on the same datetime, the higher observation_id wins
#   3: AIAN, excluded
#   4: pediatric, excluded
#   5: only an already generalized zip, and five digits under another concept
#   6: eligible, but absent from person, so pruned
#   7: eligible and kept
OBSERVATION_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
(observation_id, person_id, observation_concept_id, observation_date,
 observation_datetime, observation_type_concept_id, observation_source_concept_id,
 value_as_string)
VALUES
  (11, 1, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '35401'),
  (12, 1, 0, '2021-01-01', '2021-01-01 00:00:00', 0, 1585250, '35402-1234'),
  (21, 2, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '11111'),
  (22, 2, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '22222'),
  (31, 3, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '33333'),
  (41, 4, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '44444'),
  (51, 5, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '354**'),
  (52, 5, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 0, '55555'),
  (61, 6, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '66666'),
  (71, 7, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 1585250, '77777')
""")

PERSON_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES (1, 0, 1980, 0, 0), (2, 0, 1980, 0, 0), (3, 0, 1980, 0, 0),
       (4, 0, 2022, 0, 0), (5, 0, 1980, 0, 0), (7, 0, 1980, 0, 0)
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
VALUES (101, 1), (102, 2), (103, 3), (104, 4), (105, 5), (106, 6), (107, 7)
""")

# Participant 1 appears twice, identically, as some participants do in the real
# view. The conversion must still update their row once.
IDS_VIEW_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{fq_table}}`
(participant_id, controlled_tier_id, controlled_tier_plus_id)
VALUES
  (101, 1, 300000000001), (101, 1, 300000000001), (102, 2, 300000000002),
  (103, 3, 300000000003), (104, 4, 300000000004), (105, 5, 300000000005),
  (106, 6, 300000000006), (107, 7, 300000000007)
""")


@mock.patch('cdr_cleaner.cleaning_rules.capture_ct_plus_zip5.PIPELINE_TABLES',
            os.environ.get('RDR_DATASET_ID'))
class CaptureCtPlusZip5Test(BaseTest.CleaningRulesTestBase):

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

        for table_name in [OBSERVATION, PERSON]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
        cls.fq_zip5_table = f'{cls.project_id}.{cls.sandbox_id}.{CT_PLUS_ZIP5}'
        cls.fq_sandbox_table_names.append(cls.fq_zip5_table)

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
            OBSERVATION_DATA.render(project_id=self.project_id,
                                    dataset_id=self.dataset_id),
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

    def test_capture_prune_and_convert(self):
        """
        The capture keeps the latest eligible zip per participant and skips
        AIAN and pediatric participants, the prune drops participant 6, and the
        conversion re-keys the rest to CT+ ids.
        """
        fields = [
            'person_id', 'observation_source_concept_id', 'value_as_string'
        ]

        self._run(CaptureCtPlusZip5)
        # Asserted before the prune so the later steps cannot pass on an
        # empty table.
        self.assertTableValuesMatch(self.fq_zip5_table,
                                    fields, [(1, 1585250, '35402'),
                                             (2, 1585250, '22222'),
                                             (6, 1585250, '66666'),
                                             (7, 1585250, '77777')])

        self._run(PruneCtPlusZip5)
        self.assertTableValuesMatch(self.fq_zip5_table,
                                    fields, [(1, 1585250, '35402'),
                                             (2, 1585250, '22222'),
                                             (7, 1585250, '77777')])

        self._run(ConvertCtPlusZip5Ids)
        self.assertTableValuesMatch(self.fq_zip5_table, fields,
                                    [(300000000001, 1585250, '35402'),
                                     (300000000002, 1585250, '22222'),
                                     (300000000007, 1585250, '77777')])

    def test_unresolved_participant_stops_the_conversion(self):
        """
        A captured participant with no CT+ id stops the run, and the table
        keeps its CT ids rather than shipping a partial conversion.
        """
        fq_ids_view = (f'{self.project_id}.{self.rdr_dataset_id}.'
                       f'{CT_PLUS_IDS_VIEW}')
        self.client.query(f'DELETE FROM `{fq_ids_view}` '
                          f'WHERE controlled_tier_id = 7').result()

        self._run(CaptureCtPlusZip5)
        self._run(PruneCtPlusZip5)

        with self.assertRaises(RuntimeError):
            self._run(ConvertCtPlusZip5Ids)

        self.assertRowIDsMatch(self.fq_zip5_table, ['person_id'], [1, 2, 7])
