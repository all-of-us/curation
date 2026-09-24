"""
Integration test for the capture_ct_plus_pediatric_cohort module.

Original Issues: DL2537

The capture records the participants the CT+ run retains in the '0-6' band, and the
conversion resolves them to controlled_tier_plus_id through _deid_map and the research
IDs view, failing the run on any participant that does not resolve.
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from app_identity import PROJECT_ID
from common import (CT_PLUS_PEDIATRIC_COHORT, DEID_MAP, JINJA_ENV, PERSON,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from cdr_cleaner.cleaning_rules.capture_ct_plus_pediatric_cohort import (
    CaptureCtPlusPediatricCohort, ConvertCtPlusPediatricCohortIds)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

# Stands in for pipeline_tables.rdr_participant_research_ids_view
IDS_VIEW = 'fake_rdr_participant_research_ids_view'

UNDER18_LOOKUP_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "age_at_consent",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "age_band",
    "mode": "nullable"
}]

DEID_MAP_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "research_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "shift",
    "mode": "nullable"
}]

IDS_VIEW_SCHEMA = [{
    "type": "integer",
    "name": "controlled_tier_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "controlled_tier_plus_id",
    "mode": "nullable"
}]

LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{lookup_dataset}}.{{lookup_table}}`
(person_id, age_at_consent, age_band)
VALUES
  /* 1 and 4 are retained by the CT+ run, so they are captured */
  (1, 2, '0-6'),
  (4, 6, '0-6'),
  /* 2 is removed by the CT+ run, so it is not captured */
  (2, 12, '7-17'),
  /* 3 is flagged but has no person row, so there is nothing to split */
  (3, 1, '0-6');

INSERT INTO `{{project}}.{{dataset}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
  (1, 0, 2020, 0, 0),
  (2, 0, 2012, 0, 0),
  (4, 0, 2018, 0, 0),
  (5, 0, 1980, 0, 0);

INSERT INTO `{{project}}.{{sandbox}}.{{deid_map}}`
(person_id, research_id, shift)
VALUES
  (1, 1001, 0),
  (2, 1002, 0),
  (4, 1004, 0),
  (5, 1005, 0);

INSERT INTO `{{project}}.{{sandbox}}.{{ids_view}}`
(controlled_tier_id, controlled_tier_plus_id)
VALUES
  (1001, 3001),
  (1002, 3002),
  (1004, 3004),
  /* duplicate rows observed in the real view must not duplicate the cohort */
  (1004, 3004),
  (1005, 3005)
""")


class CaptureCtPlusPediatricCohortTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.lookup_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.fq_table_names = [f'{cls.project_id}.{cls.dataset_id}.{PERSON}']
        # Helper tables have no schema file, so they are created in setUp rather
        # than through fq_table_names. Listed here so tearDown drops them.
        cls.fq_cohort = (f'{cls.project_id}.{cls.sandbox_id}.'
                         f'{CT_PLUS_PEDIATRIC_COHORT}')
        cls.fq_helper_tables = {
            f'{cls.project_id}.{cls.lookup_dataset_id}.'
            f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}':
                UNDER18_LOOKUP_SCHEMA,
            f'{cls.project_id}.{cls.sandbox_id}.{DEID_MAP}':
                DEID_MAP_SCHEMA,
            f'{cls.project_id}.{cls.sandbox_id}.{IDS_VIEW}':
                IDS_VIEW_SCHEMA,
        }
        cls.fq_sandbox_table_names = [cls.fq_cohort, *cls.fq_helper_tables]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        for fq_table, schema in self.fq_helper_tables.items():
            self.client.create_table(Table(fq_table, schema))

        self.load_test_data([
            LOAD_DATA.render(project=self.project_id,
                             dataset=self.dataset_id,
                             sandbox=self.sandbox_id,
                             lookup_dataset=self.lookup_dataset_id,
                             lookup_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                             deid_map=DEID_MAP,
                             ids_view=IDS_VIEW)
        ])

    def _run(self, rules):
        clean_engine.clean_dataset(
            self.project_id,
            self.dataset_id,
            self.sandbox_id,
            rules,
            under18_lookup_dataset_id=self.lookup_dataset_id,
            ids_dataset_id=self.sandbox_id,
            ids_view_id=IDS_VIEW)

    def test_capture_records_the_retained_band(self):
        """The 7 to 17 participant and the one with no person row are not
        captured."""
        self._run([(CaptureCtPlusPediatricCohort,)])

        self.assertTableValuesMatch(self.fq_cohort,
                                    ['participant_id', 'age_band'],
                                    [(1, '0-6'), (4, '0-6')])

    def test_conversion_resolves_to_ct_plus_research_ids(self):
        """Pre-deid participant to controlled tier research ID to CT+ research
        ID, one row per participant despite the duplicate view row."""
        self._run([(CaptureCtPlusPediatricCohort,),
                   (ConvertCtPlusPediatricCohortIds,)])

        self.assertTableValuesMatch(
            self.fq_cohort,
            ['participant_id', 'controlled_tier_id', 'person_id'],
            [(1, 1001, 3001), (4, 1004, 3004)])

    def test_conversion_fails_on_an_unresolvable_participant(self):
        """Participant 4 has no _deid_map row, so it has no CT+ research ID and
        the table must not be rewritten with a controlled tier value."""
        self._run([(CaptureCtPlusPediatricCohort,)])
        self.load_test_data([
            f'DELETE FROM `{self.project_id}.{self.sandbox_id}.{DEID_MAP}` '
            f'WHERE person_id = 4'
        ])
        rule = ConvertCtPlusPediatricCohortIds(self.project_id,
                                               self.dataset_id,
                                               self.sandbox_id,
                                               ids_dataset_id=self.sandbox_id,
                                               ids_view_id=IDS_VIEW)

        with self.assertRaises(RuntimeError):
            rule.setup_rule(self.client)
