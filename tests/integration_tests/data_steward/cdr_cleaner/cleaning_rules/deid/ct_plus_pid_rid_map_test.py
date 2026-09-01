"""
Integration test for ct_plus_pid_rid_map.py

Original Issue: DL-2477
"""
# Python Imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.ct_plus_pid_rid_map import CtPlusPIDtoRID
from common import (CONDITION_OCCURRENCE, DEID_MAP, PERSON,
                    RDR_PARTICIPANT_RESEARCH_IDS_VIEW)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

IDS_VIEW_SCHEMA = [
    {
        "type": "integer",
        "name": "participant_id",
        "mode": "nullable"
    },
    {
        "type": "integer",
        "name": "controlled_tier_id",
        "mode": "nullable"
    },
    {
        "type": "integer",
        "name": "controlled_tier_plus_id",
        "mode": "nullable"
    },
    {
        "type": "integer",
        "name": "registered_tier_date_shift",
        "mode": "nullable"
    },
]

# 1001 is a mainline participant, covered by both sources.
# 1002 is a pediatric participant: the research IDs view covers it and
# primary_pid_rid_mapping does not, which is the case this rule exists for.
# 1003 has a view row with no controlled_tier_id, so it has no RID to receive.
# 1004 appears in neither source.
MAINLINE_PID, MAINLINE_RID = 1001, 2001
PEDIATRIC_PID, PEDIATRIC_RID = 1002, 2002
NO_CT_ID_PID = 1003
UNCOVERED_PID = 1004

# What a stale map left behind by an earlier run holds: the mainline participant
# under a different RID, plus a participant that is not in this dataset at all.
STALE_RID = 9001
FOREIGN_PID = 9999


class CtPlusPIDtoRIDTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)

        # using unioned since we don't declare a deid dataset
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.fq_deid_map_table = f'{cls.project_id}.{cls.sandbox_id}.{DEID_MAP}'
        cls.fq_ids_view_table = (f'{cls.project_id}.{cls.sandbox_id}.'
                                 f'{RDR_PARTICIPANT_RESEARCH_IDS_VIEW}')

        # The fixture stands in for pipeline_tables.rdr_participant_research_ids_view.
        cls.rule_instance = CtPlusPIDtoRID(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
            ids_dataset_id=cls.sandbox_id,
            ids_view_id=RDR_PARTICIPANT_RESEARCH_IDS_VIEW)

        table_ids = [CONDITION_OCCURRENCE, PERSON]
        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table_id}'
            for table_id in table_ids
        ] + [cls.fq_deid_map_table]
        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.'
            f'{cls.rule_instance.sandbox_table_for(table_id)}'
            for table_id in table_ids
        ]

        super().setUpClass()

    def setUp(self):
        """Create the tables the rule reads, including the ids view fixture."""
        super().setUp()

        self.client.create_table(Table(self.fq_ids_view_table, IDS_VIEW_SCHEMA))
        self.fq_table_names.append(self.fq_ids_view_table)

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

    def load_fixtures(self):
        """Load the ids view, the stale map, and the CDM tables."""
        ids_view_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_ids_view_table}}`
        (participant_id, controlled_tier_id, controlled_tier_plus_id,
         registered_tier_date_shift)
        VALUES
            ({{mainline_pid}}, {{mainline_rid}}, 3001, 100),
            -- byte identical duplicate, collapsed by the rule's SELECT DISTINCT --
            ({{mainline_pid}}, {{mainline_rid}}, 3001, 100),
            ({{pediatric_pid}}, {{pediatric_rid}}, 3002, 101),
            -- no controlled_tier_id, so no RID to substitute --
            ({{no_ct_id_pid}}, NULL, 3003, 102)
        """).render(fq_ids_view_table=self.fq_ids_view_table,
                    mainline_pid=MAINLINE_PID,
                    mainline_rid=MAINLINE_RID,
                    pediatric_pid=PEDIATRIC_PID,
                    pediatric_rid=PEDIATRIC_RID,
                    no_ct_id_pid=NO_CT_ID_PID)

        # Stands in for a map copied from primary_pid_rid_mapping by an earlier run.
        # It omits the pediatric participant and holds a different RID for the
        # mainline one, so reuse of it is visible in the output.
        stale_map_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_deid_map_table}}`
        (person_id, research_id, shift)
        VALUES
            ({{mainline_pid}}, {{stale_rid}}, 100),
            ({{foreign_pid}}, {{foreign_pid}}, 0)
        """).render(fq_deid_map_table=self.fq_deid_map_table,
                    mainline_pid=MAINLINE_PID,
                    stale_rid=STALE_RID,
                    foreign_pid=FOREIGN_PID)

        person_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.person`
        (person_id, gender_concept_id, year_of_birth, race_concept_id,
         ethnicity_concept_id)
        VALUES
            ({{mainline_pid}}, 0, 1960, 0, 0),
            ({{pediatric_pid}}, 0, 2022, 0, 0),
            ({{no_ct_id_pid}}, 0, 1975, 0, 0),
            ({{uncovered_pid}}, 0, 1980, 0, 0)
        """).render(fq_dataset_name=self.fq_dataset_name,
                    mainline_pid=MAINLINE_PID,
                    pediatric_pid=PEDIATRIC_PID,
                    no_ct_id_pid=NO_CT_ID_PID,
                    uncovered_pid=UNCOVERED_PID)

        co_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.condition_occurrence`
        (condition_occurrence_id, person_id, condition_concept_id,
         condition_start_date, condition_start_datetime,
         condition_type_concept_id)
        VALUES
            (50001, {{mainline_pid}}, 100, date('2020-08-17'),
             '2020-08-17 15:00:00', 10),
            (50002, {{pediatric_pid}}, 200, date('2020-08-17'),
             '2020-08-17 14:00:00', 11),
            (50003, {{no_ct_id_pid}}, 300, date('2020-08-17'),
             '2020-08-17 13:00:00', 12),
            (50004, {{uncovered_pid}}, 400, date('2020-08-17'),
             '2020-08-17 12:00:00', 13)
        """).render(fq_dataset_name=self.fq_dataset_name,
                    mainline_pid=MAINLINE_PID,
                    pediatric_pid=PEDIATRIC_PID,
                    no_ct_id_pid=NO_CT_ID_PID,
                    uncovered_pid=UNCOVERED_PID)

        self.load_test_data(
            [ids_view_query, stale_map_query, person_query, co_query])

    def read_deid_map(self):
        """Return the rebuilt map as a sorted list of (person_id, research_id, shift)."""
        query = f'SELECT person_id, research_id, shift FROM `{self.fq_deid_map_table}`'
        return sorted((row['person_id'], row['research_id'], row['shift'])
                      for row in self.client.query(query).result())

    def test_deid_map_is_rebuilt_from_the_ids_view(self):
        """setup_rule replaces a stale map rather than reusing it.

        The inherited setup_rule copies primary_pid_rid_mapping only when _deid_map
        is absent, so a map left in the sandbox by another tier's run would be
        reused silently. This rule rebuilds unconditionally.
        """
        self.load_fixtures()

        self.rule_instance.setup_rule(self.client)

        self.assertEqual(self.read_deid_map(), [
            (MAINLINE_PID, MAINLINE_RID, 100),
            (PEDIATRIC_PID, PEDIATRIC_RID, 101),
        ])

    def test_rebuild_is_idempotent(self):
        """Running setup_rule twice over the same view leaves the same map."""
        self.load_fixtures()

        self.rule_instance.setup_rule(self.client)
        first = self.read_deid_map()
        self.rule_instance.setup_rule(self.client)

        self.assertEqual(self.read_deid_map(), first)

    def test_field_cleaning(self):
        """The pediatric participant survives and every survivor carries its CT ID.

        1001 proves the stale map was not reused: it comes out as its
        controlled_tier_id, not as the RID that map held for it.
        1002 is the pediatric participant, absent from the stale map and kept here.
        1003 and 1004 have no controlled_tier_id in the view and are deleted, which
        is the inherited behaviour for a participant the map does not cover.
        """
        self.load_fixtures()

        self.rule_instance.setup_rule(self.client)

        person_sb_table, co_sb_table = '', ''
        for table in self.fq_sandbox_table_names:
            if PERSON in table:
                person_sb_table = table
            elif CONDITION_OCCURRENCE in table:
                co_sb_table = table

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, CONDITION_OCCURRENCE]),
            'fq_sandbox_table_name':
                co_sb_table,
            'fields': ['condition_occurrence_id', 'person_id'],
            'loaded_ids': [50001, 50002, 50003, 50004],
            'sandboxed_ids': [NO_CT_ID_PID, UNCOVERED_PID],
            'sandbox_fields': ['person_id'],
            'cleaned_values': [(50001, MAINLINE_RID), (50002, PEDIATRIC_RID)]
        }, {
            'fq_table_name': '.'.join([self.fq_dataset_name, PERSON]),
            'fq_sandbox_table_name': person_sb_table,
            'fields': ['person_id', 'year_of_birth'],
            'loaded_ids': [
                MAINLINE_PID, PEDIATRIC_PID, NO_CT_ID_PID, UNCOVERED_PID
            ],
            'sandboxed_ids': [NO_CT_ID_PID, UNCOVERED_PID],
            'sandbox_fields': ['person_id'],
            'cleaned_values': [(MAINLINE_RID, 1960), (PEDIATRIC_RID, 2022)]
        }]

        self.default_test(tables_and_counts)
