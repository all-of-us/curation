"""
Integration test for SandboxAndRemovePidsList module.
"""
# Python imports
import os
from datetime import datetime

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from common import (AOU_DEATH, FACT_RELATIONSHIP, JINJA_ENV, OBSERVATION,
                    PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE, PERSON,
                    RDR_DATASET_ID, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from cdr_cleaner.cleaning_rules.sandbox_and_remove_withdrawn_pids import SandboxAndRemoveWithdrawnPids
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation` 
        (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
    VALUES
        (10101, 101, 0, date('2022-01-01'), 0),
        (10102, 101, 0, date('2022-01-01'), 0),
        (10201, 102, 0, date('2022-01-02'), 0),
        (10202, 102, 0, date('2022-01-02'), 0),
        (10301, 103, 0, date('2022-01-03'), 0),
        (10302, 103, 0, date('2022-01-03'), 0),
        (10401, 104, 0, date('2022-01-04'), 0),
        (10402, 104, 0, date('2022-01-04'), 0),
        (20101, 201, 0, date('2022-01-01'), 0),
        (20102, 201, 0, date('2022-01-01'), 0),
        (20201, 202, 0, date('2022-01-02'), 0),
        (20202, 202, 0, date('2022-01-02'), 0),
        (20301, 203, 0, date('2022-01-03'), 0),
        (20302, 203, 0, date('2022-01-03'), 0),
        (20401, 204, 0, date('2022-01-04'), 0),
        (20402, 204, 0, date('2022-01-04'), 0),
        (30101, 301, 0, date('2022-01-01'), 0),
        (30102, 301, 0, date('2022-01-01'), 0),
        (30201, 302, 0, date('2022-01-02'), 0),
        (30202, 302, 0, date('2022-01-02'), 0),
        (30301, 303, 0, date('2022-01-03'), 0),
        (30302, 303, 0, date('2022-01-03'), 0),
        (30401, 304, 0, date('2022-01-04'), 0),
        (30402, 304, 0, date('2022-01-04'), 0),
        (40101, 401, 0, date('2022-01-01'), 0),
        (40102, 401, 0, date('2022-01-01'), 0),
        (40201, 402, 0, date('2022-01-02'), 0),
        (40202, 402, 0, date('2022-01-02'), 0),
        (40301, 403, 0, date('2022-01-03'), 0),
        (40302, 403, 0, date('2022-01-03'), 0),
        (40401, 404, 0, date('2022-01-04'), 0),
        (40402, 404, 0, date('2022-01-04'), 0)
""")

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO 
        `{{project_id}}.{{dataset_id}}.person` 
            (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
    VALUES
        (101, 0, 1991, 0, 0),
        (102, 0, 1992, 0, 0),
        (103, 0, 1993, 0, 0),
        (104, 0, 1994, 0, 0),
        (201, 0, 1991, 0, 0),
        (202, 0, 1992, 0, 0),
        (203, 0, 1993, 0, 0),
        (204, 0, 1994, 0, 0),
        (301, 0, 1991, 0, 0),
        (302, 0, 1992, 0, 0),
        (303, 0, 1993, 0, 0),
        (304, 0, 1994, 0, 0),
        (401, 0, 1991, 0, 0),
        (402, 0, 1992, 0, 0),
        (403, 0, 1993, 0, 0),
        (404, 0, 1994, 0, 0)
""")

AOU_DEATH_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO 
        `{{project_id}}.{{dataset_id}}.aou_death`
            (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
        VALUES
            ('a10101', 101, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a10202', 102, date('2020-05-05'), 0, 0, 0, 'Participant Portal 1', False),
            ('a10301', 103, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a10402', 104, date('2020-05-05'), 0, 0, 0, 'Participant Portal 1', False),
            ('a20102', 201, date('2020-05-05'), 0, 0, 0, 'Participant Portal 2', False),
            ('a20202', 202, date('2020-05-05'), 0, 0, 0, 'Participant Portal 2', False),
            ('a20302', 203, date('2020-05-05'), 0, 0, 0, 'Participant Portal 2', False),
            ('a20401', 204, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a30101', 301, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a30202', 302, date('2020-05-05'), 0, 0, 0, 'Participant Portal 3', False),
            ('a30302', 303, date('2020-05-05'), 0, 0, 0, 'Participant Portal 3', False),
            ('a30401', 304, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a40101', 401, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a40202', 402, date('2020-05-05'), 0, 0, 0, 'Participant Portal 4', False),
            ('a40301', 403, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a40401', 404, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False)
""")

LOOKUP_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.{{lookup_table}}` 
        (person_id)
    VALUES
        (104),
        (202),
        (204),
        (301),
        (401),
        (403)
""")

UNDER18_SCHEMA = [{
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

UNDER18_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{sandbox_id}}.{{under18_table}}`
        (person_id, age_at_consent, age_band)
    VALUES
        (201, 3, '0-6'),
        (202, 5, '0-6'),
        (203, 2, '0-6'),
        (220, 4, '0-6'),
        -- 210 is under 18 but outside the CT+ pediatric cohort. --
        (210, 12, '7-17')
""")

FACT_RELATIONSHIP_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.fact_relationship`
        (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
    VALUES
        -- Adult 101 is linked to two pediatric participants. The 101 to 201 link --
        -- arrives in both directions and is one pair. --
        (56, 101, 56, 201, 4053608),
        (56, 201, 56, 101, 4326600),
        (56, 101, 56, 202, 4053608),
        -- The child can sit on either side of the row. --
        (56, 203, 56, 102, 4301632),
        -- The guardian skipped the relationship question, so the concept is 0. --
        -- It is still the consent link, and still a pair. --
        (56, 105, 56, 220, 0),
        -- 210 is in the 7-17 band, which every tier removes, so this is not a --
        -- CT+ pair and not an error either. --
        (56, 103, 56, 210, 4053608),
        -- A measurement to observation row, which this query must ignore. --
        (21, 5, 27, 6, 4326600)
""")

# Rows that do not resolve to one adult and one 0-6 participant. Each stops the
# run, so only the test expecting that loads them.
UNRESOLVED_FACT_RELATIONSHIP_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.fact_relationship`
        (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
    VALUES
        -- Neither side is in the lookup: the child's age was not derived. --
        (56, 104, 56, 999, 4053608),
        -- A 7-17 participant cannot be the adult side of a 0-6 child. --
        (56, 210, 56, 201, 4218412),
        -- Both sides are 0-6, so neither is the guardian. --
        (56, 203, 56, 220, 4218412),
        -- A self reference. --
        (56, 201, 56, 201, 4053608)
""")

LOOKUP_TABLE_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "hpo_id",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "src_id",
    "mode": "nullable"
}, {
    "type": "DATE",
    "name": "consent_for_study_enrollment_authored",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "withdrawal_status",
    "mode": "nullable"
}]


class SandboxAndRemovePidsListTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = RDR_DATASET_ID
        cls.withdrawn_dups_table = 'pdr_withdrawals_list'
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.kwargs = {'withdrawn_dups_table': cls.withdrawn_dups_table}

        # Instantiate class
        cls.rule_instance = SandboxAndRemoveWithdrawnPids(
            project_id=cls.project_id,
            dataset_id=cls.dataset_id,
            sandbox_dataset_id=cls.sandbox_id,
            withdrawn_dups_table=cls.withdrawn_dups_table)

        # Generates list of fully qualified table names
        affected_table_names = ['observation', 'person', 'aou_death']
        for table_name in affected_table_names:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # Generates list of sandbox table names
        for table_name in affected_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_name)}'
            )

        # The rule writes the pairs lookup, so it goes in the sandbox list, which
        # is cleaned up and asserted absent before the run.
        cls.fq_links_table = (f'{cls.project_id}.{cls.sandbox_id}.'
                              f'{PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE}')
        cls.fq_sandbox_table_names.append(cls.fq_links_table)

        # Source of the pairs. It has no `person_id`, so the removal leaves it.
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{FACT_RELATIONSHIP}')

        # No schema files, so created and dropped by this class.
        cls.fq_withdrawn_dups_table = (
            f'{cls.project_id}.{cls.dataset_id}.{cls.withdrawn_dups_table}')
        cls.fq_under18_table = (f'{cls.project_id}.{cls.sandbox_id}.'
                                f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create tables and test data
        """
        super().setUp()

        # Kept out of `fq_table_names`: `BaseTest.setUp` cannot resolve a table
        # without a schema file, and appending to that class attribute would leak
        # across test methods.
        self.client.create_table(Table(self.fq_withdrawn_dups_table,
                                       LOOKUP_TABLE_SCHEMA),
                                 exists_ok=True)

        # Build temp records lookup table query
        lookup_table_query = LOOKUP_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            lookup_table=self.withdrawn_dups_table)

        # Written by FlagParticipantsUnder18Years at this stage, not this rule.
        self.client.create_table(Table(self.fq_under18_table, UNDER18_SCHEMA),
                                 exists_ok=True)

        under18_query = UNDER18_TEMPLATE.render(
            project_id=self.project_id,
            sandbox_id=self.sandbox_id,
            under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE)

        fact_relationship_query = FACT_RELATIONSHIP_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Build test data queries
        observation_records_query = OBSERVATION_TABLE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        person_records_query = PERSON_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        aou_death_records_query = AOU_DEATH_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        table_test_queries = [
            observation_records_query, person_records_query,
            aou_death_records_query, under18_query, fact_relationship_query
        ]

        # Load test data
        self.load_test_data([lookup_table_query] + table_test_queries)

    def tearDown(self):
        """
        Drop the two fixture tables kept out of `fq_table_names`.
        """
        for fq_table in [self.fq_withdrawn_dups_table, self.fq_under18_table]:
            self.client.delete_table(fq_table, not_found_ok=True)
        super().tearDown()

    def test_pediatric_guardian_links_are_derived(self):
        """
        The rule writes the adult to pediatric pairs at the RDR stage, where
        `fact_relationship` and the cohort lookup are both in reach.
        """
        loaded = list(
            self.client.query(
                f'SELECT COUNT(*) AS n FROM '
                f'`{self.project_id}.{self.dataset_id}.{FACT_RELATIONSHIP}`').
            result())[0].n
        self.assertEqual(loaded, 7,
                         'the fact_relationship fixture did not load')

        # Only the pairs lookup is asserted on.
        self.default_test([])

        pairs = sorted((row.adult_person_id, row.pediatric_person_id)
                       for row in self.client.query(f"""
                SELECT adult_person_id, pediatric_person_id
                FROM `{self.fq_links_table}`
            """).result())

        # 101 to 201 collapses to one pair across both directions, 203 is found
        # on side 1, and 105 to 220 pairs despite concept 0. 103 to 210 is
        # outside the CT+ cohort and 21 to 27 is not person to person.
        self.assertEqual(pairs, [(101, 201), (101, 202), (102, 203),
                                 (105, 220)])

    def test_unresolved_linkage_rows_stop_the_run(self):
        """
        An unresolved linkage row would keep a child's data after their guardian
        withdraws, so the rule raises.
        """
        self.load_test_data([
            UNRESOLVED_FACT_RELATIONSHIP_TEMPLATE.render(
                project_id=self.project_id, dataset_id=self.dataset_id)
        ])

        with self.assertRaises(RuntimeError) as ctx:
            self.rule_instance.derive_pediatric_guardian_links(self.client)

        self.assertIn('4 person domain', str(ctx.exception))

    def test_sandbox_and_remove_pids_list(self):
        """
        Validates that the data for participants in the lookup table has been removed. 
        """
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'loaded_ids': [
                10101, 10102, 10201, 10202, 10301, 10302, 10401, 10402, 20101,
                20102, 20201, 20202, 20301, 20302, 20401, 20402, 30101, 30102,
                30201, 30202, 30301, 30302, 30401, 30402, 40101, 40102, 40201,
                40202, 40301, 40302, 40401, 40402
            ],
            'sandboxed_ids': [
                10401, 10402, 20201, 20202, 20401, 20402, 30101, 30102, 40101,
                40102, 40301, 40302
            ],
            'cleaned_values': [
                (10101, 101, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (10102, 101, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (10201, 102, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (10202, 102, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (10301, 103, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (10302, 103, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (20101, 201, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (20102, 201, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (20301, 203, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (20302, 203, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (30201, 302, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (30202, 302, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (30301, 303, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (30302, 303, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (30401, 304, 0, datetime.fromisoformat('2022-01-04').date(), 0),
                (30402, 304, 0, datetime.fromisoformat('2022-01-04').date(), 0),
                (40201, 402, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (40202, 402, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (40401, 404, 0, datetime.fromisoformat('2022-01-04').date(), 0),
                (40402, 404, 0, datetime.fromisoformat('2022-01-04').date(), 0)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'race_concept_id', 'ethnicity_concept_id'
            ],
            'loaded_ids': [
                101, 102, 103, 104, 201, 202, 203, 204, 301, 302, 303, 304, 401,
                402, 403, 404
            ],
            'sandboxed_ids': [104, 202, 204, 301, 401, 403],
            'cleaned_values': [(101, 0, 1991, 0, 0), (102, 0, 1992, 0, 0),
                               (103, 0, 1993, 0, 0), (201, 0, 1991, 0, 0),
                               (203, 0, 1993, 0, 0), (302, 0, 1992, 0, 0),
                               (303, 0, 1993, 0, 0), (304, 0, 1994, 0, 0),
                               (402, 0, 1992, 0, 0), (404, 0, 1994, 0, 0)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[2],
            'fields': [
                'aou_death_id', 'person_id', 'death_date',
                'death_type_concept_id', 'cause_concept_id',
                'cause_source_concept_id', 'src_id', 'primary_death_record'
            ],
            'loaded_ids': [
                'a10101', 'a10202', 'a10301', 'a10402', 'a20102', 'a20202',
                'a20302', 'a20401', 'a30101', 'a30202', 'a30302', 'a30401',
                'a40101', 'a40202', 'a40301', 'a40401'
            ],
            'sandboxed_ids': [
                'a10402', 'a20202', 'a20401', 'a30101', 'a40101', 'a40301'
            ],
            'cleaned_values': [
                ('a10101', 101, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
                ('a10202', 102, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 1', False),
                ('a10301', 103, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
                ('a20102', 201, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 2', False),
                ('a20302', 203, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 2', False),
                ('a30202', 302, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 3', False),
                ('a30302', 303, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 3', False),
                ('a30401', 304, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
                ('a40202', 402, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 4', False),
                ('a40401', 404, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
            ]
        }]
        self.default_test(tables_and_counts)
