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
        -- Adult 101 is linked to two pediatric participants. Both are pairs. --
        (56, 101, 56, 201, 4326600),
        (56, 101, 56, 202, 4311425),
        -- Adult 102 is linked to one. --
        (56, 102, 56, 203, 4032151),
        -- 210 is in the 7-17 band, so this is not a CT+ pediatric pair. --
        (56, 103, 56, 210, 4326600),
        -- 210 is 7-17 and so cannot consent for anyone. A sibling relationship --
        -- to 0-6 participant 201 must not make them 201's guardian, which is --
        -- what an anti-join restricted to the 0-6 band would have allowed. --
        (56, 210, 56, 201, 4218412),
        -- 999 is not in the cohort lookup at all. --
        (56, 104, 56, 999, 4326600),
        -- Natural Sibling is symmetric and both sides are pediatric, so the --
        -- relationship concept alone cannot tell them apart. Not a pair. --
        (56, 203, 56, 220, 4218412),
        -- Not a family relationship concept at all, so not a pair. --
        (56, 105, 56, 201, 0),
        -- A self reference is never a pair. --
        (56, 201, 56, 201, 4326600),
        -- A measurement to observation row, which this query must ignore. --
        (21, 5, 27, 6, 4326600)
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

        # The pairs lookup the rule now derives. It is created by the rule, so it
        # belongs in the sandbox list, which is both cleaned up and asserted
        # absent before the run.
        cls.fq_links_table = (f'{cls.project_id}.{cls.sandbox_id}.'
                              f'{PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE}')
        cls.fq_sandbox_table_names.append(cls.fq_links_table)

        # `fact_relationship` supplies the adult to pediatric pairs. It carries no
        # `person_id` column, so the removal never touches it. This one does have
        # a schema file, so the base class can create it.
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{FACT_RELATIONSHIP}')

        # Created and dropped by this class rather than by the base class,
        # because neither has a schema file to resolve.
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

        # Neither of these two fixture tables has a schema file, and
        # `BaseTest.setUp` resolves every name in `fq_table_names` through
        # `resources.fields_for`, which raises on a name it cannot find. So they
        # are created here and dropped in `tearDown` instead of being added to
        # that list. `fq_table_names` is a class attribute, so appending to it
        # also leaks across test methods.
        self.client.create_table(Table(self.fq_withdrawn_dups_table,
                                       LOOKUP_TABLE_SCHEMA),
                                 exists_ok=True)

        # Build temp records lookup table query
        lookup_table_query = LOOKUP_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            lookup_table=self.withdrawn_dups_table)

        # The pediatric cohort lookup, written by FlagParticipantsUnder18Years at
        # this same stage rather than by this rule.
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
        Drop the two fixture tables that are deliberately absent from
        `fq_table_names`, so the next test method starts clean.
        """
        for fq_table in [self.fq_withdrawn_dups_table, self.fq_under18_table]:
            self.client.delete_table(fq_table, not_found_ok=True)
        super().tearDown()

    def test_pediatric_guardian_links_are_derived(self):
        """
        The rule resolves the adult to pediatric pairs while `fact_relationship`
        and the cohort lookup are both in reach, because the later stages that
        consume them carry neither table.
        """
        loaded = list(
            self.client.query(
                f'SELECT COUNT(*) AS n FROM '
                f'`{self.project_id}.{self.dataset_id}.{FACT_RELATIONSHIP}`').
            result())[0].n
        self.assertEqual(loaded, 10,
                         'the fact_relationship fixture did not load')

        # Runs the rule with no per-table expectations; the pairs lookup is what
        # this test asserts on.
        self.default_test([])

        pairs = sorted((row.adult_person_id, row.pediatric_person_id)
                       for row in self.client.query(f"""
                SELECT adult_person_id, pediatric_person_id
                FROM `{self.fq_links_table}`
            """).result())

        # 103 to 210 puts a 7-17 participant on the child side, 210 to 201 puts
        # one on the adult side, 104 to 999 is not in the cohort, 203 to 220 is
        # a sibling relationship between two pediatric participants, 201 to 201
        # is a self reference, and the 21 to 27 row is not person to person.
        # None of them is a pair.
        self.assertEqual(pairs, [(101, 201), (101, 202), (102, 203)])

        # Stated separately because it is the case a band-restricted anti-join
        # would have got wrong: a 17 year old sibling is in the lookup, so they
        # are not eligible to be anyone's guardian.
        self.assertNotIn((210, 201), pairs)

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
