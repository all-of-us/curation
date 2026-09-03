"""
Integration test for the DL-2486 pediatric withdrawal and deactivation cascades.

A participant aged 0 to 6 cannot consent for themselves, so a guardian consents
on their behalf and the two are linked. This test covers the five lifecycle
events the CT+ release requirements name, from the pediatric side:

    event                                       adult          linked child
    ------------------------------------------- -------------- ----------------
    pediatric participant withdraws             not affected   excluded
    pediatric participant deactivates           not affected   kept to the date
    adult withdraws                             excluded       deactivated
    adult withdraws, child withdrawn manually   excluded       excluded
    adult deactivates                           kept to date   deactivated

Rows one, two and four are the existing flat-list behaviour and are covered by
`sandbox_and_remove_withdrawn_pids_test` and by the participant's own row in the
participant summary. Rows three and five are the cascades this test exercises:
they resolve through the adult to pediatric pairs that the RDR stage writes to
`_pediatric_guardian_links`.

Original Issues: DL2486
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import (DRC_OPS, JINJA_ENV, OBSERVATION,
                    PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE, PS_AWARDEE)
from cdr_cleaner.cleaning_rules.remove_participant_data_past_deactivation_date import (
    DEACTIVATED_PARTICIPANTS, RemoveParticipantDataPastDeactivationDate)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

PEDIATRIC_LINKS_SCHEMA = [{
    'type': 'integer',
    'name': 'adult_person_id',
    'mode': 'nullable'
}, {
    'type': 'integer',
    'name': 'pediatric_person_id',
    'mode': 'nullable'
}]

# 1xx are adults, 2xx are the pediatric participants aged 0 to 6.
PEDIATRIC_LINKS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{sandbox_id}}.{{links_table}}`
    (adult_person_id, pediatric_person_id)
VALUES
    -- 101 withdraws, so 201 is deactivated at the withdrawal date. --
    (101, 201),
    -- 102 deactivates, so 202 is deactivated at the deactivation date. --
    (102, 202),
    -- 103 deactivates and is linked to two children, so both are deactivated. --
    (103, 203),
    (103, 204),
    -- 104 has no lifecycle event, so 205 is untouched. --
    (104, 205),
    -- 207 is linked to withdrawn 101 and is also deactivated later in their --
    -- own right. The earlier of the two dates wins. --
    (101, 207)
""")

PERSON_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
    (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
    (101, 0, 1985, 0, 0),
    (102, 0, 1986, 0, 0),
    (103, 0, 1987, 0, 0),
    (104, 0, 1988, 0, 0),
    (105, 0, 1989, 0, 0),
    (201, 0, 2021, 0, 0),
    (202, 0, 2021, 0, 0),
    (203, 0, 2021, 0, 0),
    (204, 0, 2021, 0, 0),
    (205, 0, 2021, 0, 0),
    (206, 0, 2021, 0, 0),
    (207, 0, 2021, 0, 0)
""")

# Every participant carries one observation before any relevant cut-off date and
# one after it, so a participant that is not deactivated keeps both rows and a
# participant that is deactivated keeps exactly the earlier one. That makes an
# empty cohort distinguishable from a cohort acted on: if nothing cascades, all
# 24 rows survive.
OBSERVATION_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
    (observation_id, person_id, observation_concept_id, observation_date,
     observation_datetime, observation_type_concept_id)
VALUES
    (1011, 101, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (1012, 101, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (1021, 102, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (1022, 102, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (1031, 103, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (1032, 103, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (1041, 104, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (1042, 104, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (1051, 105, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (1052, 105, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2011, 201, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2012, 201, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2021, 202, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2022, 202, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2031, 203, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2032, 203, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2041, 204, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2042, 204, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2051, 205, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2052, 205, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2061, 206, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2062, 206, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0),
    (2071, 207, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    (2072, 207, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0)
""")

# `withdrawal_status` and `suspension_status` carry the sentinel the rule tests
# against, `not_withdrawn` and `not_deactivated`. Anything else counts as the
# event having happened, which is the predicate the rule already used for
# deactivation and which this change reuses for withdrawal.
PS_AWARDEE_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{drc_ops}}.{{ps_awardee}}`
    (person_id, suspension_status, suspension_time, withdrawal_status, withdrawal_time)
VALUES
    -- 101 withdrew. The adult's own removal is the withdrawal rule's job at the --
    -- RDR stage, so 101 must not appear in the deactivation lookup. --
    (101, 'not_deactivated', NULL, 'withdrawn', '2020-01-01 00:00:00'),
    -- 102 and 103 deactivated in their own right. --
    (102, 'deactivated', '2020-06-01 00:00:00', 'not_withdrawn', NULL),
    (103, 'deactivated', '2020-06-01 00:00:00', 'not_withdrawn', NULL),
    -- 104 and 105 had no lifecycle event at all. --
    (104, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    (105, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    -- 206 is a pediatric participant deactivated in their own right, with no --
    -- linked adult. This is the second row of the release requirements table. --
    (206, 'deactivated', '2021-01-01 00:00:00', 'not_withdrawn', NULL),
    -- 207 is deactivated in their own right in 2022, later than the 2020 --
    -- withdrawal of the adult they are linked to. --
    (207, 'deactivated', '2022-01-01 00:00:00', 'not_withdrawn', NULL)
""")


class PediatricLifecycleCascadesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # The pairs lookup is written at the RDR stage. Pointing the parameter at
        # the sandbox this test already creates keeps the fixture to one dataset;
        # what the test proves is that the rule requires the parameter and reads
        # the lookup it names.
        cls.rdr_sandbox_id = cls.sandbox_id

        cls.kwargs = {
            'table_namer': 'table_namer',
            'api_project_id': 'foo-project-id',
            'rdr_sandbox_dataset_id': cls.rdr_sandbox_id
        }
        cls.rule_instance = RemoveParticipantDataPastDeactivationDate(
            cls.project_id, cls.dataset_id, cls.sandbox_id, **cls.kwargs)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{table_name}'
            for table_name in cls.rule_instance.get_sandbox_tablenames()
        ]
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{DEACTIVATED_PARTICIPANTS}')

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table_name}'
            for table_name in cls.rule_instance.affected_tables
        ]
        cls.fq_table_names.append(f'{cls.project_id}.{DRC_OPS}.{PS_AWARDEE}')

        cls.fq_links_table = (f'{cls.project_id}.{cls.rdr_sandbox_id}.'
                              f'{PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE}')

        super().setUpClass()

    def setUp(self):
        """
        Create the pairs lookup and load the fixture.
        """
        self.client.create_table(Table(self.fq_links_table,
                                       PEDIATRIC_LINKS_SCHEMA),
                                 exists_ok=True)

        self.load_statements = [
            PEDIATRIC_LINKS_TEMPLATE.render(
                project_id=self.project_id,
                sandbox_id=self.rdr_sandbox_id,
                links_table=PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE),
            PERSON_TEMPLATE.render(project_id=self.project_id,
                                   dataset_id=self.dataset_id),
            OBSERVATION_TEMPLATE.render(project_id=self.project_id,
                                        dataset_id=self.dataset_id),
            PS_AWARDEE_TEMPLATE.render(project_id=self.project_id,
                                       drc_ops=DRC_OPS,
                                       ps_awardee=PS_AWARDEE),
        ]

        super().setUp()

    def tearDown(self):
        """
        Drop the pairs lookup. It is not in `fq_sandbox_table_names` because it
        has to exist before the rule runs, and the base class asserts every table
        in that list is absent at that point.
        """
        self.client.delete_table(self.fq_links_table, not_found_ok=True)
        super().tearDown()

    def rows(self, query):
        return [
            tuple(row.values()) for row in self.client.query(query).result()
        ]

    def test_pediatric_lifecycle_cascades(self):
        """
        Run the rule and check both what it recorded and what it removed.
        """
        # The cohort this test acts on, reported so a pass cannot come from an
        # empty fixture.
        self.assertEqual(
            self.rows(f'SELECT COUNT(*) FROM `{self.fq_links_table}`')[0][0], 6,
            'the adult to pediatric pairs lookup did not load')

        fq_observation = f'{self.project_id}.{self.dataset_id}.{OBSERVATION}'
        self.default_test([{
            'name':
                OBSERVATION,
            'fq_table_name':
                fq_observation,
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'fields': ['observation_id', 'person_id'],
            'loaded_ids': [
                1011, 1012, 1021, 1022, 1031, 1032, 1041, 1042, 1051, 1052,
                2011, 2012, 2021, 2022, 2031, 2032, 2041, 2042, 2051, 2052,
                2061, 2062, 2071, 2072
            ],
            # Every 2023 row belonging to a deactivated participant, and nothing
            # else. 101 withdrew but was not deactivated, so 1012 survives here
            # and the withdrawn adult is removed at the RDR stage instead.
            'sandboxed_ids': [1022, 1032, 2012, 2022, 2032, 2042, 2062, 2072],
            'cleaned_values': [
                (1011, 101), (1012, 101), (1021, 102), (1031, 103), (1041, 104),
                (1042, 104), (1051, 105), (1052, 105), (2011, 201), (2021, 202),
                (2031, 203), (2041, 204), (2051, 205), (2052, 205), (2061, 206),
                (2071, 207)
            ]
        }])

        deactivated = {
            person_id: str(deactivated_datetime)[:10]
            for person_id, deactivated_datetime in self.rows(f"""
                SELECT person_id, deactivated_datetime
                FROM `{self.project_id}.{self.sandbox_id}.{DEACTIVATED_PARTICIPANTS}`
            """)
        }

        # 102 and 103 are deactivated in their own right and stay so.
        # 201 inherits the 2020-01-01 withdrawal of adult 101.
        # 202, 203 and 204 inherit the 2020-06-01 deactivation of their adult,
        # including both children of 103.
        # 206 is deactivated in their own right with no linked adult.
        # 207 is reachable twice and keeps the earlier date, 2020-01-01, rather
        # than their own later 2022-01-01 deactivation.
        self.assertEqual(
            deactivated, {
                102: '2020-06-01',
                103: '2020-06-01',
                201: '2020-01-01',
                202: '2020-06-01',
                203: '2020-06-01',
                204: '2020-06-01',
                206: '2021-01-01',
                207: '2020-01-01',
            })

        # 101 withdrew but was never suspended. Removing a withdrawn adult is the
        # withdrawal rule's job at the RDR stage, so this rule must leave them
        # alone rather than treating a withdrawal as a suspension.
        self.assertNotIn(101, deactivated)
        # 104 has a linked child but no lifecycle event, 105 has neither, and 205
        # is linked to an adult who had no event, so nothing cascades to any.
        self.assertNotIn(104, deactivated)
        self.assertNotIn(105, deactivated)
        self.assertNotIn(205, deactivated)
