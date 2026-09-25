"""
Integration test for the DL-2486 pediatric withdrawal and deactivation cascades.

The CT+ release requirements name five lifecycle events for a 0-6 participant
and the guardian linked to them:

    event                                       adult          linked child
    ------------------------------------------- -------------- ----------------
    pediatric participant withdraws             not affected   excluded
    pediatric participant deactivates           not affected   kept to the date
    adult withdraws                             excluded       deactivated
    adult withdraws, child withdrawn manually   excluded       excluded
    adult deactivates                           kept to date   deactivated

Three rules deliver them, so this test runs them in pipeline order on one
dataset and checks CDM, Fitbit and aou_death for every participant:

    SandboxAndRemoveWithdrawnPids              RDR: removes the withdrawn, writes
                                               the pairs
    RemoveParticipantDataPastDeactivationDate  combined, fitbit: truncates at the
                                               deactivation date
    RemoveNonExistingPids                      fitbit: drops Fitbit rows of anyone
                                               no longer in person

Original Issues: DL2486
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import (AOU_DEATH, DRC_OPS, FACT_RELATIONSHIP, JINJA_ENV,
                    OBSERVATION, PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE, PERSON,
                    PS_AWARDEE, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
import cdr_cleaner.clean_cdr_engine as engine
from cdr_cleaner.cleaning_rules.remove_non_existing_pids import RemoveNonExistingPids
from cdr_cleaner.cleaning_rules.remove_participant_data_past_deactivation_date import (
    DEACTIVATED_PARTICIPANTS, RemoveParticipantDataPastDeactivationDate)
from cdr_cleaner.cleaning_rules.sandbox_and_remove_withdrawn_pids import SandboxAndRemoveWithdrawnPids
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

ACTIVITY_SUMMARY = 'activity_summary'
WITHDRAWN_LIST = 'pdr_withdrawals_list'

# 1xx are adults, 2xx are 0-6. Each event has its own pair, sharing a last digit.
ADULTS = [111, 112, 113, 114, 115, 116]
CHILDREN = [211, 212, 213, 214, 215, 216]

# Each participant has a 2019 and a 2023 row in observation and Fitbit, and a
# 2023 aou_death row. Every cut-off falls in between, so kept keeps everything,
# truncated keeps the 2019 rows, and excluded keeps nothing.
KEPT, TRUNCATED, EXCLUDED = 'kept', 'truncated', 'excluded'

EVENTS = {
    'pediatric participant withdraws': {
        111: KEPT,
        211: EXCLUDED
    },
    'pediatric participant deactivates': {
        112: KEPT,
        212: TRUNCATED
    },
    'adult withdraws': {
        113: EXCLUDED,
        213: TRUNCATED
    },
    'adult withdraws, child withdrawn manually': {
        114: EXCLUDED,
        214: EXCLUDED
    },
    'adult deactivates': {
        115: TRUNCATED,
        215: TRUNCATED
    },
    'no lifecycle event': {
        116: KEPT,
        216: KEPT
    },
}

UNDER18_SCHEMA = [{
    'type': 'integer',
    'name': 'person_id',
    'mode': 'nullable'
}, {
    'type': 'integer',
    'name': 'age_at_consent',
    'mode': 'nullable'
}, {
    'type': 'string',
    'name': 'age_band',
    'mode': 'nullable'
}]

WITHDRAWN_LIST_SCHEMA = [{
    'type': 'integer',
    'name': 'person_id',
    'mode': 'nullable'
}, {
    'type': 'integer',
    'name': 'hpo_id',
    'mode': 'nullable'
}, {
    'type': 'string',
    'name': 'src_id',
    'mode': 'nullable'
}, {
    'type': 'date',
    'name': 'consent_for_study_enrollment_authored',
    'mode': 'nullable'
}, {
    'type': 'string',
    'name': 'withdrawal_status',
    'mode': 'nullable'
}]

FIXTURE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
    (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
{%- for pid in people %}
    ({{pid}}, 0, {{ 1985 if pid < 200 else 2021 }}, 0, 0){{ ',' if not loop.last }}
{%- endfor %};

INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
    (observation_id, person_id, observation_concept_id, observation_date,
     observation_datetime, observation_type_concept_id)
VALUES
{%- for pid in people %}
    ({{pid}}1, {{pid}}, 0, '2019-01-01', '2019-01-01 00:00:00 UTC', 0),
    ({{pid}}2, {{pid}}, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 0){{ ',' if not loop.last }}
{%- endfor %};

INSERT INTO `{{project_id}}.{{dataset_id}}.activity_summary`
    (person_id, date, steps)
VALUES
{%- for pid in people %}
    ({{pid}}, '2019-01-01', 1),
    ({{pid}}, '2023-01-01', 1){{ ',' if not loop.last }}
{%- endfor %};

INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
    (aou_death_id, person_id, death_date, death_type_concept_id, src_id,
     primary_death_record)
VALUES
{%- for pid in people %}
    ('d{{pid}}', {{pid}}, '2023-01-01', 0, 'src', TRUE){{ ',' if not loop.last }}
{%- endfor %};

-- The linkage rows. 111 to 211 arrives in both directions, 216 sits on side 1, --
-- and 115 to 215 carries relationship concept 0 because the guardian skipped --
-- the relationship question. All six are pairs. --
INSERT INTO `{{project_id}}.{{dataset_id}}.fact_relationship`
    (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
VALUES
    (56, 111, 56, 211, 4053608),
    (56, 211, 56, 111, 4326600),
    (56, 112, 56, 212, 4053608),
    (56, 113, 56, 213, 4301632),
    (56, 114, 56, 214, 4053608),
    (56, 115, 56, 215, 0),
    (56, 216, 56, 116, 4326600);

INSERT INTO `{{project_id}}.{{sandbox_id}}.{{under18_table}}`
    (person_id, age_at_consent, age_band)
VALUES
{%- for pid in children %}
    ({{pid}}, 3, '0-6'){{ ',' if not loop.last }}
{%- endfor %};

-- The RDR removal list. 214 is the child withdrawn manually after their adult. --
INSERT INTO `{{project_id}}.{{dataset_id}}.{{withdrawn_list}}`
    (person_id, withdrawal_status)
VALUES
    (211, 'NO_USE'),
    (113, 'NO_USE'),
    (114, 'NO_USE'),
    (214, 'NO_USE');

-- The participant summary. `not_withdrawn` and `not_deactivated` are the --
-- sentinels the rule tests against; anything else is the event. --
INSERT INTO `{{project_id}}.{{drc_ops}}.{{ps_awardee}}`
    (person_id, suspension_status, suspension_time, withdrawal_status, withdrawal_time)
VALUES
    (111, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    (211, 'not_deactivated', NULL, 'withdrawn', '2020-01-01 00:00:00'),
    (112, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    (212, 'deactivated', '2021-01-01 00:00:00', 'not_withdrawn', NULL),
    (113, 'not_deactivated', NULL, 'withdrawn', '2020-01-01 00:00:00'),
    (213, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    (114, 'not_deactivated', NULL, 'withdrawn', '2020-01-01 00:00:00'),
    (214, 'not_deactivated', NULL, 'withdrawn', '2020-03-01 00:00:00'),
    (115, 'deactivated', '2020-06-01 00:00:00', 'not_withdrawn', NULL),
    (215, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    (116, 'not_deactivated', NULL, 'not_withdrawn', NULL),
    (216, 'not_deactivated', NULL, 'not_withdrawn', NULL)
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

        # Created from their schema files by the base class.
        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table}' for table in [
                PERSON, OBSERVATION, ACTIVITY_SUMMARY, AOU_DEATH,
                FACT_RELATIONSHIP
            ]
        ] + [f'{cls.project_id}.{DRC_OPS}.{PS_AWARDEE}']

        # No schema files, so created here. Listing the lookup as a sandbox table
        # makes the base class create the sandbox dataset and drop it after.
        cls.fq_withdrawn_list = (f'{cls.project_id}.{cls.dataset_id}.'
                                 f'{WITHDRAWN_LIST}')
        cls.fq_under18_table = (f'{cls.project_id}.{cls.sandbox_id}.'
                                f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}')
        cls.fq_sandbox_table_names = [cls.fq_under18_table]

        # One sandbox serves as the RDR stage sandbox too, so the deactivation
        # rule reads the pairs where the withdrawal rule writes them.
        cls.kwargs = {
            'withdrawn_dups_table': WITHDRAWN_LIST,
            'rdr_sandbox_dataset_id': cls.sandbox_id,
            'api_project_id': 'foo-project-id',
            'reference_dataset_id': cls.dataset_id,
        }

        super().setUpClass()

    def setUp(self):
        super().setUp()

        # The rules' sandbox tables depend on what they find, so tearDown drops
        # whatever is new.
        self.preexisting_sandbox_tables = {
            table.table_id for table in self.client.list_tables(self.sandbox_id)
        }

        self.client.create_table(Table(self.fq_withdrawn_list,
                                       WITHDRAWN_LIST_SCHEMA),
                                 exists_ok=True)
        self.client.create_table(Table(self.fq_under18_table, UNDER18_SCHEMA),
                                 exists_ok=True)

        self.load_test_data([
            FIXTURE.render(project_id=self.project_id,
                           dataset_id=self.dataset_id,
                           sandbox_id=self.sandbox_id,
                           under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                           withdrawn_list=WITHDRAWN_LIST,
                           drc_ops=DRC_OPS,
                           ps_awardee=PS_AWARDEE,
                           people=ADULTS + CHILDREN,
                           children=CHILDREN)
        ])

    def tearDown(self):
        """
        Drop the withdrawal list and the sandbox tables the rules created.
        """
        self.client.delete_table(self.fq_withdrawn_list, not_found_ok=True)
        for table in self.client.list_tables(self.sandbox_id):
            if table.table_id not in self.preexisting_sandbox_tables:
                self.client.delete_table(table, not_found_ok=True)
        super().tearDown()

    def rows(self, query):
        return [
            tuple(row.values()) for row in self.client.query(query).result()
        ]

    def surviving(self, table, columns):
        """Rows left in `table` per person_id, as a set of `columns` values."""
        survivors = {}
        for person_id, *values in self.rows(f"""
                SELECT person_id, {', '.join(columns)}
                FROM `{self.project_id}.{self.dataset_id}.{table}`
            """):
            survivors.setdefault(person_id,
                                 set()).add(tuple(str(v) for v in values))
        return survivors

    def test_five_lifecycle_events_across_cdm_fitbit_and_aou_death(self):
        # The input, reported so a pass cannot come from an empty fixture.
        self.assertEqual(
            self.rows(f'SELECT COUNT(*) FROM `{self.project_id}.'
                      f'{self.dataset_id}.{FACT_RELATIONSHIP}`')[0][0], 7,
            'the linkage fixture did not load')
        self.assertEqual(
            self.rows(f'SELECT COUNT(*) FROM `{self.project_id}.'
                      f'{self.dataset_id}.{OBSERVATION}`')[0][0], 24,
            'the observation fixture did not load')

        engine.clean_dataset(self.project_id,
                             self.dataset_id,
                             self.sandbox_id,
                             [(SandboxAndRemoveWithdrawnPids,),
                              (RemoveParticipantDataPastDeactivationDate,),
                              (RemoveNonExistingPids,)],
                             table_namer='peds',
                             **self.kwargs)

        # The cohort the cascades acted on.
        pairs = sorted(
            self.rows(f"""
                SELECT adult_person_id, pediatric_person_id
                FROM `{self.project_id}.{self.sandbox_id}.{PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE}`
            """))
        self.assertEqual(pairs, [(111, 211), (112, 212), (113, 213), (114, 214),
                                 (115, 215), (116, 216)])

        deactivated = {
            person_id: str(deactivated_datetime)[:10]
            for person_id, deactivated_datetime in self.rows(f"""
                SELECT person_id, deactivated_datetime
                FROM `{self.project_id}.{self.sandbox_id}.{DEACTIVATED_PARTICIPANTS}`
            """)
        }
        # 212 and 115 in their own right; 213 and 214 inherit their adult's
        # withdrawal date, 215 their adult's deactivation date. Withdrawn
        # participants are removed rather than deactivated, so 211, 113 and 114
        # are absent.
        self.assertEqual(
            deactivated, {
                212: '2021-01-01',
                213: '2020-01-01',
                214: '2020-01-01',
                115: '2020-06-01',
                215: '2020-06-01',
            })

        observation = self.surviving(OBSERVATION, ['observation_date'])
        fitbit = self.surviving(ACTIVITY_SUMMARY, ['date'])
        aou_death = self.surviving(AOU_DEATH, ['death_date'])

        expected_dated = {
            KEPT: {('2019-01-01',), ('2023-01-01',)},
            TRUNCATED: {('2019-01-01',)},
            EXCLUDED: None,
        }
        expected_death = {
            KEPT: {('2023-01-01',)},
            TRUNCATED: None,
            EXCLUDED: None,
        }

        for event, outcomes in EVENTS.items():
            for person_id, outcome in outcomes.items():
                with self.subTest(event=event,
                                  person_id=person_id,
                                  outcome=outcome):
                    self.assertEqual(observation.get(person_id),
                                     expected_dated[outcome], OBSERVATION)
                    self.assertEqual(fitbit.get(person_id),
                                     expected_dated[outcome], ACTIVITY_SUMMARY)
                    self.assertEqual(aou_death.get(person_id),
                                     expected_death[outcome], AOU_DEATH)
