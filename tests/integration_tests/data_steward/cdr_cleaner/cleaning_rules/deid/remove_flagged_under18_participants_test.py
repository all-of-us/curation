"""
Integration test for remove_flagged_under18_participants module

Original Issues: DL2418

The intent is to remove data for participants flagged in the
_under18_participants lookup at the tier stages, optionally retaining one age
band so the CT+ pediatric run keeps ages 0 to 6.
"""

# Python Imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
import cdr_cleaner.clean_cdr as clean_cdr
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION, VISIT_OCCURRENCE, UNDER18_PARTICIPANTS_LOOKUP_TABLE
from cdr_cleaner.cleaning_rules.deid.remove_flagged_under18_participants import (
    RemoveFlaggedUnder18Participants, RemoveFlaggedUnder18ParticipantsCtPlus)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

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

UNDER18_LOOKUP_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{lookup_table}}`
(person_id, age_at_consent, age_band)
VALUES
      /* Participants 1 and 2 were 18 or older at consent, so they are not flagged. */
      /* Participant 3 was 17 at consent. Removed by every variant of the rule. */
      /* Participant 4 was 1 at consent. Removed by default, retained by the CT+ variant. */
      (3, 17, '7-17'),
      (4, 1, '0-6')
""")

VISIT_OCCURRENCE_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.visit_occurrence`
(visit_occurrence_id, person_id, visit_start_date, visit_end_date, visit_concept_id, visit_type_concept_id)
VALUES
      (1, 1, '2020-01-01', '2020-01-02', 0, 0),
      (2, 2, '2020-01-02', '2020-01-03', 0, 0),
      (3, 3, '2020-01-01', '2020-03-01', 0, 0),
      (4, 4, '2020-01-02', '2022-01-03', 0, 0)
""")

OBSERVATION_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
(observation_id, person_id, observation_date, observation_concept_id, observation_source_concept_id, observation_type_concept_id)
VALUES
      (11, 1, '2020-01-01', 0, 0, 0),
      (12, 1, '2020-01-01', 1585482, 0, 0),
      (21, 2, '2020-01-01', 0, 0, 0),
      (22, 2, '2020-01-01', 0, 1585482, 0),
      (31, 3, '2020-03-01', 0, 0, 0),
      (32, 3, '2021-02-28', 1585482, 0, 0),
      (41, 4, '2022-01-01', 0, 0, 0),
      (42, 4, '2022-01-01', 0, 1585482, 0)
""")


class RemoveFlaggedUnder18ParticipantsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets. The lookup lives in a dataset other
        # than the one being cleaned, which is the point of the parameter: at the
        # tier stages the rule cannot see the RDR sandbox through its own
        # sandbox_dataset_id.
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.under18_lookup_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {
            'under18_lookup_dataset_id': cls.under18_lookup_dataset_id
        }

        cls.rule_instance = RemoveFlaggedUnder18Participants(
            cls.project_id, cls.dataset_id, cls.sandbox_id, **cls.kwargs)

        cls.fq_lookup_table_name = (f'{cls.project_id}.'
                                    f'{cls.under18_lookup_dataset_id}.'
                                    f'{UNDER18_PARTICIPANTS_LOOKUP_TABLE}')

        # Generates list of fully qualified table names and their corresponding
        # sandbox table names. Only the two tested domain tables are listed; the
        # rule narrows itself to the tables that exist in the dataset.
        for table_name in [VISIT_OCCURRENCE, OBSERVATION]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.'
                f'{cls.rule_instance.sandbox_table_for(table_name)}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on and populate the lookup.
        """
        super().setUp()

        # The lookup has no schema in resources, so it is created explicitly
        # rather than through create_tables. tearDown drops it after each test.
        self.client.create_table(
            Table(self.fq_lookup_table_name, UNDER18_LOOKUP_SCHEMA))
        if self.fq_lookup_table_name not in self.fq_table_names:
            self.fq_table_names.append(self.fq_lookup_table_name)

        under18_lookup_data_query = UNDER18_LOOKUP_DATA_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.under18_lookup_dataset_id,
            lookup_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE)
        visit_occurrence_data_query = VISIT_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        observation_data_query = OBSERVATION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{under18_lookup_data_query};
                {visit_occurrence_data_query};
                {observation_data_query}'''
        ])

    def _tables_and_counts(self, sandboxed_visit_ids, cleaned_visit_values,
                           sandboxed_observation_ids,
                           cleaned_observation_values):
        """
        Build the default_test expectations for the two tested domain tables.
        """
        return [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{VISIT_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(VISIT_OCCURRENCE)}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids':
                sandboxed_visit_ids,
            'fields': ['visit_occurrence_id', 'person_id'],
            'cleaned_values':
                cleaned_visit_values
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids': [11, 12, 21, 22, 31, 32, 41, 42],
            'sandboxed_ids':
                sandboxed_observation_ids,
            'fields': ['observation_id', 'person_id'],
            'cleaned_values':
                cleaned_observation_values
        }]

    def test_remove_all_flagged_participants(self):
        """
        With no age band to retain, every flagged participant is removed.
        """
        self.rule_instance = RemoveFlaggedUnder18Participants(
            self.project_id, self.dataset_id, self.sandbox_id, **self.kwargs)

        self.default_test(
            self._tables_and_counts(sandboxed_visit_ids=[3, 4],
                                    cleaned_visit_values=[(1, 1), (2, 2)],
                                    sandboxed_observation_ids=[31, 32, 41, 42],
                                    cleaned_observation_values=[
                                        (11, 1), (12, 1), (21, 2), (22, 2)
                                    ]))

    def test_ct_plus_retains_the_pediatric_band(self):
        """
        The CT+ variant keeps the '0-6' band and removes '7-17'.

        age_band_to_retain is passed here on purpose: the subclass does not
        declare it, so the engine drops it and the pinned '0-6' stands. If it
        were overridable, participant 4 would be removed and this test fails.
        """
        self.rule_instance = RemoveFlaggedUnder18ParticipantsCtPlus(
            self.project_id, self.dataset_id, self.sandbox_id, **self.kwargs)
        self.kwargs = dict(self.kwargs, age_band_to_retain='7-17')

        self.default_test(
            self._tables_and_counts(sandboxed_visit_ids=[3],
                                    cleaned_visit_values=[(1, 1), (2, 2),
                                                          (4, 4)],
                                    sandboxed_observation_ids=[31, 32],
                                    cleaned_observation_values=[
                                        (11, 1), (12, 1), (21, 2), (22, 2),
                                        (41, 4), (42, 4)
                                    ]))

    def test_invalid_age_band_is_rejected(self):
        """
        An age band outside the two the lookup writes fails at construction.
        """
        with self.assertRaises(ValueError):
            RemoveFlaggedUnder18Participants(self.project_id,
                                             self.dataset_id,
                                             self.sandbox_id,
                                             age_band_to_retain='0-17',
                                             **self.kwargs)

    def test_missing_lookup_dataset_fails_validation(self):
        """
        A run without under18_lookup_dataset_id fails before any query executes.
        """
        with self.assertRaises(RuntimeError):
            clean_cdr.validate_custom_params([
                (RemoveFlaggedUnder18Participants,)
            ])
