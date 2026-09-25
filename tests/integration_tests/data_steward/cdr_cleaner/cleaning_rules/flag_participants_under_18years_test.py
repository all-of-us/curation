"""
Integration test for flag_participants_under_18years module

Original Issues: DL2416, DL2501

The intent is to record every participant who was under 18 at consent, with the
age band that decides whether a tier retains them, and to record every
participant whose consent date does not resolve rather than dropping them
silently.
"""

# Python Imports
import os

# Project Imports
from common import SURVEY_CONDUCT, VISIT_OCCURRENCE, OBSERVATION, PERSON, JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.flag_participants_under_18years import FlagParticipantsUnder18Years
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, birth_datetime, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
      -- Participant 1 ... 50 years old at consent --
      -- Participant 2 ... 18 years 0 day old at consent --
      -- Participant 3 ... 17 years 364 days old at consent -> flagged '7-17' --
      -- Participant 4 ... guardian consented, 1 year old at consent -> flagged '0-6' --
      -- Participant 5 ... guardian consented, 3 years old at consent -> flagged '0-6' --
      -- Participant 6 ... consent row carries the 0001-01-01 sentinel -> unresolved --
      -- Participant 7 ... surveyed but never consented -> unresolved --
      (1, '1970-01-01 00:00:00 UTC', 0, 1970, 0, 0),
      (2, '2002-01-01 00:00:00 UTC', 0, 2002, 0, 0),
      (3, '2003-03-01 00:00:00 UTC', 0, 2003, 0, 0),
      (4, '2021-01-01 00:00:00 UTC', 0, 2021, 0, 0),
      (5, '2020-06-01 00:00:00 UTC', 0, 2020, 0, 0),
      (6, '2019-01-01 00:00:00 UTC', 0, 2019, 0, 0),
      (7, '2018-05-05 00:00:00 UTC', 0, 2018, 0, 0)
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
      -- Participants 1, 2 and 3 carry the adult-only question this rule used to --
      -- key on. Participants 4 to 7 carry none, which is the pediatric case. --
      (11, 1, '2020-01-01', 0, 0, 0),
      (12, 1, '2020-01-01', 1585482, 0, 0),
      (21, 2, '2020-01-01', 0, 0, 0),
      (22, 2, '2020-01-01', 0, 1585482, 0),
      (31, 3, '2020-03-01', 0, 0, 0),
      (32, 3, '2021-02-28', 1585482, 0, 0),
      (41, 4, '2022-01-01', 0, 0, 0)
""")

SURVEY_CONDUCT_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.survey_conduct`
(survey_conduct_id, person_id, survey_concept_id, survey_end_date, survey_end_datetime,
 survey_source_value, survey_source_concept_id, assisted_concept_id, respondent_type_concept_id,
 timing_concept_id, collection_method_concept_id, validated_survey_concept_id)
VALUES
      -- Adults. Mixed case, so resolution has to be case insensitive. --
      (1, 1, 1585594, '2020-01-01', '2020-01-01 00:00:00 UTC', 'ConsentPII', 1585594, 0, 0, 0, 0, 0),
      (2, 2, 1585594, '2020-01-01', '2020-01-01 00:00:00 UTC', 'ConsentPII', 1585594, 0, 0, 0, 0, 0),
      (3, 3, 1585594, '2021-02-28', '2021-02-28 00:00:00 UTC', 'ConsentPII', 1585594, 0, 0, 0, 0, 0),
      -- Participant 4 consented once and re-consented later. The earliest --
      -- consent is the one that decides the age band. --
      (4, 4, 0, '2022-01-01', '2022-01-01 00:00:00 UTC', 'consentpii_0to6', 0, 0, 0, 0, 0, 0),
      (5, 4, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 'consentpii_0to6', 0, 0, 0, 0, 0, 0),
      -- Participant 5 carries the pediatric export's numeric ids: --
      -- survey_concept_id 0 and an RDR internal survey_source_concept_id that --
      -- collides with an unrelated ICD10CM concept. Keying on either would --
      -- resolve nobody or resolve the wrong thing. --
      (6, 5, 0, '2023-06-01', '2023-06-01 00:00:00 UTC', 'consentpii_0to6', 10263, 0, 0, 0, 0, 0),
      -- Participant 6's only consent row carries the sentinel date. --
      (7, 6, 0, '0001-01-01', '0001-01-01 00:00:00 UTC', 'consentpii_0to6', 0, 0, 0, 0, 0, 0),
      -- Participant 7 answered a survey but never a consent instrument, so no --
      -- consent date exists to take as an age-at-consent date. --
      (8, 7, 0, '2023-01-01', '2023-01-01 00:00:00 UTC', 'ped_basics', 9712, 0, 0, 0, 0, 0)
""")


class FlagParticipantsUnder18YearsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = FlagParticipantsUnder18Years(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        # adding death table name for setup/cleanup operations
        for table_name in [
                VISIT_OCCURRENCE, OBSERVATION, PERSON, SURVEY_CONDUCT
        ]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create the observation, concept, and concept_relationship tables required for the test
        super().setUp()

        person_data_query = PERSON_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        visit_occurrence_data_query = VISIT_OCCURRENCE_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        observation_data_query = OBSERVATION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        survey_conduct_data_query = SURVEY_CONDUCT_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{person_data_query};
                {visit_occurrence_data_query};
                {observation_data_query};
                {survey_conduct_data_query}'''
        ])

    def test_flag_participants_under_18years(self):
        self.default_test([])
        self.assertTableValuesMatch(self.fq_sandbox_table_names[0],
                                    ['person_id', 'age_at_consent', 'age_band'],
                                    [(3, 17, '7-17'), (4, 1, '0-6'),
                                     (5, 3, '0-6')])
        self.assertTableValuesMatch(self.fq_sandbox_table_names[1],
                                    ['person_id'], [(6,), (7,)])
