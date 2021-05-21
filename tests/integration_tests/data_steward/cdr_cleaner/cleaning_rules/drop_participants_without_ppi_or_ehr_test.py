# Python imports
import os
from datetime import date, datetime
from unittest.mock import patch

# Third party imports

# Project imports
import common
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_participants_without_ppi_or_ehr import DropParticipantsWithoutPPI
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

CONSENT_CONCEPT_ID = 1586100
BASICS_CONCEPT_ID = 1586134
TYPE_CONCEPT_ID_SURVEY = 45905771


class DropParticipantsWithoutPPITest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')
        cls.rule_instance = DropParticipantsWithoutPPI(cls.project_id,
                                                       cls.dataset_id,
                                                       cls.sandbox_id)

        cls.affected_tables = [
            common.PERSON, common.OBSERVATION, common.DRUG_EXPOSURE
        ]
        supporting_tables = ['_mapping_observation']
        cls.vocab_tables = ['concept', 'concept_ancestor']
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{table}"
            for table in cls.affected_tables + supporting_tables +
            cls.vocab_tables
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table)}'
            for table in cls.affected_tables
        ]

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # create tables
        super().setUp()

        self.copy_vocab_tables()

        # Participant 1: no data (to be removed)
        # Participant 2: has the basics only
        # Participant 3: has EHR data only (to be removed as of DC-706)
        # Participant 4: has both basics and EHR
        # Participant 5: has only RDR consent (to be removed)
        # Participant 6: has EHR observation only (to be removed as of DC-706)
        drug_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.drug_exposure`
                (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date,
                drug_exposure_start_datetime, drug_type_concept_id)
            VALUES
                (200, 3, 10, '2021-01-01', TIMESTAMP('2021-01-01'), 1),
                (201, 4, 11, '2021-01-01', TIMESTAMP('2021-01-01'), 2)
            """)
        observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date,
                observation_type_concept_id)
            VALUES
                (100, 2, {{rdr_basics_concept_id}}, '2021-01-01', {{survey_concept_id}}),
                (101, 2, {{rdr_consent_concept_id}}, '2021-01-01', {{survey_concept_id}}),
                (102, 4, {{rdr_basics_concept_id}}, '2021-01-01', {{survey_concept_id}}),
                (103, 5, {{rdr_consent_concept_id}}, '2021-01-01', {{survey_concept_id}}),
                (104, 6, 12345, '2021-01-01', 23456)
            """)
        mapping_observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_observation`
                (observation_id, src_hpo_id)
            VALUES
                (100, 'rdr'),
                (101, 'rdr'),
                (102, 'rdr'),
                (103, 'rdr'),
                (104, 'fake')
            """)
        person_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.person`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES 
                (1, 2, 2000, 3, 4),
                (2, 3, 2001, 4, 5),
                (3, 4, 2001, 5, 6),
                (4, 5, 2002, 6, 7),
                (5, 6, 2002, 6, 7),
                (6, 7, 2002, 7, 8)
            """)

        drug_query = drug_tmpl.render(project_id=self.project_id,
                                      dataset_id=self.dataset_id)
        observation_query = observation_tmpl.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            rdr_basics_concept_id=BASICS_CONCEPT_ID,
            rdr_consent_concept_id=CONSENT_CONCEPT_ID,
            survey_concept_id=TYPE_CONCEPT_ID_SURVEY)
        mapping_observation_query = mapping_observation_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        person_query = person_tmpl.render(project_id=self.project_id,
                                          dataset_id=self.dataset_id)

        # load the test data
        self.load_test_data([
            drug_query, observation_query, mapping_observation_query,
            person_query
        ])

    def copy_vocab_tables(self):
        for table in self.vocab_tables:
            self.client.copy_table(
                f'{self.project_id}.{self.vocabulary_id}.{table}',
                f'{self.project_id}.{self.dataset_id}.{table}')

    @patch(
        'cdr_cleaner.cleaning_rules.drop_rows_for_missing_persons.TABLES_TO_DELETE_FROM',
        [common.PERSON, common.OBSERVATION, common.DRUG_EXPOSURE])
    def test_queries(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        tables_and_counts = [{
            'name': common.PERSON,
            'fq_table_name': self.fq_table_names[0],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'race_concept_id', 'ethnicity_concept_id'
            ],
            'loaded_ids': [1, 2, 3, 4, 5, 6],
            'cleaned_values': [(2, 3, 2001, 4, 5), (4, 5, 2002, 6, 7)]
        }, {
            'name':
                common.OBSERVATION,
            'fq_table_name':
                self.fq_table_names[1],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'loaded_ids': [100, 101, 102, 103, 104],
            'cleaned_values': [
                (100, 2, BASICS_CONCEPT_ID, date.fromisoformat('2021-01-01'),
                 TYPE_CONCEPT_ID_SURVEY),
                (101, 2, CONSENT_CONCEPT_ID, date.fromisoformat('2021-01-01'),
                 TYPE_CONCEPT_ID_SURVEY),
                (102, 4, BASICS_CONCEPT_ID, date.fromisoformat('2021-01-01'),
                 TYPE_CONCEPT_ID_SURVEY)
            ]
        }, {
            'name':
                common.DRUG_EXPOSURE,
            'fq_table_name':
                self.fq_table_names[2],
            'fields': [
                'drug_exposure_id', 'person_id', 'drug_concept_id',
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_type_concept_id'
            ],
            'loaded_ids': [200, 201],
            'cleaned_values': [
                (201, 4, 11, date.fromisoformat('2021-01-01'),
                 datetime.fromisoformat('2021-01-01 00:00:00+00:00'), 2)
            ]
        }]

        self.default_test(tables_and_counts)
