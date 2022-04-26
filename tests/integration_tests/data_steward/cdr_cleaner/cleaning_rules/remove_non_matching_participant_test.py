"""Integration test
"""

# Python Imports
import os

# Third party imports
from google.cloud.bigquery import Table, TimePartitioning, TimePartitioningType

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import RemoveNonMatchingParticipant
from common import JINJA_ENV, IDENTITY_MATCH, LOCATION, OBSERVATION, PARTICIPANT_MATCH, PERSON, PII_ADDRESS
from gcloud.bq import BigQueryClient
from validation.participants.create_update_drc_id_match_table import (
    create_drc_validation_table, populate_validation_table)
from tests import test_util
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
import resources

# HPO_1 and HPO_2 have both participant_match and identity_match.
# HPO_3 has only identity_match.
# HPO_4 has only participant_match.
HPO_1, HPO_2, HPO_3, HPO_4 = 'nyc', 'pitt', 'chs', 'vcu'

POPULATE_STATEMENTS = {
    PERSON:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
        VALUES
        (1, 0, 1991, 0, 0),
        (2, 0, 1992, 0, 0),
        (3, 0, 1993, 0, 0),
        (4, 0, 1994, 0, 0),
        (5, 0, 1995, 0, 0)
        """),
    OBSERVATION:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
        VALUES
        (11, 1, 0, date('2022-01-01'), 0),
        (12, 2, 0, date('2022-01-02'), 0),
        (13, 3, 0, date('2022-01-03'), 0),
        (14, 4, 0, date('2022-01-04'), 0),
        (15, 5, 0, date('2022-01-05'), 0)
        """),
    PII_ADDRESS:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}`
        (person_id, location_id)
        VALUES
        (1, 101),
        (2, 102),
        (3, 103),
        (4, 104),
        (5, 105)
        """),
    LOCATION:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        (location_id, address_1, address_2, city, state, zip)
        VALUES
        (101, 'xyz', '', 'New York', 'NY', '12345'),
        (102, 'xyz', '', 'New York', 'NY', '12345'),
        (103, 'xyz', '', 'New York', 'NY', '12345'),
        (104, 'xyz', '', 'New York', 'NY', '12345'),
        (105, 'xyz', '', 'New York', 'NY', '12345')
        """),
    PARTICIPANT_MATCH:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        (person_id, algorithm_validation, manual_validation)
        VALUES
        (1, 'yes', 'yes'),
        (2, 'yes', 'yes'),
        (3, 'yes', 'yes'),
        (4, 'yes', 'yes'),
        (5, 'yes', 'yes')
        """),
    IDENTITY_MATCH:
        JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}` 
        (person_id, first_name, middle_name, last_name, phone_number, email, address_1, address_2, city, state, zip, birth_date, sex, algorithm)
        VALUES
        (1, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (2, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (3, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (4, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes'),
        (5, 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'match', 'yes')
        """)
}


class RemoveNonMatchingParticipantTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        ehr_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.ehr_dataset_id = ehr_dataset_id
        validation_dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.validation_dataset_id = validation_dataset_id

        cls.rule_instance = RemoveNonMatchingParticipant(
            project_id, dataset_id, sandbox_id, ehr_dataset_id,
            validation_dataset_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        # TODO 20220427 Move this section to setUpClass and make multiple test cases

        # TODO Delete the following command before finalizing
        test_util.delete_all_tables(self.dataset_id)

        self.client = BigQueryClient(self.project_id)

        self.create_idendity_match_tables([HPO_1, HPO_2, HPO_3])
        self.create_participant_match_tables([HPO_1, HPO_2, HPO_4])

        for hpo_id in [HPO_1, HPO_2, HPO_3, HPO_4]:

            for _schema in [PERSON, OBSERVATION, PII_ADDRESS, LOCATION]:
                schema = resources.fields_for(_schema)
                table = Table(
                    f'{self.project_id}.{self.dataset_id}.{hpo_id}_{_schema}',
                    schema=schema)
                table.time_partitioning = TimePartitioning(
                    type_=TimePartitioningType.HOUR)
                table = self.client.create_table(table, exists_ok=True)

                populate_query = POPULATE_STATEMENTS[_schema].render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table_id=f'{hpo_id}_{_schema}')
                job = self.client.query(populate_query)
                job.result()

    def create_idendity_match_tables(self, hpo_ids):
        """_summary_
        """
        self.id_match_table_ids = [
            f'{hpo_id}_{IDENTITY_MATCH}' for hpo_id in hpo_ids
        ]

        for id_match_table_id in self.id_match_table_ids:
            create_drc_validation_table(self.client, id_match_table_id,
                                        self.dataset_id)

            populate_query = POPULATE_STATEMENTS[IDENTITY_MATCH].render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=id_match_table_id)
            job = self.client.query(populate_query)
            job.result()

    def create_participant_match_tables(self, hpo_ids):
        """_summary_
        """
        self.participant_match_table_ids = [
            f'{hpo_id}_{PARTICIPANT_MATCH}' for hpo_id in hpo_ids
        ]

        schema = resources.fields_for(PARTICIPANT_MATCH)
        for participant_match_table_id in self.participant_match_table_ids:
            table = Table(
                f'{self.project_id}.{self.dataset_id}.{participant_match_table_id}',
                schema=schema)
            table = self.client.create_table(table, exists_ok=True)

            populate_query = POPULATE_STATEMENTS[PARTICIPANT_MATCH].render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=participant_match_table_id)
            job = self.client.query(populate_query)
            job.result()

    def test_dummy(self):
        self.assertEqual(1, 1)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
