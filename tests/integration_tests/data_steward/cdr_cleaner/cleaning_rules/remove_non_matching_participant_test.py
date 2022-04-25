"""Integration test
"""

# Python Imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import RemoveNonMatchingParticipant
from common import JINJA_ENV, IDENTITY_MATCH, PARTICIPANT_MATCH
from validation.participants.create_update_drc_id_match_table import (
    create_drc_validation_table, populate_validation_table)
from tests import test_util
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
import resources

HPO_1, HPO_2, HPO_3, HPO_4 = 'nyc', 'pitt', 'chs', 'vcu'


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

        table_names = [
            # nyc has both participant_match and identity_match
            'nyc_person',
            'nyc_observation',
            'nyc_location',
            'nyc_participant_match',
            'nyc_identity_match',
            # pitt has both participant_match
            'pitt_person',
            'pitt_observation',
            'pitt_location',
            'pitt_participant_match',
            # chs has both identity_match
            'chs_person',
            'chs_observation',
            'chs_location',
            'chs_identity_match',
            # vcu has no match tables
            'vcu_person',
            'vcu_observation',
            'vcu_location',
        ]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{table_name}'
            for table_name in table_names
        ]

        # TODO This needs to come after tables created with person_id
        # sandbox for person and observation will be created.
        # location will not have sandbox because it does not have person_id.

        # TODO start from adding test data
        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.id_match_table_ids = [
            f'{hpo_id}_{IDENTITY_MATCH}' for hpo_id in [HPO_1, HPO_2, HPO_3]
        ]

        for id_match_table_id in self.id_match_table_ids:
            create_drc_validation_table(self.client, id_match_table_id,
                                        self.dataset_id)

        self.participant_match_table_ids = [
            f'{hpo_id}_{PARTICIPANT_MATCH}' for hpo_id in [HPO_1, HPO_2, HPO_4]
        ]

        schema = resources.fields_for(IDENTITY_MATCH)
        for participant_match_table_id in self.participant_match_table_ids:
            table = Table(
                f'{self.project_id}.{self.dataset_id}.{participant_match_table_id}',
                schema=schema)
            table = self.client.create_table(table, exists_ok=True)

        super().setUp()

    def test_dummy(self):
        self.assertEqual(1, 1)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
