"""Integration test
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_non_matching_participant import RemoveNonMatchingParticipant
from common import JINJA_ENV
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


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
            'nyc_person',
            'nyc_observation',
            'nyc_location',
            'pitt_person',
            'pitt_observation',
            'pitt_location',
            'chs_person',
            'chs_observation',
            'chs_location',
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

        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()
