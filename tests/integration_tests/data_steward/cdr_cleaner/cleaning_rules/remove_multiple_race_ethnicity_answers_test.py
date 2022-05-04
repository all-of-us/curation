"""Integration test for RemoveMultipleRaceEthnicityAnswersQueries
"""
# Python imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_multiple_race_ethnicity_answers import RemoveMultipleRaceEthnicityAnswersQueries
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RemoveMultipleRaceEthnicityAnswersQueriesTest(
        BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = RemoveMultipleRaceEthnicityAnswersQueries(
            project_id, dataset_id, sandbox_id)
