# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.remove_multiple_race_ethnicity_answers import (
    RemoveMultipleRaceEthnicityAnswersQueries,
    SANDBOX_ADDITIONAL_RESPONSES_OTHER_THAN_NOT,
    REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT, OBSERVATION, JIRA_ISSUE_NUMBERS)
from common import PERSON
from constants.cdr_cleaner import clean_cdr as clean_consts


class RemoveMultipleRaceEthnicityAnswersQueriesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.sandbox_table_name = JIRA_ISSUE_NUMBERS[0].lower(
        ) + '_' + OBSERVATION
        self.client = None

        self.rule_instance = RemoveMultipleRaceEthnicityAnswersQueries(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_sandbox_table_for(self):
        self.assertEqual(self.rule_instance.get_sandbox_tablenames()[0],
                         self.sandbox_table_name)

    def test_get_sandbox_tablenames(self):
        self.assertListEqual(self.rule_instance.get_sandbox_tablenames(),
                             [self.sandbox_table_name])

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        results_list = self.rule_instance.get_query_specs()

        sandbox_query_dict = {
            clean_consts.QUERY:
                SANDBOX_ADDITIONAL_RESPONSES_OTHER_THAN_NOT.render(
                    dataset=self.dataset_id,
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_id,
                    sandbox_table=self.sandbox_table_name)
        }

        delete_query_dict = {
            clean_consts.QUERY:
                REMOVE_ADDITIONAL_RESPONSES_OTHER_THAN_NOT.render(
                    project=self.project_id, dataset=self.dataset_id)
        }

        self.assertEqual(results_list, [sandbox_query_dict, delete_query_dict])
