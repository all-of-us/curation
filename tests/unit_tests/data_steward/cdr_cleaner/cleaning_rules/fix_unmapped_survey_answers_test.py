# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.fix_unmapped_survey_answers import (
    FixUnmappedSurveyAnswers, SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY,
    UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY, OBSERVATION, JIRA_ISSUE_NUMBERS)
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import PERSON


class FixUnmappedSurveyAnswersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.sandbox_table_name = '_'.join(
            JIRA_ISSUE_NUMBERS).lower() + '_' + OBSERVATION
        self.client = None

        self.rule_instance = FixUnmappedSurveyAnswers(self.project_id,
                                                      self.dataset_id,
                                                      self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

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
            cdr_consts.QUERY:
                SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_id,
                    sandbox_table=self.sandbox_table_name,
                    dataset=self.dataset_id)
        }

        update_query_dict = {
            cdr_consts.QUERY:
                UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.rule_instance.project_id,
                    sandbox_dataset=self.sandbox_id,
                    sandbox_table=self.sandbox_table_name,
                    dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        self.assertEqual(results_list, [sandbox_query_dict, update_query_dict])
