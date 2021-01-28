"""
Unit tests for drop_cope_duplicate_responses module

Removes the duplicate sets of COPE responses to the same questions in the same survey version.
"""
import unittest

from cdr_cleaner.cleaning_rules.drop_cope_duplicate_responses import (
    DropCopeDuplicateResponses, COPE_SURVEY_VERSION_MAP_TABLE, ISSUE_NUMBERS,
    OBSERVATION, SANDBOX_DUPLICATE_COPE_RESPONSES,
    REMOVE_DUPLICATE_COPE_RESPONSES)
from constants.cdr_cleaner import clean_cdr as clean_consts


class DropCopeDuplicateResponsesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_id = 'foo_sandbox'
        self.client = None
        self.sandbox_table = f'{ISSUE_NUMBERS[0].lower()}_{OBSERVATION}'

        self.rule_instance = DropCopeDuplicateResponses(self.project_id,
                                                        self.dataset_id,
                                                        self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        results_list = self.rule_instance.get_query_specs()
        # Post conditions
        sandbox_query = dict()
        sandbox_query[
            clean_consts.QUERY] = SANDBOX_DUPLICATE_COPE_RESPONSES.render(
                project=self.project_id,
                dataset=self.dataset_id,
                sandbox_dataset=self.sandbox_id,
                cope_survey_version_table=COPE_SURVEY_VERSION_MAP_TABLE,
                intermediary_table=self.sandbox_table)

        update_query = dict()
        update_query[
            clean_consts.QUERY] = REMOVE_DUPLICATE_COPE_RESPONSES.render(
                project=self.project_id,
                dataset=self.dataset_id,
                sandbox_dataset=self.sandbox_id,
                intermediary_table=self.sandbox_table)

        expected_list = [sandbox_query, update_query]

        for ex_dict, rs_dict in zip(expected_list, results_list):
            self.assertDictEqual(ex_dict, rs_dict)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        store_duplicate_rows = SANDBOX_DUPLICATE_COPE_RESPONSES.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            cope_survey_version_table=COPE_SURVEY_VERSION_MAP_TABLE,
            intermediary_table=self.sandbox_table)

        delete_duplicate_rows = REMOVE_DUPLICATE_COPE_RESPONSES.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_id,
            intermediary_table=self.sandbox_table)

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + store_duplicate_rows,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + delete_duplicate_rows
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
