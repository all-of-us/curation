import unittest
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.manual_cleaning_rules.ppi_drop_duplicate_responses as ppi_drop
import constants.bq_utils as bq_consts
import sandbox


class PPIDropDuplicateResponsesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = "project_id"
        self.dataset_id = "dataset_id"
        self.sandbox_dataset_id = "sandbox_dataset_id"

    def test_get_remove_duplicate_queries(self):
        select_query = dict()
        select_query[cdr_consts.QUERY] = ppi_drop.get_select_statement(
            self.project_id, self.dataset_id)
        select_query[
            cdr_consts.DESTINATION_TABLE] = sandbox.get_sandbox_table_name(
                self.dataset_id, ppi_drop.CLEANING_RULE_NAME)
        select_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        select_query[cdr_consts.DESTINATION_DATASET] = self.sandbox_dataset_id

        delete_query = dict()
        delete_query[cdr_consts.QUERY] = ppi_drop.get_delete_statement(
            project_id=self.project_id, dataset_id=self.dataset_id)
        delete_query[cdr_consts.BATCH] = True
        expected = [select_query, delete_query]

        actual = ppi_drop.get_remove_duplicate_set_of_responses_to_same_questions_queries(
            self.project_id, self.dataset_id, self.sandbox_dataset_id)

        self.assertCountEqual(expected, actual)
