import unittest
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.manual_cleaning_rules.ppi_drop_duplicate_responses as ppi_drop


class PPIDropDuplicateResponsesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = "project_id"
        self.dataset_id = "dataset_id"

    def test_get_remove_duplicate_queries(self):
        query = dict()
        query[cdr_consts.QUERY] = ppi_drop.REMOVE_DUPLICATE_TEMPLATE.format(project_id=self.project_id,
                                                                            dataset_id=self.dataset_id)
        query[cdr_consts.BATCH] = True
        expected = [query]

        actual = ppi_drop.get_remove_duplicate_set_of_responses_to_same_questions_queries(self.project_id,
                                                                                          self.dataset_id)

        self.assertItemsEqual(expected, actual)
