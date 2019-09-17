import unittest
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.manual_cleaning_rules.ac_69_remove_duplicate_rows_for_select_pids as ac69
import constants.bq_utils as bq_consts


class AcRemoveDuplicateRowsForSelectPidsTest(unittest.TestCase):

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
        query[cdr_consts.QUERY] = ac69.REMOVE_DUPLICATE_TEMPLATE.format(project_id=self.project_id,
                                                                        dataset_id=self.dataset_id)
        query[cdr_consts.DESTINATION_TABLE] = ac69.OBSERVATION_TABLE
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        expected = [query]

        actual = ac69.get_remove_duplicate_queries(self.project_id, self.dataset_id)

        self.assertItemsEqual(expected, actual)
