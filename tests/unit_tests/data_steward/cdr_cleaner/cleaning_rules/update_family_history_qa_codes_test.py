import unittest

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import update_family_history_qa_codes as family_history


class UpdateFamilyHistory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.dataset_id = 'dataset_id'
        self.project_id = 'project_id'

    def test_get_update_family_history_qa_queries(self):
        actual_dict = family_history.get_update_family_history_qa_queries(
            self.project_id, self.dataset_id)
        actual = actual_dict[0][cdr_consts.QUERY]
        expected = family_history.UPDATE_FAMILY_HISTORY_QUERY.format(
            project_id=self.project_id, dataset_id=self.dataset_id)

        self.assertEqual(expected, actual)
