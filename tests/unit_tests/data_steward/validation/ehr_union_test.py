import unittest
from unittest import mock

import bq_utils
from validation import ehr_union


class EhrUnionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    @mock.patch('bq_utils.list_all_table_ids')
    def test_mapping_subqueries_person(self, mock_list_all_table_ids):
        hpo_ids = ['fake_site_1', 'fake_site_2']
        mock_list_all_table_ids.return_value = [
            bq_utils.get_table_id(hpo_id, 'person') for hpo_id in hpo_ids
        ]
        hpo_ids = ['fake_site_1', 'fake_site_2']
        actual = ehr_union._mapping_subqueries('person', hpo_ids,
                                               'fake_dataset', 'fake_project')
        self.assertEqual(len(hpo_ids), len(actual))
        for i in range(0, len(hpo_ids)):
            hpo_id = hpo_ids[i]
            subquery = actual[i]
            hpo_table = bq_utils.get_table_id(hpo_id, 'person')
            self.assertTrue(f"'{hpo_table}' AS src_table_id" in subquery)
            self.assertTrue('person_id AS src_person_id' in subquery)
            # src_person_id and person_id fields both use participant ID value
            # (offset is NOT added to the value)
            self.assertTrue('person_id AS person_id' in subquery)

    def tearDown(self):
        pass
