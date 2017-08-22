import unittest
import resources


class ResourcesTest(unittest.TestCase):
    def setUp(self):
        super(ResourcesTest, self).setUp()

    def test_cdm_csv(self):
        cdm_data_rows = resources.cdm_csv()
        expected_keys = {'table_name', 'column_name', 'is_nullable', 'data_type', 'description'}
        expected_table_names = {'person', 'visit_occurrence', 'condition_occurrence', 'procedure_occurrence',
                                'drug_exposure', 'measurement', 'death'}
        for row in cdm_data_rows:
            keys = set(row.keys())
            self.assertSetEqual(expected_keys, keys)
            self.assertIn(row['table_name'], expected_table_names)

    def tearDown(self):
        super(ResourcesTest, self).tearDown()
