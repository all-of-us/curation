import unittest

import app_identity
import bq_utils
import tools.generate_ext_tables as gen_ext


class GenerateExtTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.bq_project_id = app_identity.get_application_id()
        self.bq_dataset_id = bq_utils.get_unioned_dataset_id()

    def test_create_populate_source_mapping_table(self):
        # pre-conditions
        mapping_list = gen_ext.get_hpo_and_rdr_mappings()
        expected = str(len(mapping_list))

        # test
        num_rows_affected = gen_ext.create_and_populate_source_mapping_table(
            self.bq_project_id, self.bq_dataset_id)

        # post condition
        self.assertEqual(expected, num_rows_affected)

    def tearDown(self):
        bq_utils.delete_table(gen_ext.SITE_TABLE_ID, self.bq_dataset_id)
