import unittest

import cdr_cleaner.cleaning_rules.clean_mapping as cm
import common


class CleanMappingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'

    def test_get_cdm_table(self):
        cdm_tables = set(common.CDM_TABLES)
        mapping_tables = [cm.MAPPING_PREFIX + table for table in cdm_tables]
        ext_tables = [table + cm.EXT_SUFFIX for table in cdm_tables]
        for table in mapping_tables:
            cdm_table = cm.get_cdm_table(table, cm.MAPPING)
            self.assertIn(cdm_table, cdm_tables)

        for table in ext_tables:
            cdm_table = cm.get_cdm_table(table, cm.EXT)
            self.assertIn(cdm_table, cdm_tables)
