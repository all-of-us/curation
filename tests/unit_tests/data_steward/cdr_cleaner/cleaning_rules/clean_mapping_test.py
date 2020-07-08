import unittest

import mock
import pandas as pd

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
        self.sandbox_dataset_id = 'sandbox_dataset_id'

        self.query_class = cm.CleanMappingExtTables(self.project_id,
                                                    self.dataset_id,
                                                    self.sandbox_dataset_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id,
                         self.sandbox_dataset_id)
        self.client = None

    @mock.patch(
        'cdr_cleaner.cleaning_rules.clean_mapping.CleanMappingExtTables.get_tables'
    )
    def test_setup_rule(self, mock_tables):
        # pre conditions
        mock_tables.side_effect = [['_mapping_drug'], ['measurement_ext']]

        # test
        self.query_class.setup_rule(self.client)

        # post conditions
        self.assertEqual(['_mapping_drug'], self.query_class.mapping_tables)
        self.assertEqual(['measurement_ext'], self.query_class.ext_tables)

    def test_get_cdm_table(self):
        cdm_tables = set(common.CDM_TABLES)
        mapping_tables = [cm.MAPPING_PREFIX + table for table in cdm_tables]
        ext_tables = [table + cm.EXT_SUFFIX for table in cdm_tables]
        for table in mapping_tables:
            cdm_table = self.query_class.get_cdm_table(table, cm.MAPPING)
            self.assertIn(cdm_table, cdm_tables)

        for table in ext_tables:
            cdm_table = self.query_class.get_cdm_table(table, cm.EXT)
            self.assertIn(cdm_table, cdm_tables)

    @mock.patch('utils.bq.query')
    def test_get_tables(self, mock_tables):
        tables = [
            '_mapping_observation', '_site_mappings', 'measurement_ext',
            '_mapping_drug', 'extension_observation'
        ]
        mock_tables.return_value = pd.DataFrame({cm.TABLE_NAME: tables})
        expected = ['_mapping_observation']
        actual = self.query_class.get_tables(cm.MAPPING)
        self.assertEqual(expected, actual)

        expected = ['measurement_ext']
        actual = self.query_class.get_tables(cm.EXT)
        self.assertEqual(expected, actual)

    def test_get_clean_mapping_queries(self):
        cdm_tables = [
            common.OBSERVATION, common.MEASUREMENT, common.NOTE,
            common.PROCEDURE_OCCURRENCE, common.CONDITION_OCCURRENCE
        ]
        self.query_class.mapping_tables = [
            cm.MAPPING_PREFIX + cdm_table for cdm_table in cdm_tables
        ]
        self.query_class.ext_tables = [
            cdm_table + cm.EXT_SUFFIX for cdm_table in cdm_tables
        ]
        actual = self.query_class.get_query_specs()
        self.assertEqual(len(actual), len(cdm_tables) * 4)
