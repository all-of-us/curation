import re
import unittest

import mock

import cdr_cleaner.cleaning_rules.generate_ext_tables as gen_ext
import common
import resources


class GenerateExtTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project_id'
        self.dataset_id = 'dataset_id'
        self.obs_fields = [{
                                "type": "integer",
                                "name": "observation_id",
                                "mode": "nullable",
                                "description": "The observation_id used in the observation table."
                            }, {
                                "type": "string",
                                "name": "src_id",
                                "mode": "nullable",
                                "description": "The provenance of the data associated with the observation_id."
                            }]

    def test_get_obs_fields(self):
        table = common.OBSERVATION
        expected = self.obs_fields
        actual = gen_ext.get_table_fields(table)
        self.assertItemsEqual(expected, actual)

    def test_get_cdm_table_id(self):
        observation_table_id = common.OBSERVATION
        expected = observation_table_id
        mapping_observation = gen_ext.MAPPING_PREFIX + observation_table_id
        actual = gen_ext.get_cdm_table_from_mapping(mapping_observation)
        self.assertItemsEqual(expected, actual)

    def test_site_mapping_list(self):
        hpo_list = resources.hpo_csv()
        mapping_list = gen_ext.generate_site_mappings()
        self.assertEqual(len(mapping_list), len(hpo_list))
