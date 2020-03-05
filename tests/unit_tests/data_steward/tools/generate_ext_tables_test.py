import unittest

import mock

import bq_utils
import common
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import tools.generate_ext_tables as gen_ext


class GenerateExtTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.obs_fields = [{
            "type": "integer",
            "name": "observation_id",
            "mode": "nullable",
            "description": "The observation_id used in the observation table."
        }, {
            "type":
                "string",
            "name":
                "src_id",
            "mode":
                "nullable",
            "description":
                "The provenance of the data associated with the observation_id."
        }]
        self.hpo_list = [{
            "hpo_id": "hpo_1",
            "name": "hpo_name_1"
        }, {
            "hpo_id": "hpo_2",
            "name": "hpo_name_2"
        }]
        self.mapping_tables = [
            gen_ext.MAPPING_PREFIX + cdm_table
            for cdm_table in common.AOU_REQUIRED
            if cdm_table not in
            [common.PERSON, common.DEATH, common.FACT_RELATIONSHIP]
        ]
        self.bq_string = '("{hpo_name}", "EHR site nnn")'

    def test_get_obs_fields(self):
        # pre-conditions
        table = common.OBSERVATION
        expected = self.obs_fields

        # test
        actual = gen_ext.get_table_fields(table)

        # post conditions
        self.assertCountEqual(expected, actual)

    @mock.patch('bq_utils.get_hpo_info')
    def test_get_cdm_table_id(self, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        # pre-conditions
        observation_table_id = common.OBSERVATION
        expected = observation_table_id
        mapping_observation = gen_ext.MAPPING_PREFIX + observation_table_id

        # test
        actual = gen_ext.get_cdm_table_from_mapping(mapping_observation)

        # post conditions
        self.assertCountEqual(expected, actual)

    @mock.patch('bq_utils.get_hpo_info')
    def test_site_mapping_dict(self, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        mapping_dict = gen_ext.generate_site_mappings()
        self.assertEqual(len(mapping_dict), len(self.hpo_list))
        for hpo_id in mapping_dict:
            self.assertGreaterEqual(mapping_dict[hpo_id], 100)
            self.assertLessEqual(mapping_dict[hpo_id], 999)

    @mock.patch('bq_utils.get_hpo_info')
    def test_get_hpo_and_rdr_mappings(self, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        expected = [hpo_dict["hpo_id"] for hpo_dict in self.hpo_list
                   ] + [gen_ext.RDR]
        hpo_rdr_mapping_list = gen_ext.get_hpo_and_rdr_mappings()
        self.assertEqual(len(hpo_rdr_mapping_list), len(expected))
        for hpo_rdr_id in expected:
            self.assertIn(hpo_rdr_id,
                          [hpo_item[0] for hpo_item in hpo_rdr_mapping_list])
        for hpo_item in hpo_rdr_mapping_list:
            self.assertIn(hpo_item[0], expected)
            if hpo_item[0] == gen_ext.RDR:
                self.assertEqual(hpo_item[1], gen_ext.PPI_PM)

    @mock.patch('bq_utils.get_hpo_info')
    def test_convert_to_bq_string(self, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        hpo_rdr_mapping_list = gen_ext.get_hpo_and_rdr_mappings()
        hpo_bq_list = []
        for hpo in bq_utils.get_hpo_info():
            hpo_bq_list.append(self.bq_string.format(hpo_name=hpo["hpo_id"]))
        hpo_bq_list.append('("{rdr}", "{ppi_pm}")'.format(
            rdr=gen_ext.RDR, ppi_pm=gen_ext.PPI_PM))
        expected = ', '.join(hpo_bq_list)
        actual = gen_ext.convert_to_bq_string(hpo_rdr_mapping_list)
        self.assertEqual(len(actual), len(expected))

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('bq_utils.create_table')
    @mock.patch(
        'tools.generate_ext_tables.create_and_populate_source_mapping_table')
    @mock.patch('tools.generate_ext_tables.get_mapping_table_ids')
    def test_generate_ext_table_queries(self, mock_mapping_tables,
                                        mock_create_and_populate_mapping_table,
                                        mock_create_table, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        mock_mapping_tables.return_value = self.mapping_tables
        expected = []
        for cdm_table in common.AOU_REQUIRED:
            if cdm_table not in [
                    common.PERSON, common.DEATH, common.FACT_RELATIONSHIP
            ]:
                query = dict()
                query[cdr_consts.QUERY] = gen_ext.REPLACE_SRC_QUERY.format(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    combined_dataset_id=self.dataset_id,
                    mapping_table_id=gen_ext.MAPPING_PREFIX + cdm_table,
                    site_mappings_table_id=gen_ext.SITE_TABLE_ID,
                    cdm_table_id=cdm_table)
                query[cdr_consts.
                      DESTINATION_TABLE] = cdm_table + gen_ext.EXT_TABLE_SUFFIX
                query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
                query[cdr_consts.DISPOSITION] = bq_consts.WRITE_EMPTY
                expected.append(query)
        actual = gen_ext.get_generate_ext_table_queries(self.project_id,
                                                        self.dataset_id,
                                                        self.dataset_id)
        self.assertCountEqual(expected, actual)
