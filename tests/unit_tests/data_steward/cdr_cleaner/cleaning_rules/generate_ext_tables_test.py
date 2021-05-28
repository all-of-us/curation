import json
import os
import unittest

import mock

import common
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.generate_ext_tables as gen_ext
from resources import fields_path


class GenerateExtTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.sandbox_id = 'sandbox_dataset'
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

    def test_get_dynamic_table_fields(self):
        """
        Get table fields when a schema file is not defined.
        """
        # pre-conditions
        expected_fields = [{
            "type": "integer",
            "name": "foo_id",
            "mode": "nullable",
            "description": "The foo_id used in the foo table."
        }, {
            "type":
                "string",
            "name":
                "src_id",
            "mode":
                "nullable",
            "description":
                "The provenance of the data associated with the foo_id."
        }]
        table = 'foo'
        ext_table = f'foo{gen_ext.EXT_TABLE_SUFFIX}'

        # test
        with self.assertLogs(level='INFO') as cm:
            actual = gen_ext.get_table_fields(table, ext_table)

        # post conditions
        static_msg = 'using dynamic extension table schema for table:'
        self.assertIn(static_msg, cm.output[0])
        self.assertCountEqual(expected_fields, actual)

    def test_get_schema_defined_table_fields(self):
        """
        Get table fields when a schema file is defined.
        """
        # pre-conditions
        table = common.OBSERVATION
        ext_table = common.OBSERVATION + gen_ext.EXT_TABLE_SUFFIX
        table_path = os.path.join(fields_path, 'extension_tables',
                                  ext_table + '.json')
        with open(table_path, 'r') as schema:
            expected = json.load(schema)

        # test
        with self.assertLogs(level='INFO') as cm:
            actual = gen_ext.get_table_fields(table, ext_table)

        # post conditions
        static_msg = 'using json schema file definition for table:'
        self.assertIn(static_msg, cm.output[0])
        self.assertCountEqual(expected, actual)

    @mock.patch('bq_utils.get_hpo_info')
    def test_get_cdm_table_id(self, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        # pre-conditions
        observation_table_id = common.OBSERVATION
        expected = observation_table_id
        mapping_observation = f'{gen_ext.MAPPING_PREFIX}{observation_table_id}'

        # test
        actual = gen_ext.get_cdm_table_from_mapping(mapping_observation)

        # post conditions
        self.assertCountEqual(expected, actual)

    @mock.patch('bq_utils.create_table')
    @mock.patch('tools.generate_ext_tables.get_mapping_table_ids')
    def test_generate_ext_table_queries(self, mock_mapping_tables,
                                        mock_create_table):
        mock_mapping_tables.return_value = self.mapping_tables
        expected = []
        for cdm_table in common.AOU_REQUIRED:
            if cdm_table not in [
                    common.PERSON, common.DEATH, common.FACT_RELATIONSHIP
            ]:
                query = dict()
                query[cdr_consts.QUERY] = gen_ext.REPLACE_SRC_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    mapping_dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_id,
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
                                                        self.sandbox_id,
                                                        self.dataset_id)
        self.assertCountEqual(expected, actual)
