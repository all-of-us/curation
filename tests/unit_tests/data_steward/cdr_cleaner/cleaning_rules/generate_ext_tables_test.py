"""
Unit test for generate_ext_tables module

Original Issues: DC-1640

The intent of this unit test is to ensure the cleaning rule is generating the extension tables with the proper fields
    and populating each with the correct <table>_id and src_id data from the site_masking table.
"""

# Python imports
import unittest

# Third Party imports
import os
import json
import mock

# Project imports
import common
from resources import fields_path
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.generate_ext_tables as gen_ext

FIELDS_TMPL = common.JINJA_ENV.from_string("""
            {{name}} {{col_type}} {{mode}} OPTIONS(description="{{desc}}")
        """)


class GenerateExtTablesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'foo_dataset'
        self.sandbox_dataset_id = 'foo_sandbox'
        self.mapping_dataset_id = 'foo_mapping_dataset'
        self.client = None
        self.maxDiff = None

        self.hpo_list = [{
            "hpo_id": "hpo_1",
            "name": "hpo_name_1"
        }, {
            "hpo_id": "hpo_2",
            "name": "hpo_name_2"
        }]

        self.fields = [{
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

        self.mapping_tables = [
            gen_ext.MAPPING_PREFIX + cdm_table
            for cdm_table in common.AOU_REQUIRED
            if cdm_table not in
            [common.PERSON, common.DEATH, common.FACT_RELATIONSHIP]
        ]

        self.rule_instance = gen_ext.GenerateExtTables(self.project_id,
                                                       self.dataset_id,
                                                       self.sandbox_dataset_id,
                                                       self.mapping_dataset_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id,
                         self.sandbox_dataset_id)

    def test_get_dynamic_table_fields_str(self):
        """
        Get table fields strings when a schema file is not defined.
        """
        # pre-conditions
        fields_list = []

        for field in self.fields:
            expected_fields = FIELDS_TMPL.render(
                name=field.get('name'),
                col_type=self.rule_instance.get_bq_col_type(field.get('type')),
                mode=self.rule_instance.get_bq_mode(field.get('mode')),
                desc=field.get('description'))
            fields_list.append(expected_fields)

        fields_str = ','.join(fields_list)
        table = 'foo'
        ext_table = f'foo{gen_ext.EXT_TABLE_SUFFIX}'

        # test
        with self.assertLogs(level='INFO') as cm:
            actual = self.rule_instance.get_table_fields_str(table, ext_table)

        # post conditions
        static_msg = 'using dynamic extension table schema for table:'
        self.assertIn(static_msg, cm.output[0])
        self.assertCountEqual(fields_str, actual)

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

        fields_list = []

        for field in expected:
            expected_fields = FIELDS_TMPL.render(
                name=field.get('name'),
                col_type=self.rule_instance.get_bq_col_type(field.get('type')),
                mode=self.rule_instance.get_bq_mode(field.get('mode')),
                desc=field.get('description'))
            fields_list.append(expected_fields)

        fields_str = ','.join(fields_list)

        # test
        with self.assertLogs(level='INFO') as cm:
            actual = self.rule_instance.get_table_fields_str(table, ext_table)

        # post conditions
        static_msg = 'using json schema file definition for table:'
        self.assertIn(static_msg, cm.output[0])
        self.assertCountEqual(fields_str, actual)

    @mock.patch('bq_utils.get_hpo_info')
    def test_get_cdm_table_id(self, mock_hpo_list):
        mock_hpo_list.return_value = self.hpo_list
        # pre-conditions
        observation_table_id = common.OBSERVATION
        expected = observation_table_id
        mapping_observation = f'{gen_ext.MAPPING_PREFIX}{observation_table_id}'

        # test
        actual = self.rule_instance.get_cdm_table_from_mapping(
            mapping_observation)

        # post conditions
        self.assertCountEqual(expected, actual)

    @mock.patch('bq_utils.create_table')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.generate_ext_tables.GenerateExtTables.get_mapping_table_ids'
    )
    def test_get_query_specs(self, mock_mapping_tables, mock_create_table):
        mock_mapping_tables.return_value = self.mapping_tables
        expected = []
        for cdm_table in common.AOU_REQUIRED:
            ext_table_fields_str = self.rule_instance.get_table_fields_str(
                cdm_table, (cdm_table + gen_ext.EXT_TABLE_SUFFIX))
            if cdm_table not in [
                    common.PERSON, common.DEATH, common.FACT_RELATIONSHIP
            ]:
                query = dict()
                query[cdr_consts.QUERY] = gen_ext.REPLACE_SRC_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    ext_table=cdm_table + gen_ext.EXT_TABLE_SUFFIX,
                    ext_table_fields=ext_table_fields_str,
                    cdm_table_id=cdm_table,
                    mapping_dataset_id=self.mapping_dataset_id,
                    mapping_table_id=gen_ext.MAPPING_PREFIX + cdm_table,
                    shared_sandbox_id=self.sandbox_dataset_id,
                    site_maskings_table_id=gen_ext.SITE_TABLE_ID)
                expected.append(query)

        # Test
        actual = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertCountEqual(expected, actual)
