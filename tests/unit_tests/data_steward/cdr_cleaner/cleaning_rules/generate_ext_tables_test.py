"""
Unit test for generate_ext_tables module

Original Issues: DC-1640

The intent of this unit test is to ensure the cleaning rule is generating the extension tables with the proper fields
    and populating each with the correct <table>_id and src_id data from the site_masking table.
"""

# Python imports
import json
import os
import unittest

# Third Party imports
from google.cloud import bigquery
import mock

# Project imports
import common
from resources import fields_for, fields_path
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

        # Excluding DEATH, and FACT_RELATIONSHIP beacuse they do not
        # have mapping tables in the combined dataset.
        mapped_table_names = [
            cdm_table for cdm_table in common.CATI_TABLES
            if cdm_table not in [common.DEATH, common.FACT_RELATIONSHIP]
        ]

        mapping_table_names = [
            f"{common.MAPPING_PREFIX}{cdm_table}"
            for cdm_table in mapped_table_names
        ]

        dataset_ref = bigquery.DatasetReference(self.project_id,
                                                self.dataset_id)

        self.mapping_table_objs = [
            bigquery.TableReference(dataset_ref, table_name)
            for table_name in mapping_table_names + mapped_table_names
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
        ext_table = f'{table}{common.EXT_SUFFIX}'

        # test
        with self.assertLogs(level='INFO') as cm:
            actual_str, _ = self.rule_instance.get_table_fields_str(
                table, ext_table)

        # post conditions
        static_msg = 'using dynamic extension table schema for table:'
        self.assertIn(static_msg, cm.output[0])
        self.assertCountEqual(fields_str, actual_str)

    def test_get_schema_defined_table_fields(self):
        """
        Get table fields when a schema file is defined.
        """
        # pre-conditions
        table = common.SURVEY_CONDUCT
        ext_table = f"{table}{common.EXT_SUFFIX}"
        table_path = os.path.join(fields_path, 'extension_tables',
                                  f"{ext_table}.json")
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
            actual_str, _ = self.rule_instance.get_table_fields_str(
                table, ext_table)

        # post conditions
        static_msg = 'using json schema file definition for table:'
        self.assertIn(static_msg, cm.output[0])
        self.assertCountEqual(fields_str, actual_str)

    def test_get_query_specs(self):
        mock_client = mock.Mock()
        mock_client.list_tables.return_value = self.mapping_table_objs
        expected = []
        for cdm_table in common.CATI_TABLES:
            ext_table_fields_str, _ = self.rule_instance.get_table_fields_str(
                cdm_table, f"{cdm_table}{common.EXT_SUFFIX}")

            additional_fields = _get_field_names(
                f'{cdm_table}{common.EXT_SUFFIX}')
            additional_fields = [
                f for f in additional_fields
                if f not in ['src_id', f'{cdm_table}_id']
            ]

            mapping_fields = _get_field_names(
                f'{common.MAPPING_PREFIX}{cdm_table}')

            if cdm_table not in [common.DEATH, common.FACT_RELATIONSHIP]:
                query = dict()
                query[cdr_consts.QUERY] = gen_ext.REPLACE_SRC_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    ext_table=f"{cdm_table}{common.EXT_SUFFIX}",
                    ext_table_fields=ext_table_fields_str,
                    cdm_table_id=cdm_table,
                    additional_fields=additional_fields,
                    mapping_fields=mapping_fields,
                    mapping_dataset_id=self.mapping_dataset_id,
                    mapping_table_id=f"{common.MAPPING_PREFIX}{cdm_table}",
                    shared_sandbox_id=self.sandbox_dataset_id,
                    site_maskings_table_id=common.SITE_MASKING_TABLE_ID)
                expected.append(query)

        # Test
        self.rule_instance.setup_rule(mock_client)
        actual = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertCountEqual(expected, actual)


def _get_field_names(tablename):
    try:
        fields = fields_for(tablename)
    except RuntimeError:
        additional_fields = []
    else:
        additional_fields = [f.get('name') for f in fields]

    return additional_fields
