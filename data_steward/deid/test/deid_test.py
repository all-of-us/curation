"""
This unit test requires a valid configuration at `deid/test_config.json` and the environment variables below.
I_DATASET: Input dataset_id.
O_DATASET: Output dataset_id. Note that tests currently assume that this has been populated.

TODO encapsulate deid in a single function for testability
"""

import os
import unittest
import json
from google.cloud import bigquery as bq

DEID_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
DEID_PATH = os.path.abspath(os.path.join(DEID_TEST_PATH, '..'))
DATA_STEWARD_PATH = os.path.abspath(os.path.join(DEID_PATH, '..'))
DEFAULT_CONFIG_PATH = os.path.join(DEID_PATH, 'test_config.json')
FIELDS_PATH = os.path.join(DATA_STEWARD_PATH, 'resources', 'fields')
CDM_TABLE_NAMES = ['person', 'observation', 'measurement', 'condition_occurrence', 'device_exposure',
                   'procedure_occurrence', 'death', 'drug_exposure', 'visit_occurrence']

# TODO Error on deid of location and care_site "KeyError: 'dropfields'" (DC-142)
ALL_CDM_TABLE_NAMES = CDM_TABLE_NAMES + ['location', 'care_site']


class DeidTest(unittest.TestCase):

    def get_config(self, config_path=DEFAULT_CONFIG_PATH):
        with open(config_path, 'r') as fp:
            return json.load(fp)

    def get_schema(self, table):
        schema_path = os.path.join(FIELDS_PATH, table + '.json')
        with open(schema_path, 'r') as fp:
            return json.load(fp)

    def setUp(self):
        # TODO Deid some minimal dataset prior to tests
        super(DeidTest, self).setUp()
        self.maxDiff = 5000  # for comparing large objects
        config = self.get_config()
        service_account_path = config['constants']['service-account-path']
        self.i_dataset = os.getenv('I_DATASET')
        self.o_dataset = os.getenv('O_DATASET')
        self.client = bq.Client.from_service_account_json(service_account_path)

    def test_output_all_cdm_tables(self):
        # Check all CDM tables in output
        # TODO Output of deid should be all tables (even if empty due to suppression)
        o_dataset_ref = self.client.dataset(self.o_dataset)
        expected_tables = set(ALL_CDM_TABLE_NAMES)
        actual_tables = set([actual_table.table_id for actual_table in self.client.list_tables(o_dataset_ref)])
        # Note: not currently checking for extra tables
        missing_tables = list(expected_tables - actual_tables)
        if len(missing_tables) > 0:
            self.fail('Output dataset is missing the following tables: %s' % missing_tables)

    def test_output_schema(self):
        # Output of deid should use standard schema (DC-120)
        o_dataset_ref = self.client.dataset(self.o_dataset)
        for table_name in CDM_TABLE_NAMES:
            table_ref = o_dataset_ref.table(table_name)
            table = self.client.get_table(table_ref)

            # Table fields should have names and types from standard schema
            # Note: We do not check nullability as deid may entail relaxing constraint
            actual_items = [(item.name, item.field_type.lower()) for item in table.schema]
            actual_schema = dict(actual_items)
            expected_items = [(item['name'], item['type'].lower()) for item in self.get_schema(table_name)]
            expected_schema = dict(expected_items)
            self.assertDictEqual(expected_schema, actual_schema)

    def test_output_clustering(self):
        # TODO Output of deid should be clustered by person_id (DC-134)
        o_dataset_ref = self.client.dataset(self.o_dataset)
        tables_missing_cluster = []
        for table_name in CDM_TABLE_NAMES:
            expected_items = [(item['name'], item['type'].lower()) for item in self.get_schema(table_name)]
            expected_schema = dict(expected_items)
            if 'person_id' in expected_schema.keys():
                table_ref = o_dataset_ref.table(table_name)
                table = self.client.get_table(table_ref)
                clustering = table._properties.get('clustering', dict(fields=[]))
                if ['person_id'] != clustering['fields']:
                    tables_missing_cluster.append(table_name)
        if len(tables_missing_cluster) > 0:
            self.fail('The following output tables are missing cluster on person_id: %s' % tables_missing_cluster)

    def test_output_column_descriptions(self):
        # TODO Output of deid should have column descriptions (DC-135)
        o_dataset_ref = self.client.dataset(self.o_dataset)
        for table_name in CDM_TABLE_NAMES:
            table_ref = o_dataset_ref.table(table_name)
            table = self.client.get_table(table_ref)

            # Check table field descriptions
            actual_items = [(item.name, item.description) for item in table.schema]
            actual_schema = dict(actual_items)
            expected_items = [(item['name'], item.get('description', None)) for item in self.get_schema(table_name)]
            expected_schema = dict(expected_items)
            self.assertDictEqual(expected_schema, actual_schema)
