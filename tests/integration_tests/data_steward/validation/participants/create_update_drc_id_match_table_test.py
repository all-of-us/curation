"""
Integration test for the create_update_drc_id_match_table module.

Ensures that a partitioned (by hour) drc validation table is created for the site properly and that the created table is
    populated with 'missing_rdr' if there does not exist data for that field in the `ps_values` table or the default
    value 'missing_ehr'

Original Issue: DC-1216

The intent of this module is to check that the drc validation table is created properly and the drc validation table is
 updated properly.
"""

# Python imports
import os
import mock
from unittest import TestCase

# Third party imports
from google.cloud.bigquery import DatasetReference, SchemaField, Table, TimePartitioning, TimePartitioningType

# Project imports
import bq_utils
from utils import bq
from tests import test_util
from app_identity import PROJECT_ID
from common import JINJA_ENV, PS_API_VALUES
from validation.participants import create_update_drc_id_match_table as id_validation
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE

POPULATE_PS_VALUES = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{drc_dataset_id}}.{{ps_values_table_id}}` 
(person_id, first_name, last_name)
VALUES 
    (1, 'fee', 'faa'),
    (2, null, 'foe'),
    (3, 'fum', null),
    (4, null, null)
""")

CONTENT_QUERY = JINJA_ENV.from_string("""
SELECT *
FROM {{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}
""")


class CreateUpdateDrcIdMatchTableTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = os.environ.get(PROJECT_ID)
        self.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.dataset_ref = DatasetReference(self.project_id, self.dataset_id)
        self.client = bq.get_client(self.project_id)

        self.schema = [
            SchemaField("person_id", "INT64"),
            SchemaField("first_name", "STRING"),
            SchemaField("last_name", "STRING"),
            SchemaField("algorithm", "STRING")
        ]

        self.ps_api_fields = [
            dict(name='person_id', type='integer', mode='nullable'),
            dict(name='first_name', type='string', mode='nullable'),
            dict(name='last_name', type='string', mode='nullable')
        ]

        self.id_match_fields = [
            dict(name='person_id', type='integer', mode='nullable'),
            dict(name='first_name', type='string', mode='nullable'),
            dict(name='last_name', type='string', mode='nullable'),
            dict(name='algorithm', type='string', mode='nullable')
        ]

        self.hpo_id = 'fake_site'
        self.id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{self.hpo_id}'
        self.ps_values_table_id = f'ps_api_values_{self.hpo_id}'

        # Create and populate the ps_values site table

        schema = bq.get_table_schema(PS_API_VALUES)
        tablename = self.ps_values_table_id

        table = Table(f'{self.project_id}.{self.dataset_id}.{tablename}',
                      schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.HOUR)
        table = self.client.create_table(table)

        populate_query = POPULATE_PS_VALUES.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            ps_values_table_id=self.ps_values_table_id)
        job = self.client.query(populate_query)
        job.result()

    @mock.patch('utils.bq.get_table_schema')
    def test_get_case_statements(self, mock_table_schema):
        # Pre conditions
        mock_table_schema.return_value = self.schema

        expected = f'\nCASE WHEN first_name IS NULL THEN \'missing_rdr\' ELSE \'missing_ehr\' END AS first_name, \n' \
                   f'CASE WHEN last_name IS NULL THEN \'missing_rdr\' ELSE \'missing_ehr\' END AS last_name'

        # Test
        actual = id_validation.get_case_statements()

        self.assertEqual(actual, expected)

    @mock.patch('resources.fields_for')
    def test_create_drc_validation_table(self, mock_fields_for):
        # Preconditions
        mock_fields_for.return_value = self.id_match_fields

        # Test
        expected = id_validation.create_drc_validation_table(
            self.client,
            self.project_id,
            self.id_match_table_id,
            drc_dataset_id=self.dataset_id)

        all_tables_obj = self.client.list_tables(self.dataset_id)
        all_tables = [t.table_id for t in all_tables_obj]

        self.assertTrue(expected in all_tables)

    @mock.patch('resources.fields_for')
    @mock.patch('utils.bq.get_table_schema')
    def test_validation_creation_and_population(self, mock_table_schema,
                                                mock_fields_for):
        # Preconditions
        mock_table_schema.return_value = self.schema
        mock_fields_for.return_value = self.id_match_fields

        expected = [{
            'person_id': 1,
            'first_name': 'missing_ehr',
            'last_name': 'missing_ehr',
            'algorithm': 'no'
        }, {
            'person_id': 2,
            'first_name': 'missing_rdr',
            'last_name': 'missing_ehr',
            'algorithm': 'no'
        }, {
            'person_id': 3,
            'first_name': 'missing_ehr',
            'last_name': 'missing_rdr',
            'algorithm': 'no'
        }, {
            'person_id': 4,
            'first_name': 'missing_rdr',
            'last_name': 'missing_rdr',
            'algorithm': 'no'
        }]

        # Creates validation table if it does not already exist
        # Will need to be created if this test is ran individually
        if not bq_utils.table_exists(self.id_match_table_id, self.dataset_id):
            id_validation.create_drc_validation_table(
                self.client,
                self.project_id,
                self.id_match_table_id,
                drc_dataset_id=self.dataset_id)

        # Test validation table population
        id_validation.populate_validation_table(self.client,
                                                self.project_id,
                                                self.id_match_table_id,
                                                self.hpo_id,
                                                drc_dataset_id=self.dataset_id)

        query_contents = CONTENT_QUERY.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            id_match_table_id=self.id_match_table_id)

        content_job = self.client.query(query_contents)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]

        self.assertCountEqual(actual, expected)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
