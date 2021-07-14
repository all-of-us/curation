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
from google.cloud.bigquery import DatasetReference, SchemaField

# Project imports
from utils import bq
from common import JINJA_ENV
from app_identity import PROJECT_ID
from tools import create_update_drc_id_match_table as id_validation

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
        self.dataset_id = 'drc_ops'
        self.dataset_ref = DatasetReference(self.project_id, self.dataset_id)
        self.client = bq.get_client(self.project_id)

        self.schema = [
            SchemaField("person_id", "INT64"),
            SchemaField("first_name", "STRING"),
            SchemaField("last_name", "STRING"),
        ]

        self.fields = [
            dict(name='person_id', type='integer', mode='nullable'),
            dict(name='first_name', type='string', mode='nullable'),
            dict(name='last_name', type='string', mode='nullable')
        ]

        self.hpo_id = 'fake_site'
        self.id_match_table_id = f'drc_identity_match_{self.hpo_id}'
        self.ps_values_table_id = f'ps_api_values_{self.hpo_id}'

    @mock.patch('utils.bq.get_table_schema')
    def test_get_case_statements(self, mock_table_schema):
        # Pre conditions
        mock_table_schema.return_value = self.schema

        expected = f'\nCASE WHEN first_name IS NULL THEN \'missing_rdr\' ELSE \'missing_ehr\' END AS first_name, , \n' \
                   f'CASE WHEN last_name IS NULL THEN \'missing_rdr\' ELSE \'missing_ehr\' END AS last_name'

        # Test
        actual = id_validation.get_case_statements()

        self.assertEqual(actual, expected)

    @mock.patch('resources.fields_for')
    @mock.patch('utils.bq.get_table_schema')
    def test_validation_creation_and_population(self, mock_table_schema, mock_fields_for):
        # Preconditions
        mock_table_schema.return_value = self.schema
        mock_fields_for.return_value = self.fields

        expected = [{
            'person_id': 1,
            'first_name': 'missing_ehr',
            'last_name': 'missing_ehr'
        }, {
            'person_id': 2,
            'first_name': 'missing_rdr',
            'last_name': 'missing_ehr'
        }, {
            'person_id': 3,
            'first_name': 'missing_ehr',
            'last_name': 'missing_rdr'
        }, {
            'person_id': 4,
            'first_name': 'missing_rdr',
            'last_name': 'missing_rdr'
        }]

        # Tests that validation table created properly
        validation_table = id_validation.create_drc_validation_table(self.client, self.project_id,
                                                                     self.id_match_table_id)

        all_tables_obj = self.client.list_tables(self.dataset_id)
        all_tables = [t.table_id for t in all_tables_obj]

        self.assertTrue(validation_table in all_tables)

        # Test validation table population
        id_validation.populate_validation_table(self.client, self.project_id,
                                                self.id_match_table_id, self.hpo_id)

        query_contents = CONTENT_QUERY.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            id_match_table_id=self.id_match_table_id)

        content_job = self.client.query(query_contents)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]

        self.assertCountEqual(actual, expected)
