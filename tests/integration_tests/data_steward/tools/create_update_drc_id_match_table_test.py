"""
Integration test for the create_update_drc_id_match_table module.

Ensures that a partitioned (by hour) drc validation table is created for the site, the values from the ps_api_values
    table for that site are copied into the drc validation table, and that any null fields are populated with either
    'missing_rdr' if the entire record contains no information or 'missing_ehr' if there is missing data for a
    particular field in the record.

Original Issue: DC-1216

The intent of this module is to check that the drc validation table is created properly. The values are copied over
    from the ps_api_values table to the drc validation table. The drc validation table is updated with the proper data
    as necessary
"""

# Python imports
import os
import mock
from unittest import TestCase

# Third party imports
from google.cloud.bigquery import DatasetReference, SchemaField

# Project imports
import bq_utils
from utils import bq
from common import JINJA_ENV, DRC_OPS
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
            SchemaField("person_id", "STRING"),
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
    def test_get_set_expression(self, mock_table_schema):
        # Pre conditions
        mock_table_schema.return_value = self.schema

        expected = f'\nfirst_name = \'missing_rdr\', \n'\
                   f'last_name = \'missing_rdr\''

        # Test
        actual = id_validation.get_set_expression()

        self.assertEqual(actual, expected)

    @mock.patch('resources.fields_for')
    def test_create_drc_validation_table(self, mock_fields_for):
        # Preconditions
        mock_fields_for.return_value = self.fields

        # Test
        expected = id_validation.create_drc_validation_table(
            self.client, self.project_id, self.id_match_table_id)

        all_tables_obj = self.client.list_tables(self.dataset_id)
        all_tables = [t.table_id for t in all_tables_obj]

        self.assertTrue(expected in all_tables)

    @mock.patch('resources.fields_for')
    @mock.patch('utils.bq.get_table_schema')
    def test_copy_ps_values_data_to_id_match_table(self, mock_table_schema,
                                                   mock_fields_for):
        # Pre conditions
        mock_table_schema.return_value = self.schema
        mock_fields_for.return_value = self.fields

        expected = [{
            'person_id': 1,
            'first_name': 'fee',
            'last_name': 'faa'
        }, {
            'person_id': 2,
            'first_name': None,
            'last_name': 'foe'
        }, {
            'person_id': 3,
            'first_name': 'fum',
            'last_name': None
        }, {
            'person_id': 4,
            'first_name': None,
            'last_name': None
        }]

        # Test
        id_validation.create_drc_validation_table(self.client, self.project_id,
                                                  self.id_match_table_id)

        bq_utils.create_table(self.ps_values_table_id,
                              self.fields,
                              drop_existing=True,
                              dataset_id=DRC_OPS)

        insert_query = POPULATE_PS_VALUES.render(
            project_id=self.project_id,
            drc_dataset_id=DRC_OPS,
            ps_values_table_id=self.ps_values_table_id)
        self.client.query(insert_query)

        id_validation.copy_ps_values_data_to_id_match_table(
            self.client, self.project_id, self.id_match_table_id, self.hpo_id)

        query_contents = CONTENT_QUERY.render(project_id=self.project_id,
                                              drc_dataset_id=self.dataset_id,
                                              id_match_table_id=self.id_match_table_id)

        content_job = self.client.query(query_contents)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]

        self.assertCountEqual(actual, expected)

    @mock.patch('utils.bq.get_table_schema')
    def test_update_site_drc_table(self, mock_table_schema):
        # Preconditions
        mock_table_schema.return_value = self.schema

        expected = [{
            'person_id': 1,
            'first_name': 'fee',
            'last_name': 'faa'
        }, {
            'person_id': 2,
            'first_name': 'missing_ehr',
            'last_name': 'foe'
        }, {
            'person_id': 3,
            'first_name': 'fum',
            'last_name': 'missing_ehr'
        }, {
            'person_id': 4,
            'first_name': 'missing_rdr',
            'last_name': 'missing_rdr'
        }]

        # Test

        id_validation.update_site_drc_table(self.client, self.project_id, self.id_match_table_id)

        query_contents = CONTENT_QUERY.render(project_id=self.project_id,
                                              drc_dataset_id=self.dataset_id,
                                              id_match_table_id=self.id_match_table_id)

        content_job = self.client.query(query_contents)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]

        self.assertCountEqual(actual, expected)
