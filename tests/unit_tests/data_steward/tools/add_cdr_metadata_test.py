import unittest

import mock
import pandas as pd

from tools.add_cdr_metadata import *


class AddCdrMetadataTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.dataset_id = 'dataset_id'
        self.project_id = 'project_id'
        self.fields = [{
            "type": "string",
            "name": "etl_version",
            "mode": "nullable",
            "description": "The version of pipeline used to generate CDR"
        }, {
            "type": "string",
            "name": "ehr_source",
            "mode": "nullable",
            "description": "EHR dataset used to generate cdr"
        }, {
            "type": "date",
            "name": "ehr_cutoff_date",
            "mode": "nullable",
            "description": "The Cutoff date for the ehr_submissions"
        }]
        self.field_values = {
            'etl_version': 'test',
            'ehr_source': 'test',
            'ehr_cutoff_date': '2020-01-01'
        }
        self.update_string_value = "ehr_source = 'test', ehr_cutoff_date = cast('2020-01-01' as DATE)"

    def test_parse_update_statement(self):
        expected_statement = self.update_string_value
        actual_statement = parse_update_statement(self.fields,
                                                  self.field_values)
        self.assertEqual(expected_statement, actual_statement)

    @mock.patch('tools.add_cdr_metadata.parse_update_statement')
    @mock.patch('utils.bq.query')
    @mock.patch('tools.add_cdr_metadata.get_etl_version')
    def test_add_metadata(self, mock_get_etl_version, mock_query,
                          mock_update_statement):
        mock_get_etl_version.return_value = ['test']
        mock_query.return_value = pd.DataFrame(columns=['etl_version'])
        mock_update_statement.return_value = self.update_string_value
        add_metadata(self.dataset_id, self.project_id, self.fields,
                     self.field_values)
        self.assertEqual(mock_query.call_count, 1)

        mock_get_etl_version.return_value = []
        add_metadata(self.dataset_id, self.project_id, self.fields,
                     self.field_values)
        self.assertEqual(mock_query.call_count, 3)

    @mock.patch('bq_utils.create_table')
    @mock.patch('bq_utils.table_exists')
    def test_create_metadata_table(self, mock_table_exists, mock_create_table):
        mock_table_exists.return_value = True
        mock_create_table.return_value = True
        create_metadata_table(self.dataset_id, self.fields)
        self.assertEqual(mock_create_table.call_count, 0)

        mock_table_exists.return_value = False
        create_metadata_table(self.dataset_id, self.fields)
        self.assertEqual(mock_create_table.call_count, 1)

    def test_etl_metadata_query(self):
        expected_query = ADD_ETL_METADATA_QUERY.format(
            project=self.project_id,
            dataset=self.dataset_id,
            metadata_table=METADATA_TABLE,
            etl_version=ETL_VERSION,
            field_value=self.field_values[ETL_VERSION])

        actual_query = f'\ninsert into `{self.project_id}.{self.dataset_id}.{METADATA_TABLE}` ({ETL_VERSION}) values(\'{self.field_values[ETL_VERSION]}\')\n'

        self.assertEquals(expected_query, actual_query)

    def test_copy_query(self):
        expected_query = COPY_QUERY.format(project=self.project_id,
                                           dataset=self.dataset_id,
                                           metadata_table=METADATA_TABLE)

        actual_query = f'\nselect * from `{self.project_id}.{self.dataset_id}.{METADATA_TABLE}`\n'

        self.assertEqual(expected_query, actual_query)
