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
        self.target_dataset = 'foo_dataset'
        self.component = 'copy'

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

        self.correct_parameter_list = [
            '--component', self.component, '--project_id', self.project_id,
            '--target_dataset', self.target_dataset, '--source_dataset',
            self.dataset_id, '--etl_version', None, '--ehr_cutoff_date', None,
            '--rdr_source', None, '--rdr_export_date', None,
            '--cdr_generation_date', None, '--qa_handoff_date', None,
            '--vocabulary_version', None, '--ehr_source', None
        ]

        # Required fields removed
        self.incorrect_parameter_list_1 = [
            '--project_id', self.project_id, '--target_dataset',
            self.target_dataset, '--source_dataset_id', self.dataset_id
        ]
        self.incorrect_parameter_list_2 = [
            '--component', self.component, '--target_dataset',
            self.target_dataset, '--source_dataset', self.dataset_id
        ]

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
        expected_query = ADD_ETL_METADATA_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            metadata_table=METADATA_TABLE,
            etl_version=ETL_VERSION,
            field_value=self.field_values[ETL_VERSION])

        actual_query = f'\ninsert into `{self.project_id}.{self.dataset_id}.{METADATA_TABLE}` ({ETL_VERSION}) values(\'{self.field_values[ETL_VERSION]}\')'

        self.assertEqual(expected_query, actual_query)

    def test_copy_query(self):
        expected_query = COPY_QUERY.render(project=self.project_id,
                                           dataset=self.dataset_id,
                                           metadata_table=METADATA_TABLE)

        actual_query = f'\nselect * from `{self.project_id}.{self.dataset_id}.{METADATA_TABLE}`'

        self.assertEqual(expected_query, actual_query)

    def test_parse_cdr_metadata_args(self):
        # Tests if incorrect parameters are given
        self.assertRaises(SystemExit, parse_cdr_metadata_args,
                          self.incorrect_parameter_list_1)
        self.assertRaises(SystemExit, parse_cdr_metadata_args,
                          self.incorrect_parameter_list_2)

        # Tests if incorrect choice for component are given
        incorrect_component_choice_args = [[
            '--component', 'delete', '--project_id', self.project_id,
            '--target_dataset', self.target_dataset, '--source_dataset',
            self.dataset_id
        ],
                                           [
                                               '--component', 'update',
                                               '--project_id', self.project_id,
                                               '--target_dataset',
                                               self.target_dataset,
                                               '--source_dataset',
                                               self.dataset_id
                                           ]]

        for args in incorrect_component_choice_args:
            self.assertRaises(SystemExit, parse_cdr_metadata_args, args)

        # Preconditions
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {
            k.strip('-'): v for (k, v) in correct_parameter_dict.items()
        }

        # Test if correct parameters are given
        args, kwargs = parse_cdr_metadata_args(self.correct_parameter_list)
        results_dict = vars(args)

        # Post conditions
        self.assertEqual(correct_parameter_dict, results_dict)
