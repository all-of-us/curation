"""
Unit test for run_deid module

test_parse_args -- ensures the output dataset name includes _deid
test_main -- ensures the parameter list contains the output dataset command line argument
test_known_tables -- ensures all table names known to curation are returned
test_get_output_table_schemas -- ensures only table schemas for suppressed tables are copied

Original Issue: DC-744
"""
# Python imports
import os
import unittest

# Third party imports
from mock import patch
import mock
from resources import DEID_PATH

# Project imports
from tools import run_deid


class RunDeidTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.input_dataset = 'foo_input'
        self.private_key = 'fake/SA/file/path.json'
        self.output_dataset = 'foo_output_deid'
        self.action = 'debug'
        self.skip_tables = 'foo_table'
        self.tablename = 'bar_table'
        self.max_age = '89'
        self.run_as_email = 'test@test.com'

        self.correct_parameter_list = [
            '--idataset', self.input_dataset, '--private_key', self.private_key,
            '--odataset', self.output_dataset, '--action', self.action,
            '--skip-tables', self.skip_tables, '--tables', self.tablename,
            '--age_limit', self.max_age, '--run_as', self.run_as_email
        ]

        self.incorrect_parameter_list = [
            '--idataset',
            self.input_dataset,
            '--private_key',
            self.private_key,
            '--action',
            self.action,
            '--skip-tables',
            self.skip_tables,
            '--tables',
            self.tablename,
        ]

    def tearDown(self):
        pass

    def test_parse_args(self):
        # Tests if incorrect parameters are given
        self.incorrect_parameter_list.extend(['--odataset', 'random_deid_tag'])
        self.assertRaises(SystemExit, run_deid.parse_args,
                          self.incorrect_parameter_list)

        # Preconditions
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {
            k.strip('-').replace('-', '_'): v
            for (k, v) in correct_parameter_dict.items()
        }

        # setting correct_parameter_dict values not set in setUp function
        correct_parameter_dict['console_log'] = False
        correct_parameter_dict['interactive_mode'] = False
        correct_parameter_dict['input_dataset'] = self.input_dataset
        correct_parameter_dict['run_as_email'] = correct_parameter_dict.pop(
            'run_as')

        # need to delete idataset argument from correct_parameter_dict because input_dataset argument is returned
        # when self.correct_parameter_list is supplied to parse_args
        if 'idataset' in correct_parameter_dict:
            del correct_parameter_dict['idataset']

        # Tests if correct parameters are given
        results_dict = vars(run_deid.parse_args(self.correct_parameter_list))

        # Post conditions
        self.assertEqual(correct_parameter_dict, results_dict)

    @patch('tools.run_deid.copy_ext_tables')
    @patch('tools.run_deid.BigQueryClient')
    @patch('tools.run_deid.fields_for')
    @patch('tools.run_deid.copy_suppressed_table_schemas')
    @patch('deid.aou.main')
    @patch('tools.run_deid.copy_deid_map_table')
    @patch('tools.run_deid.load_deid_map_table')
    @patch('tools.run_deid.get_output_tables')
    def test_main(self, mock_tables, mock_load, mock_copy, mock_main,
                  mock_suppressed, mock_fields, mock_bq_client,
                  mock_copy_ext_tables):
        # Tests if incorrect parameters are given
        self.assertRaises(SystemExit, run_deid.main,
                          self.incorrect_parameter_list)

        # Preconditions
        mock_tables.return_value = ['fake1']
        mock_fields.return_value = {}

        # Tests if correct parameters are given
        run_deid.main(self.correct_parameter_list)

        # Post conditions
        mock_main.assert_called_once_with([
            '--rules',
            os.path.join(DEID_PATH, 'config', 'ids', 'config.json'),
            '--private_key', self.private_key, '--table', 'fake1', '--action',
            self.action, '--idataset', self.input_dataset, '--log', 'LOGS',
            '--odataset', self.output_dataset, '--age-limit', self.max_age,
            '--run_as', self.run_as_email
        ])
        self.assertEqual(mock_main.call_count, 1)
        self.assertEqual(mock_copy_ext_tables.call_count, 1)

    @patch('tools.run_deid.os.walk')
    def test_known_tables(self, mock_walk):
        # preconditions
        mock_walk.return_value = [
            ('', '', ['fake/file/table.json', 'table2.json', 'odd_name.csv'])
        ]

        # test
        result = run_deid.get_known_tables('fake_field_path')

        # post conditions
        expected = ['fake/file/table', 'table2', 'odd_name.csv']
        self.assertEqual(result, expected)

    @patch('tools.run_deid.BigQueryClient')
    def test_get_output_tables(self, mock_bq_client):
        # pre-conditions
        input_dataset = 'fake_input_dataset'
        known_tables = [
            'table', 'observation', 'odd_name.csv', '_map_table', 'skip_table'
        ]
        skip_tables = 'odd_name.csv,madeup,skip_table'
        only_tables = 'observation,table_zed'

        table_ids = [
            '_map_table',
            'pii_fake',
            'note',
            'observation',
            'table_zed',
            'table',
            'skip_table',
        ]

        mock_table_object = mock.MagicMock()
        type(mock_table_object).table_id = mock.PropertyMock(
            side_effect=table_ids)
        tables = [mock_table_object] * 7
        mock_bq_client.list_tables.return_value = tables

        # test
        result = run_deid.get_output_tables(mock_bq_client, input_dataset,
                                            known_tables, skip_tables,
                                            only_tables)

        # post condition
        expected = ['observation']

        self.assertEqual(result, expected)

    @patch('tools.run_deid.fields_for')
    @patch('tools.run_deid.bq_utils')
    def test_copy_suppressed_table_schemas(self, mock_bq_utils, mock_fields):
        # pre-conditions
        known_tables = ['note', 'camper', 'observation']
        dest_dataset = 'foo'
        fields_list = [{"name": "foo_id", "mode": "nullable", "type": "string"}]
        mock_fields.return_value = fields_list

        # test
        run_deid.copy_suppressed_table_schemas(known_tables, dest_dataset)

        # post conditions
        self.assertEqual(
            mock_bq_utils.create_table.assert_called_once_with(
                'note',
                fields_list,
                drop_existing=True,
                dataset_id=dest_dataset), None)
        self.assertEqual(mock_bq_utils.create_table.call_count, 1)
