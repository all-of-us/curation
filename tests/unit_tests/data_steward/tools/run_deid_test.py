"""
Unit test for run_deid module

test_parse_args -- ensures the output dataset name includes _deid
test_main -- ensures the parameter list contains the output dataset command line argument
test_known_tables -- ensures all table names known to curation are returned
test_get_output_table_schemas -- ensures only table schemas for suppressed tables are copied

Original Issue: DC-744
"""

# Python imports
import unittest
import os
from argparse import Namespace, ArgumentTypeError

# Third party imports
from mock import mock, patch

# Project imports
from tools import run_deid
from resources import DEID_PATH
from tools.run_deid import parse_args, main


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

        self.correct_parameter_list = [
            '--idataset', self.input_dataset, '--private_key', self.private_key,
            '--odataset', self.output_dataset, '--action', self.action,
            '--skip-tables', self.skip_tables, '--tables', self.tablename
        ]

        self.incorrect_parameter_list = [
            '--idataset',
            self.input_dataset,
            '--private_key',
            self.private_key,
            '--action',
            self.action,
            '--skip_tables',
            self.skip_tables,
            '--tables',
            self.tablename,
        ]

    def tearDown(self):
        pass

    def test_parse_args(self):
        # Tests if incorrect parameters are given
        self.assertRaises(SystemExit, parse_args, self.incorrect_parameter_list)

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

        # need to delete idataset argument from correct_parameter_dict because input_dataset argument is returned
        # when self.correct_parameter_list is supplied to parse_args
        if 'idataset' in correct_parameter_dict:
            del correct_parameter_dict['idataset']

        # Tests if correct parameters are given
        results_dict = vars(parse_args(self.correct_parameter_list))

        # Post conditions
        self.assertEqual(correct_parameter_dict, results_dict)

    @patch('tools.run_deid.fields_for')
    @patch('tools.run_deid.copy_suppressed_table_schemas')
    @patch('deid.aou.main')
    @patch('tools.run_deid.get_output_tables')
    def test_main(self, mock_tables, mock_main, mock_suppressed, mock_fields):
        # Tests if incorrect parameters are given
        self.assertRaises(SystemExit, main, self.incorrect_parameter_list)

        # Preconditions
        mock_tables.return_value = ['fake1']
        mock_fields.return_value = {}

        # Tests if correct parameters are given
        results_dict = main(self.correct_parameter_list)

        # Post conditions
        self.assertEqual(results_dict, None)

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

    @patch('tools.run_deid.bq_utils.list_dataset_contents')
    def test_get_output_tables(self, mock_contents):
        # pre-conditions
        input_dataset = 'fake_input_dataset'
        known_tables = [
            'table', 'observation', 'odd_name.csv', '_map_table', 'skip_table'
        ]
        skip_tables = 'odd_name.csv,madeup,skip_table'
        only_tables = 'observation,table_zed'

        mock_contents.return_value = [
            '_map_table',
            'pii_fake',
            'note',
            'observation',
            'table_zed',
            'table',
            'skip_table',
        ]

        # test
        result = run_deid.get_output_tables(input_dataset, known_tables,
                                            skip_tables, only_tables)

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
