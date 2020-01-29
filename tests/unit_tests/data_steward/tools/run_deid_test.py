import unittest

# Third party imports
from mock import mock, patch

from tools import run_deid


class RunDeidTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def tearDown(self):
        pass

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
