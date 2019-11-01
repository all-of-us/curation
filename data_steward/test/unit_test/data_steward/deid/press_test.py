# Python imports
import unittest

# Third party imports
from mock import Mock, patch

# Project imports
from deid.press import Press 


class PressTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    @patch('deid.press.json.loads')
    @patch('deid.press.set_up_logging')
    @patch('deid.press.codecs.open')
    def setUp(self, mock_open, mock_logging, mock_file):
        # set up mocks for initialization
        mock_logging = Mock()
        mock_open = Mock()
        mock_open.side_effect = [[], StandardError]
        mock_file = Mock()
        mock_file.side_effect = [[], []]

        # input parameters expected by the class
        self.input_dataset = 'foo_input'
        self.tablename = 'bar_table'
        self.log_path = 'deid-fake.log'
        self.rules_path = 'fake/config/path.json'
        self.pipeline = None
        self.action = 'submit'

        self.press_obj = Press(
            idataset=self.input_dataset,
            table=self.tablename,
            logs=self.log_path,
            rules=self.rules_path,
            pipeline=self.pipeline,
            action=self.action)

    def test_gather_dml_queries(self):
        # pre-conditions
        table_path = self.input_dataset + self.tablename
        info = [
            {
                # This is the only statement that should be returned
                'apply': 'delete * from ' + table_path,
                'name': 'bogus_delete_dml',
                'label': 'dropping table contents',
                'dml_statement': True
            },
            {
                'apply': 'select count(*) from ' + table_path,
                'name': 'bogus_count_statement',
                'label': 'getting a count'
                # leave dml_statement out to make sure it defaults to False
            },
            {
                'apply': 'select * from ' + table_path,
                # leave name out to make sure it doesn't break
                'label': 'select all contents',
                'dml_statement': False
            }
        ]

        # test
        result = self.press_obj.gather_dml_queries(info)

        # post conditions
        expected = ['delete * from ' + table_path]
        self.assertEqual(result, expected)
