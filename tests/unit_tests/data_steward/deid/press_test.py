# Python imports
import unittest

# Third party imports
from mock import patch, mock

# Project imports
from deid.press import Press


class BasePass(Press):

    def get_dataframe(self, sql=None, limit=None):
        pass

    def submit(self, sql, create, dml=None):
        pass

    def update_rules(self):
        pass


class PressTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # set up mocks for initialization
        self.mock_bq_client_patcher = patch('deid.press.BigQueryClient')
        self.mock_bq_client = self.mock_bq_client_patcher.start()
        self.addCleanup(self.mock_bq_client_patcher.stop)

        mock_logs = patch('deid.press.set_up_logging')
        mock_logs.start()
        self.addCleanup(mock_logs.stop)

        mock_open = patch('deid.press.codecs')
        self.mock_open_file = mock_open.start()
        self.mock_open_file.side_effect = [[], OSError]
        self.addCleanup(mock_open.stop)

        mock_json = patch('deid.press.json.loads')
        self.mock_read_json = mock_json.start()
        self.mock_read_json.side_effect = [[], []]
        self.addCleanup(mock_json.stop)

        # input parameters expected by the class
        self.input_dataset = 'foo_input'
        self.tablename = 'bar_table'
        self.log_path = 'deid-fake.log'
        self.rules_path = 'fake/config/path.json'
        self.pipeline = None
        self.action = 'submit'

        self.press_obj = BasePass(idataset=self.input_dataset,
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
                'apply': 'select count(*) from foo_input.bar_table',
                'name': 'bogus_count_statement',
                'label': 'getting a count'
                # leave dml_statement out to make sure it defaults to False
            },
            {
                'apply': 'select * from foo_input.bar_table',
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
