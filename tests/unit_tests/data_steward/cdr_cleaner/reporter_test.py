import logging
import mock
import unittest
from cdr_cleaner import reporter
import constants.cdr_cleaner.reporter as consts


class CleanRulesReporterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):

        self.query_dict = {
            #            cdr_consts.QUERY: 'query',
            #            cdr_consts.DESTINATION_DATASET: 'foo',
            #            cdr_consts.DESTINATION_TABLE: 'bar',
        }

    def test_check_field_list_validity(self):
        # preconditions
        fields = [k for k in consts.FIELDS_PROPERTIES_MAP.keys()]
        list_dictionary = [{'name': 'foo', 'module': 'bar'}]

        # test
        actual = reporter.check_field_list_validity(fields, list_dictionary)

        # post conditions
        expected = fields + ['name', 'module']
        self.assertEqual(set(actual), set(expected))

    def test_separate_sql_statements(self):
        # preconditions
        statement_dicts = [{
            'name':
                'foo',
            'sql': [{
                'query': 'query one'
            }, {
                'query': 'query two  '
            }, {
                'query': '  query three'
            }]
        }, {
            'name': 'bar'
        }]

        # test
        actual = reporter.separate_sql_statements(statement_dicts)

        # post conditions
        expected = [{
            'name': 'foo',
            'sql': 'query one'
        }, {
            'name': 'foo',
            'sql': 'query two'
        }, {
            'name': 'foo',
            'sql': 'query three'
        }, {
            'name': 'bar',
        }]
        self.assertEqual(actual, expected)

    def test_format_values_with_sql_field(self):
        # preconditions
        report_dicts = [{'sql': [{'query': 'query one'}]}]

        # test
        with self.assertLogs('', level='INFO') as cm:
            actual = reporter.format_values(report_dicts)

        expected_logs = []
        log_record = cm.records[0]
        self.assertEqual(log_record.levelno, logging.INFO)
        self.assertEqual(log_record.msg, 'SQL field exists')

        expected = [{'sql': 'query one'}]
        self.assertEqual(actual, expected)

    def test_format_values_raises_errors(self):
        # preconditions
        statement_dicts = [{'name': 'foo', 'jira-issues': [0, 1]}]

        # test
        with self.assertLogs('', level='INFO') as cm:
            self.assertRaises(TypeError, reporter.format_values,
                              statement_dicts)

        expected_logs = []
        log_record = cm.records[0]
        self.assertEqual(log_record.levelno, logging.ERROR)
        self.assertEqual(
            log_record.msg,
            'erroneous field is jira-issues\nerroneous value is [0, 1]')

    def test_format_values(self):
        # preconditions
        statement_dicts = [{'name': 'foo', 'jira-issues': ['DC-000', 'DC-0']}]

        # test
        actual = reporter.format_values(statement_dicts)

        # post conditions
        expected = [{'name': 'foo', 'jira-issues': 'DC-000, DC-0'}]
        self.assertEqual(actual, expected)

    def test_parse_args(self):
        # preconditions
        arg_list = [
            '--data-stage', 'rdr', 'ehr', '--fields', 'name', 'sandbox-tables',
            'description', '--output-file', 'temp.csv'
        ]

        # test
        elements = reporter.parse_args(arg_list)

        #post conditions
        self.assertEqual(elements.data_stage, ['rdr', 'ehr'])
        self.assertEqual(elements.fields,
                         ['name', 'sandbox-tables', 'description'])
        self.assertEqual(elements.output_filepath, 'temp.csv')
        self.assertFalse(elements.console_log)

    def test_main(self):
        pass

    def test_write_csv_report(self):
        pass

    def test_get_stage_elements(self):
        pass
