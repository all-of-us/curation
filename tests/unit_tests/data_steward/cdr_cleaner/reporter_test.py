import csv
import logging
import mock
import os
import unittest

from cdr_cleaner import reporter
import cdr_cleaner.clean_cdr as control
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.cdr_cleaner.reporter as consts


def mock_function_obj(self):
    """
    A mock description.
    """
    pass


class CleanRulesReporterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

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
        fields_list = ['sql']

        # test
        with self.assertLogs('cdr_cleaner.reporter', level='DEBUG') as cm:
            actual = reporter.format_values(report_dicts, fields_list)

        expected_logs = []
        log_record = cm.records[0]
        self.assertEqual(log_record.levelno, logging.DEBUG)
        self.assertEqual(log_record.msg, 'SQL field exists')

        expected = [{'sql': 'query one'}]
        self.assertEqual(actual, expected)

    def test_format_values_raises_errors(self):
        # preconditions
        report_dicts = [{'name': 'foo', 'jira-issues': [0, 1]}]
        fields_list = ['name', 'jira-issues']

        # test
        with self.assertLogs('', level='INFO') as cm:
            self.assertRaises(TypeError, reporter.format_values, report_dicts,
                              fields_list)

        expected_logs = []
        log_record = cm.records[0]
        self.assertEqual(log_record.levelno, logging.ERROR)
        self.assertEqual(
            log_record.msg,
            'erroneous field is jira-issues\nerroneous value is [0, 1]')

    def test_format_values(self):
        # preconditions
        report_dicts = [{'name': 'foo', 'jira-issues': ['DC-000', 'DC-0']}]
        fields_list = ['name', 'jira-issues']

        # test
        actual = reporter.format_values(report_dicts, fields_list)

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

    def test_get_function_info(self):
        # preconditions
        fields_list = ['name', 'module', 'description', 'sql', 'depends-on']

        # test
        actual = reporter.get_function_info(mock_function_obj, fields_list)

        # post condition
        expected = {
            'name': mock_function_obj.__name__,
            'module': mock_function_obj.__module__,
            'description': mock_function_obj.__doc__,
            'sql': 'unknown',
            'depends-on': 'unknown',
        }

        self.assertEqual(actual, expected)

    def test_get_stage_elements(self):
        """
        Makes sure the lists are all readable.
        """
        # preconditions
        fields_list = ['name', 'module', 'sql', 'jira-issues', 'description']

        # test
        for data_stage in control.DATA_STAGE_RULES_MAPPING.keys():
            actual = reporter.get_stage_elements(data_stage, fields_list)

            # post conditions
            for class_info in actual:
                keys = list(class_info.keys())
                self.assertEqual(keys, fields_list)

    def test_write_csv_report_to_non_csv_file(self):
        self.assertRaises(RuntimeError, reporter.write_csv_report, 'foo.bar',
                          ['rdr'], ['description'])

    @mock.patch('cdr_cleaner.reporter.format_values')
    @mock.patch('cdr_cleaner.reporter.get_stage_elements')
    def test_write_csv_report(self, mock_elements, mock_format):
        # preconditions
        fields_list = ['description']
        stage_list = ['rdr']
        output_file = 'foo.csv'
        list_of_dicts = [{'description': 'mock desc'}]
        mock_elements.return_value = list_of_dicts
        mock_format.return_value = list_of_dicts
        open_mock = mock.mock_open()

        # test
        with mock.patch("cdr_cleaner.reporter.open", open_mock, create=True):
            reporter.write_csv_report(output_file, stage_list, fields_list)

        # post conditions
        mock_elements.assert_called_once_with(stage_list[0], fields_list)

        mock_format.assert_called_once_with(list_of_dicts, fields_list)

        open_mock.assert_called_once_with(output_file, 'w', newline='')
        mock_writer = open_mock()
        mock_writer.write.assert_any_call('"description"\n')
        mock_writer.write.assert_any_call('"mock desc"\n')

    @mock.patch('cdr_cleaner.reporter.engine.add_console_logging')
    @mock.patch('cdr_cleaner.reporter.write_csv_report')
    def test_main(self, mock_writer, mock_logger):
        # preconditions
        args_list = [
            '--data-stage', 'rdr', 'ehr', '--fields', 'name', 'sandbox-tables',
            'description', '--output-file', 'temp.csv'
        ]

        # test
        reporter.main(args_list)

        # post conditions
        mock_writer.assert_called_once_with(
            'temp.csv', ['rdr', 'ehr'],
            ['name', 'sandbox-tables', 'description'])
        mock_logger.assert_called_once_with(False)

    @mock.patch('cdr_cleaner.reporter.engine.add_console_logging')
    @mock.patch('cdr_cleaner.reporter.write_csv_report')
    def test_main_unspecified(self, mock_writer, mock_logger):
        # preconditions
        args_list = [
            '--data-stage', 'unspecified', '--fields', 'name', 'sandbox-tables',
            'description', '--output-file', 'temp.csv', '-c'
        ]

        data_stages = [
            s.value
            for s in cdr_consts.DataStage
            if s is not cdr_consts.DataStage.UNSPECIFIED
        ]

        # test
        reporter.main(args_list)

        # post conditions
        mock_writer.assert_called_once_with(
            'temp.csv', data_stages, ['name', 'sandbox-tables', 'description'])
        mock_logger.assert_called_once_with(True)
