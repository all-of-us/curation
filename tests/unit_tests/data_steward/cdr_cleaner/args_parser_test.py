import mock
import os
import unittest

from cdr_cleaner import args_parser as parser
import cdr_cleaner.clean_cdr as control
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.cdr_cleaner.reporter as consts


class CleanRulesArgsParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # precondition
        self.values = {
            'good_stages': ['--data-stage', 'ehr', 'rdr'],
            'bad_stages': ['--data-stage', 'foo', 'bar'],
            'no_stages': ['--data-stage'],
            'good_fields': ['--fields', 'sql', 'description', 'name'],
            'bad_fields': ['--fields', 'sql', 'description', 'lineno'],
            'no_fields': ['--fields'],
            'set_console_logging': ['--console-log'],
            'good_output_file': ['--output-file', 'output.csv'],
            'bad_output_file': ['--output-file', 'output.txt']
        }

        self.defaults_list = self.values['good_stages'] + self.values[
            'good_fields']

        self.report_parser = parser.get_report_parser()

    def test_parse_bad_report_output_file(self):
        bad_output_file_list = self.values['good_stages'] + self.values[
            'good_fields'] + self.values['bad_output_file']
        # test
        namespace = self.report_parser.parse_args(bad_output_file_list)

        # post condition
        self.assertEqual(namespace.output_filepath, 'clean_rules_report.csv')

    def test_parse_good_report_output_file(self):
        good_output_file_list = self.values['good_stages'] + self.values[
            'good_fields'] + self.values['good_output_file']
        # test
        namespace = self.report_parser.parse_args(good_output_file_list)

        # post condition
        self.assertEqual(namespace.output_filepath, 'output.csv')

    def test_parse_good_stages(self):
        # test
        namespace = self.report_parser.parse_args(self.defaults_list)

        # post condition
        self.assertEqual(namespace.data_stage, ['ehr', 'rdr'])

    def test_parse_bad_stages(self):
        bad_stages_list = self.values['bad_stages'] + self.values['good_fields']

        # test
        self.assertRaises(SystemExit, self.report_parser.parse_args,
                          bad_stages_list)

    def test_parse_no_stages(self):
        no_stages_list = self.values['good_fields']

        # test
        self.assertRaises(SystemExit, self.report_parser.parse_args,
                          no_stages_list)

    def test_parse_good_fields(self):
        # test
        namespace = self.report_parser.parse_args(self.defaults_list)

        # post condition
        self.assertEqual(namespace.fields, ['sql', 'description', 'name'])

    def test_parse_bad_fields(self):
        bad_fields_list = self.values['good_stages'] + self.values['bad_fields']

        self.assertRaises(SystemExit, self.report_parser.parse_args,
                          bad_fields_list)

    def test_parse_no_field(self):
        no_fields_list = self.values['good_stages']

        self.assertRaises(SystemExit, self.report_parser.parse_args,
                          no_fields_list)

    def test_parse_setting_console_logging(self):
        console_logs = self.defaults_list + ['--console-log']

        namespace = self.report_parser.parse_args(console_logs)

        self.assertTrue(namespace.console_log)

    def test_parse_default_report_values(self):
        # test
        namespace = self.report_parser.parse_args(self.defaults_list)

        # post condition
        self.assertEqual(namespace.data_stage, ['ehr', 'rdr'])
        self.assertFalse(namespace.console_log)
        self.assertEqual(namespace.output_filepath, 'clean_rules_report.csv')
        self.assertEqual(namespace.fields, ['sql', 'description', 'name'])
