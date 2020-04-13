"""
Unit Test for parser module

Ensure that the output dataset name includes _deid

Original Issue: DC-744
"""

# Python imports
import unittest
import os
from argparse import ArgumentTypeError, ArgumentError

# Project imports
from deid.parser import odataset_name_verification, parse_args, Parse
from resources import DEID_PATH


class ParserTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.output_dataset = 'foo_output_deid'
        self.input_dataset = 'foo_input'
        self.tablename = 'bar_table'
        self.action = 'debug'
        self.log_path = 'deid-fake.log'
        self.private_key = 'fake/SA/file/path.json'
        self.rules = os.path.join(DEID_PATH, 'config', 'ids', 'config.json')
        self.pipeline = ['generalize', 'suppress', 'shift', 'compute']
        self.interactive = 'BATCH'

        self.correct_parameter_list = [
            '--rules', self.rules, '--private_key', self.private_key, '--table', self.tablename,
            '--action', self.action, '--idataset', self.input_dataset, '--odataset',
            self.output_dataset, '--log', self.log_path, '--pipeline', self.pipeline,
            '--interactive', self.interactive, '--interactive', self.interactive
        ]
        self.incorrect_parameter_list = [
            '--rules', self.rules, '--private_key', self.private_key, '--table', self.tablename,
            '--action', self.action, '--idataset', self.input_dataset, '--log', self.log_path
        ]

    def test_parse_args(self):
        # Tests if incorrect parameters are given
        self.assertRaises(SystemExit, parse_args, self.incorrect_parameter_list)

        # Tests if correct parameters are given
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {k.strip('--'): v for (k, v) in correct_parameter_dict.items()}

        results_dict = parse_args(self.correct_parameter_list)
        if 'cluster' in results_dict:
            del results_dict['cluster']
            del results_dict['age-limit']

        self.assertEqual(correct_parameter_dict, results_dict)

    def test_odataset_name_verification(self):
        # Tests if output dataser name does not contain _deid
        self.assertRaises(ArgumentTypeError, odataset_name_verification, "foo_output")

        # Tests if output dataset name does contain _deid
        result = odataset_name_verification("foo_output_deid")

        self.assertEqual(result, self.output_dataset)
