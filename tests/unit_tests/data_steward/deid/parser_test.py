"""
Unit Test for parser module

Ensure the output dataset name includes _deid

Original Issue: DC-744
"""

# Python imports
import unittest
import os
from argparse import ArgumentTypeError

# Project imports
from constants.deid.deid import MAX_AGE
from deid.parser import odataset_name_verification, parse_args
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
        self.run_as_email = 'test@test.com'

        self.correct_parameter_list = [
            '--rules', self.rules, '--private_key', self.private_key, '--table',
            self.tablename, '--action', self.action, '--idataset',
            self.input_dataset, '--odataset', self.output_dataset, '--log',
            self.log_path, '--pipeline', self.pipeline, '--interactive',
            self.interactive, '--interactive', self.interactive, '--run_as',
            self.run_as_email
        ]
        self.incorrect_parameter_list = [
            '--rules', self.rules, '--private_key', self.private_key, '--table',
            self.tablename, '--action', self.action, '--idataset',
            self.input_dataset, '--log', self.log_path
        ]

    def test_parse_args(self):
        # Tests if incorrect parameters are given
        self.incorrect_parameter_list.extend(['--odataset', 'random_deid_tag'])
        self.assertRaises(SystemExit, parse_args, self.incorrect_parameter_list)

        # Preconditions
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {
            k.strip('-'): v for (k, v) in correct_parameter_dict.items()
        }

        # setting correct_parameter_dict values not set in setUp function
        correct_parameter_dict['cluster'] = False
        correct_parameter_dict['age_limit'] = MAX_AGE
        correct_parameter_dict['run_as_email'] = correct_parameter_dict.pop(
            'run_as')

        # Test if correct parameters are given
        results_dict = parse_args(self.correct_parameter_list)

        # Post conditions
        self.assertEqual(correct_parameter_dict, results_dict)

    def test_odataset_name_verification(self):
        # Tests if output dataset name does not end with _deid
        self.assertRaises(ArgumentTypeError, odataset_name_verification,
                          "foo_output")
        self.assertRaises(ArgumentTypeError, odataset_name_verification,
                          "foo_deid_output")

        # Tests if output dataset name does contain _deid
        result = odataset_name_verification(self.output_dataset)

        # Post conditions
        self.assertEqual(result, self.output_dataset)
