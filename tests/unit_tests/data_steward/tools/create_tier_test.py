"""
Unit Test for create tier module
"""

# Python imports
import unittest

# Project imports
from tools.create_tier import parse_deid_args, validate_deid_stage_param, validate_tier_param, \
    validate_release_tag_param


class CreateTierTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.credentials_filepath = 'fake/file/path.json'
        self.project_id = 'fake_project_id'
        self.tier = 'controlled'
        self.input_dataset = 'fake_input'
        self.release_tag = '2020q4r3'
        self.deid_stage = 'deid'

        self.correct_parameter_list = [
            '--credentials_filepath', self.credentials_filepath, '--project_id',
            self.project_id, '--tier', self.tier, '--idataset',
            self.input_dataset, '--release_tag', self.release_tag,
            '--deid_stage', self.deid_stage
        ]
        # incorrect parameter lists
        self.incorrect_parameter_list_1 = [
            '--project_id', self.project_id, '--tier', self.tier, '--idataset',
            self.input_dataset, '--release_tag', self.release_tag,
            '--deid_stage', self.deid_stage
        ]
        self.incorrect_parameter_list_2 = [
            '--credentials_filepath', self.credentials_filepath, '--tier',
            self.tier, '--idataset', self.input_dataset, '--release_tag',
            self.release_tag, '--deid_stage', self.deid_stage
        ]
        self.incorrect_parameter_list_3 = [
            '--credentials_filepath', self.credentials_filepath, '--project_id',
            self.project_id, '--idataset', self.input_dataset, '--release_tag',
            self.release_tag, '--deid_stage', self.deid_stage
        ]
        self.incorrect_parameter_list_4 = [
            '--credentials_filepath', self.credentials_filepath, '--project_id',
            self.project_id, '--tier', self.tier, '--release_tag',
            self.release_tag, '--deid_stage', self.deid_stage
        ]
        self.incorrect_parameter_list_5 = [
            '--credentials_filepath', self.credentials_filepath, '--project_id',
            self.project_id, '--tier', self.tier, '--idataset',
            self.input_dataset, '--deid_stage', self.deid_stage
        ]
        self.incorrect_parameter_list_6 = [
            '--credentials_filepath', self.credentials_filepath, '--project_id',
            self.project_id, '--tier', self.tier, '--idataset',
            self.input_dataset, '--release_tag', self.release_tag
        ]

    def test_parse_args(self):
        # Tests if incorrect parameters are given
        self.assertRaises(SystemExit, parse_deid_args,
                          self.incorrect_parameter_list_1)
        self.assertRaises(SystemExit, parse_deid_args,
                          self.incorrect_parameter_list_2)
        self.assertRaises(SystemExit, parse_deid_args,
                          self.incorrect_parameter_list_3)
        self.assertRaises(SystemExit, parse_deid_args,
                          self.incorrect_parameter_list_4)
        self.assertRaises(SystemExit, parse_deid_args,
                          self.incorrect_parameter_list_5)
        self.assertRaises(SystemExit, parse_deid_args,
                          self.incorrect_parameter_list_6)

        # Preconditions
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {
            k.strip('-'): v for (k, v) in correct_parameter_dict.items()
        }

        # Test if correct parameters are given
        results_dict = parse_deid_args(self.correct_parameter_list)

        # Post conditions
        self.assertEqual(correct_parameter_dict, results_dict)

    def test_validate_release_tag_param(self):
        # Preconditions
        invalid_release_tags = [
            '202q3r4', '2020q34r2', '2020q3r22', '2020qq3r2'
        ]

        # Test if invalid parameters are given
        for tag in invalid_release_tags:
            expected_error_output = f'ERROR:tools.create_tier:Parameter ERROR {tag} is in an incorrect format, ' \
                                    f'accepted: YYYYq#r#'
            with self.assertLogs() as cm:
                validate_release_tag_param(tag)
            self.assertIn(expected_error_output, cm.output)

    def test_validate_tier_param(self):
        # Preconditions
        invalid_tier_params = ['foo', 'bar', 'controled', 'registry']

        # Test if invalid parameters are given
        for tier in invalid_tier_params:
            expected_error_output = f"ERROR:tools.create_tier:Parameter ERROR: {tier} is an incorrect input for the " \
                                    f"tier parameter, accepted: controlled or registered"
            with self.assertLogs() as cm:
                validate_tier_param(tier)
            self.assertIn(expected_error_output, cm.output)

    def test_validate_deid_stage_param(self):
        # Preconditions
        invalid_deid_stage_params = [
            'baseee', 'deid_base', 'deid_clean', 'clean_base'
        ]

        # Test if invalid parameters are given
        for ds in invalid_deid_stage_params:
            expected_error_output = f"ERROR:tools.create_tier:Parameter ERROR: {ds} is an incorrect input for " \
                                    f"the deid_stage parameter, accepted: deid, base, clean"
            with self.assertLogs() as cm:
                validate_deid_stage_param(ds)
            self.assertIn(expected_error_output, cm.output)
