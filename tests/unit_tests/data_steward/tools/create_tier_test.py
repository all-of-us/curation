"""
Unit Test for create tier module
"""

# Python imports
import unittest
import argparse
import mock

# Third party imports

# Project imports
from constants.cdr_cleaner import clean_cdr as consts
from tools.create_tier import parse_deid_args, validate_deid_stage_param, validate_tier_param, \
    validate_release_tag_param, create_datasets, get_dataset_name


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
        self.name = 'foo_name'

        self.description = f'Dataset created for {self.tier}{self.release_tag} CDR run'
        self.labels_and_tags = {
            'release_tag': self.release_tag,
            'data_tier': self.tier,
        }

        # Tools for mocking the client
        mock_bq_client_patcher = mock.patch('utils.bq.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()

        self.correct_parameter_list = [
            '--credentials_filepath', self.credentials_filepath, '--project_id',
            self.project_id, '--tier', self.tier, '--idataset',
            self.input_dataset, '--release_tag', self.release_tag,
            '--deid_stage', self.deid_stage, '--console_log'
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

        # Tests if incorrect choice for deid_stage are given
        incorrect_deid_stage_choice_args = [
            [
                '--credentials_filepath', self.credentials_filepath,
                '--project_id', self.project_id, '--tier', self.tier,
                '--idataset', self.input_dataset, '--release_tag',
                self.release_tag, '--deid_stage', 'deid_base'
            ],
            [
                '--credentials_filepath', self.credentials_filepath,
                '--project_id', self.project_id, '--tier', self.tier,
                '--idataset', self.input_dataset, '--release_tag',
                self.release_tag, '--deid_stage', 'deid_clean'
            ]
        ]
        for args in incorrect_deid_stage_choice_args:
            self.assertRaises(SystemExit, parse_deid_args, args)

        # Tests if incorrect choice for tier are given
        incorrect_tier_choice_args = [
            [
                '--credentials_filepath', self.credentials_filepath,
                '--project_id', self.project_id, '--tier', 'uncontrolled',
                '--idataset', self.input_dataset, '--release_tag',
                self.release_tag, '--deid_stage', self.deid_stage
            ],
            [
                '--credentials_filepath', self.credentials_filepath,
                '--project_id', self.project_id, '--tier', 'registry',
                '--idataset', self.input_dataset, '--release_tag',
                self.release_tag, '--deid_stage', self.deid_stage
            ]
        ]
        for args in incorrect_tier_choice_args:
            self.assertRaises(SystemExit, parse_deid_args, args)

        # Preconditions
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {
            k.strip('-'): v for (k, v) in correct_parameter_dict.items()
        }
        correct_parameter_dict['console_log'] = True

        # Test if correct parameters are given
        results_dict = vars(parse_deid_args(self.correct_parameter_list))

        # Post conditions
        self.assertEqual(correct_parameter_dict, results_dict)

    def test_validate_release_tag_param(self):
        # Preconditions
        invalid_release_tags = ['202q3r4', '2020q34r22']

        # Test if invalid parameters are given
        for tag in invalid_release_tags:
            self.assertRaises(argparse.ArgumentTypeError,
                              validate_release_tag_param, tag)

    def test_validate_tier_param(self):
        # Preconditions
        invalid_tier_params = ['foo', 'bar', 'controled', 'registry']

        # Test if invalid parameters are given
        for tier in invalid_tier_params:
            # test type error is raised
            self.assertRaises(argparse.ArgumentTypeError, validate_tier_param,
                              tier)

    def test_validate_deid_stage_param(self):
        # Preconditions
        invalid_deid_stage_params = [
            'baseee', 'deid_base', 'deid_clean', 'clean_base'
        ]

        # Test if invalid parameters are given
        for ds in invalid_deid_stage_params:
            # test type error is raised
            self.assertRaises(argparse.ArgumentTypeError,
                              validate_deid_stage_param, ds)

    def test_get_dataset_name(self):
        # Preconditions
        expected_dataset_name = 'C2020q4r3_deid'
        incorrect_tier_param = 'uncontrolled'
        incorrect_release_tag_param = '20222q33R5'
        incorrect_deid_stage_param = 'deid_base_clean'

        # Test if correct parameters are given
        result = get_dataset_name(self.tier, self.release_tag, self.deid_stage)

        # Post conditions
        self.assertEqual(result, expected_dataset_name)

        # Test if incorrect parameters are given
        self.assertRaises(argparse.ArgumentTypeError, get_dataset_name,
                          incorrect_tier_param, self.release_tag,
                          self.deid_stage)
        self.assertRaises(argparse.ArgumentTypeError, get_dataset_name,
                          self.tier, incorrect_release_tag_param,
                          self.deid_stage)
        self.assertRaises(argparse.ArgumentTypeError, get_dataset_name,
                          self.tier, self.release_tag,
                          incorrect_deid_stage_param)

    @mock.patch('utils.bq.update_labels_and_tags')
    @mock.patch('utils.bq.define_dataset')
    @mock.patch('utils.bq.get_client')
    def test_create_dataset(self, mock_client, mock_define_dataset,
                            mock_update_labels_tags):
        # Preconditions
        client = mock_client.return_value = self.mock_bq_client
        client.side_effects = create_datasets

        datasets = {
            self.name: self.name,
            consts.BACKUP: f'{self.name}_{consts.BACKUP}',
            consts.SANDBOX: f'{self.name}_{consts.SANDBOX}',
            consts.STAGING: f'{self.name}_{consts.STAGING}'
        }

        # Tests if incorrect parameters are given
        self.assertRaises(RuntimeError, create_datasets, None, self.name,
                          self.input_dataset, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client, None,
                          self.input_dataset, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client, self.name,
                          None, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client, self.name,
                          self.input_dataset, None, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client, self.name,
                          self.input_dataset, self.tier, None)

        # Test
        expected = create_datasets(client, self.name, self.input_dataset,
                                   self.tier, self.release_tag)

        # Post conditions
        client.create_dataset.assert_called()

        self.assertEqual(expected, datasets)

        # Ensures datasets are created with the proper name, descriptions, and labels and tags
        self.assertEqual(mock_define_dataset.call_count, 4)

        mock_define_dataset.assert_has_calls([
            mock.call(client.project, datasets[self.name], self.description,
                      self.labels_and_tags),
            mock.call(client.project, datasets[consts.BACKUP], self.description,
                      self.labels_and_tags),
            mock.call(client.project, datasets[consts.STAGING],
                      self.description, self.labels_and_tags),
            mock.call(client.project, datasets[consts.SANDBOX],
                      self.description, self.labels_and_tags)
        ])

        # Ensures datasets are updated with the proper labels and tags (if dataset is de-identified or not)
        self.assertEqual(mock_update_labels_tags.call_count, 4)

        mock_update_labels_tags.assert_has_calls([
            mock.call(datasets[consts.SANDBOX], self.labels_and_tags,
                      {'phase': {consts.SANDBOX}}),
            mock.call(datasets[consts.BACKUP], self.labels_and_tags, {
                'de-identified': 'false',
                'phase': {consts.BACKUP}
            }),
            mock.call(datasets[consts.STAGING], self.labels_and_tags, {
                'de-identified': 'true',
                'phase': {consts.STAGING}
            }),
            mock.call(datasets[self.name], self.labels_and_tags,
                      {'de-identified': 'true'})
        ])
