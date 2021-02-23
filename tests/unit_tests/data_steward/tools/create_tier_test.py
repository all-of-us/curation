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
    validate_release_tag_param, create_datasets, get_dataset_name, create_tier, SCOPES

from utils import bq


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
        self.run_as = 'foo@bar.com'
        self.client = bq.get_client(self.project_id)

        self.description = f'dataset created from {self.input_dataset} for {self.tier}{self.release_tag} CDR run'
        self.labels_and_tags = {
            'release_tag': self.release_tag,
            'data_tier': self.tier,
        }

        # Tools for mocking the client
        mock_bq_client_patcher = mock.patch('utils.bq.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()
        self.addCleanup(mock_bq_client_patcher.stop)

        self.correct_parameter_list = [
            '--credentials_filepath',
            self.credentials_filepath,
            '--project_id',
            self.project_id,
            '--tier',
            self.tier,
            '--idataset',
            self.input_dataset,
            '--release_tag',
            self.release_tag,
            '--deid_stage',
            self.deid_stage,
            '--run_as',
            self.run_as,
            '--console_log',
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
        incorrect_deid_stage_choice_args = [[
            '--credentials_filepath',
            self.credentials_filepath,
            '--project_id',
            self.project_id,
            '--tier',
            self.tier,
            '--idataset',
            self.input_dataset,
            '--release_tag',
            self.release_tag,
            '--deid_stage',
            'deid_base',
            '--run_as',
            self.run_as,
        ],
                                            [
                                                '--credentials_filepath',
                                                self.credentials_filepath,
                                                '--project_id',
                                                self.project_id,
                                                '--tier',
                                                self.tier,
                                                '--idataset',
                                                self.input_dataset,
                                                '--release_tag',
                                                self.release_tag,
                                                '--deid_stage',
                                                'deid_clean',
                                                '--run_as',
                                                self.run_as,
                                            ]]
        for args in incorrect_deid_stage_choice_args:
            self.assertRaises(SystemExit, parse_deid_args, args)

        # Tests if incorrect choice for tier are given
        incorrect_tier_choice_args = [[
            '--credentials_filepath',
            self.credentials_filepath,
            '--project_id',
            self.project_id,
            '--tier',
            'uncontrolled',
            '--idataset',
            self.input_dataset,
            '--release_tag',
            self.release_tag,
            '--deid_stage',
            self.deid_stage,
            '--run_as',
            self.run_as,
        ],
                                      [
                                          '--credentials_filepath',
                                          self.credentials_filepath,
                                          '--project_id',
                                          self.project_id,
                                          '--tier',
                                          'registry',
                                          '--idataset',
                                          self.input_dataset,
                                          '--release_tag',
                                          self.release_tag,
                                          '--deid_stage',
                                          self.deid_stage,
                                          '--run_as',
                                          self.run_as,
                                      ]]
        for args in incorrect_tier_choice_args:
            self.assertRaises(SystemExit, parse_deid_args, args)

        # Preconditions
        it = iter(self.correct_parameter_list)
        correct_parameter_dict = dict(zip(it, it))
        correct_parameter_dict = {
            k.strip('-'): v for (k, v) in correct_parameter_dict.items()
        }
        correct_parameter_dict['target_principal'] = correct_parameter_dict.pop(
            'run_as', 'f@b.com')
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
            consts.CLEAN: self.name,
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
        self.assertEqual(mock_define_dataset.call_count, 3)

        mock_define_dataset.assert_has_calls([
            mock.call(client.project, datasets[consts.CLEAN], self.description,
                      self.labels_and_tags),
            mock.call(client.project, datasets[consts.STAGING],
                      self.description, self.labels_and_tags),
            mock.call(client.project, datasets[consts.SANDBOX],
                      self.description, self.labels_and_tags)
        ])

        # Ensures datasets are updated with the proper labels and tags (if dataset is de-identified or not)
        self.assertEqual(mock_update_labels_tags.call_count, 3)

        mock_update_labels_tags.assert_has_calls([
            mock.call(datasets[consts.CLEAN], self.labels_and_tags, {
                'de-identified': 'true',
                'phase': consts.CLEAN
            }),
            mock.call(datasets[consts.STAGING], self.labels_and_tags, {
                'de-identified': 'true',
                'phase': consts.STAGING
            }),
            mock.call(datasets[consts.SANDBOX], self.labels_and_tags, {
                'de-identified': 'false',
                'phase': consts.SANDBOX
            }),
        ])

    @mock.patch('tools.create_tier.create_schemaed_snapshot_dataset')
    @mock.patch('tools.create_tier.clean_cdr.main')
    @mock.patch('tools.create_tier.bq.copy_datasets')
    @mock.patch('tools.create_tier.create_datasets')
    @mock.patch('tools.create_tier.get_dataset_name')
    @mock.patch('tools.create_tier.bq.get_client')
    @mock.patch('tools.create_tier.auth.get_impersonation_credentials')
    @mock.patch('tools.create_tier.validate_create_tier_args')
    def test_create_tier(self, mock_validate_args, mock_impersonate_credentials,
                         mock_get_client, mock_dataset_name,
                         mock_create_datasets, mock_copy_datasets,
                         mock_cdr_main, mock_create_schemaed_snapshot):
        final_dataset_name = f"{self.tier[0].upper()}{self.release_tag}_{self.deid_stage}"
        datasets = {
            consts.CLEAN: final_dataset_name,
            consts.STAGING: f'{final_dataset_name}_staging',
            consts.SANDBOX: f'{final_dataset_name}_sandbox'
        }
        mock_dataset_name.return_value = final_dataset_name
        mock_create_datasets.return_value = datasets

        create_tier(self.credentials_filepath, self.project_id, self.tier,
                    self.input_dataset, self.release_tag, self.deid_stage,
                    self.run_as)

        mock_validate_args.assert_called_with(self.tier, self.deid_stage,
                                              self.release_tag)

        mock_impersonate_credentials.assert_called_with(
            self.run_as, SCOPES, self.credentials_filepath)

        mock_get_client.assert_called_with(self.project_id,
                                           mock_impersonate_credentials())

        mock_dataset_name.assert_called_with(self.tier, self.release_tag,
                                             self.deid_stage)

        mock_create_datasets.asserd_called_with(mock_get_client(),
                                                final_dataset_name,
                                                self.input_dataset, self.tier,
                                                self.release_tag)

        mock_copy_datasets.assert_called_with(mock_get_client(),
                                              self.input_dataset,
                                              datasets[consts.STAGING])

        controlled_tier_cleaning_args = [
            '-p', self.project_id, '-d', datasets[consts.STAGING], '-b',
            datasets[consts.SANDBOX], '--data_stage', self.tier
        ]
        mock_cdr_main.assert_called_with(args=controlled_tier_cleaning_args)
        mock_create_schemaed_snapshot.assert_called_with(
            self.project_id, datasets[consts.STAGING], final_dataset_name,
            False)
