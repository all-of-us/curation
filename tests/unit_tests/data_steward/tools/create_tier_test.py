"""
Unit Test for create tier module
"""

# Python imports
import unittest
import argparse
import mock
from datetime import datetime

# Third party imports

# Project imports
from constants.cdr_cleaner import clean_cdr as consts
from tools.add_cdr_metadata import INSERT
from tools.create_tier import parse_deid_args, validate_deid_stage_param, validate_tier_param, \
    validate_release_tag_param, create_datasets, get_dataset_name, create_tier, SCOPES, add_kwargs_to_args


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
        self.run_as = 'foo@bar.com'
        self.dataset_name = 'C2020q4r3'

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
            'base',
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
                                                'clean',
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
        args, kwargs = parse_deid_args(self.correct_parameter_list)
        results_dict = vars(args)

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

    def test_add_kwargs_to_args(self):
        actual_args = [
            '-p', self.project_id, '-d', self.input_dataset, '-b',
            f'{self.input_dataset}_sandbox', '--data_stage',
            f'{self.tier}_tier_{self.deid_stage}'
        ]
        expected_kwargs = [
            '-p', self.project_id, '-d', self.input_dataset, '-b',
            f'{self.input_dataset}_sandbox', '--data_stage',
            f'{self.tier}_tier_{self.deid_stage}', '--key', 'fake', '-w',
            'fake2'
        ]
        kwargs = {'key': 'fake', 'w': 'fake2'}
        no_kwargs = {}
        self.assertEqual(actual_args,
                         add_kwargs_to_args(actual_args, no_kwargs))
        self.assertEqual(expected_kwargs,
                         add_kwargs_to_args(actual_args, kwargs))

    def test_validate_deid_stage_param(self):
        # Preconditions
        invalid_deid_stage_params = ['baseee', 'base', 'clean', 'clean_base']

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
            consts.CLEAN: self.dataset_name,
            consts.SANDBOX: f'{self.dataset_name[1:]}_{consts.SANDBOX}',
            consts.STAGING: f'{self.dataset_name}_{consts.STAGING}'
        }

        # Tests if incorrect parameters are given
        self.assertRaises(RuntimeError, create_datasets, None,
                          self.dataset_name, self.input_dataset, self.tier,
                          self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client, None,
                          self.input_dataset, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client,
                          self.dataset_name, None, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client,
                          self.dataset_name, self.input_dataset, None,
                          self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, client,
                          self.dataset_name, self.input_dataset, self.tier,
                          None)

        # Test
        actual = create_datasets(client, self.dataset_name, self.input_dataset,
                                 self.tier, self.release_tag)

        # Post conditions
        client.create_dataset.assert_called()

        self.assertEqual(actual, datasets)

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
    @mock.patch('tools.create_tier.add_kwargs_to_args')
    @mock.patch('tools.create_tier.bq.copy_datasets')
    @mock.patch('tools.create_tier.create_datasets')
    @mock.patch('tools.create_tier.get_dataset_name')
    @mock.patch('tools.create_tier.bq.get_client')
    @mock.patch('tools.create_tier.auth.get_impersonation_credentials')
    @mock.patch('tools.create_tier.validate_create_tier_args')
    def test_create_tier(self, mock_validate_args, mock_impersonate_credentials,
                         mock_get_client, mock_dataset_name,
                         mock_create_datasets, mock_copy_datasets,
                         mock_add_kwargs, mock_cdr_main,
                         mock_create_schemaed_snapshot):
        final_dataset_name = f"{self.tier[0].upper()}{self.release_tag}_{self.deid_stage}"
        datasets = {
            consts.CLEAN: final_dataset_name,
            consts.STAGING: f'{final_dataset_name}_staging',
            consts.SANDBOX: f'{final_dataset_name}_sandbox'
        }
        controlled_tier_cleaning_args = [
            '-p', self.project_id, '-d', datasets[consts.STAGING], '-b',
            datasets[consts.SANDBOX], '--data_stage',
            f'{self.tier}_tier_{self.deid_stage}'
        ]
        mock_dataset_name.return_value = final_dataset_name
        mock_create_datasets.return_value = datasets
        client = mock_get_client.return_value = self.mock_bq_client
        cleaning_args = mock_add_kwargs.return_value = controlled_tier_cleaning_args
        kwargs = {}

        create_tier(self.credentials_filepath, self.project_id, self.tier,
                    self.input_dataset, self.release_tag, self.deid_stage,
                    self.run_as, **kwargs)

        mock_validate_args.assert_called_with(self.tier, self.deid_stage,
                                              self.release_tag)

        mock_impersonate_credentials.assert_called_with(
            self.run_as, SCOPES, self.credentials_filepath)

        mock_get_client.assert_called_with(
            self.project_id, credentials=mock_impersonate_credentials())

        mock_dataset_name.assert_called_with(self.tier, self.release_tag,
                                             self.deid_stage)

        mock_create_datasets.asserd_called_with(client, final_dataset_name,
                                                self.input_dataset, self.tier,
                                                self.release_tag)

        mock_copy_datasets.assert_called_with(client, self.input_dataset,
                                              datasets[consts.STAGING])

        mock_add_kwargs.assert_called_with(controlled_tier_cleaning_args,
                                           kwargs)
        mock_cdr_main.assert_called_with(args=cleaning_args)
        mock_create_schemaed_snapshot.assert_called_with(
            self.project_id, datasets[consts.STAGING], final_dataset_name,
            False)

    @mock.patch('tools.add_cdr_metadata.get_etl_version')
    @mock.patch('tools.create_tier.create_schemaed_snapshot_dataset')
    @mock.patch('tools.create_tier.clean_cdr.main')
    @mock.patch('tools.create_tier.add_kwargs_to_args')
    @mock.patch('tools.create_tier.auth.get_impersonation_credentials')
    @mock.patch('tools.create_tier.bq.get_client')
    @mock.patch('tools.add_cdr_metadata.main')
    @mock.patch('tools.create_tier.create_datasets')
    @mock.patch('tools.create_tier.get_dataset_name')
    def test_qa_handoff_date_update(
        self, mock_dataset_name, mock_create_datasets,
        mock_add_cdr_metadata_main, mock_get_client,
        mock_impersonate_credentials, mock_add_kwargs, mock_cdr_main,
        mock_create_schemaed_snapshot, mock_etl_version):
        final_dataset_name = f"{self.tier[0].upper()}{self.release_tag}_deid_base"
        datasets = {
            consts.CLEAN: final_dataset_name,
            consts.STAGING: f'{final_dataset_name}_staging',
            consts.SANDBOX: f'{final_dataset_name}_sandbox'
        }

        mock_dataset_name.return_value = final_dataset_name
        mock_create_datasets.return_value = datasets

        controlled_tier_cleaning_args = [
            '-p', self.project_id, '-d', datasets[consts.STAGING], '-b',
            datasets[consts.SANDBOX], '--data_stage',
            f'{self.tier}_tier_deid_base'
        ]

        mock_get_client.return_value = self.mock_bq_client
        mock_add_kwargs.return_value = controlled_tier_cleaning_args
        mock_get_client.return_value = self.mock_bq_client
        cleaning_args = mock_add_kwargs.return_value = controlled_tier_cleaning_args
        versions = mock_etl_version.return_value = ['test']

        kwargs = {}

        create_tier(self.credentials_filepath, self.project_id, self.tier,
                    self.input_dataset, self.release_tag, 'deid_base',
                    self.run_as, **kwargs)

        mock_cdr_main.assert_called_with(args=cleaning_args)

        updated_qa_handoff_date_args = [
            '--component', INSERT, '--project_id', self.project_id,
            '--target_dataset', datasets[consts.STAGING], '--qa_handoff_date',
            datetime.strftime(datetime.now(),
                              '%Y-%m-%d'), '--etl_version', versions[0]
        ]

        mock_add_cdr_metadata_main.assert_called_with(
            updated_qa_handoff_date_args)
