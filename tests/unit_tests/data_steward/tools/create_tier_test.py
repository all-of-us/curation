"""
Unit Test for create tier module
"""

# Python imports
from datetime import datetime
import unittest
import argparse
import mock

# Project imports
from cdr_cleaner.clean_cdr import DATA_STAGE_RULES_MAPPING
from constants.cdr_cleaner import clean_cdr as consts
from tools.add_cdr_metadata import INSERT
from tools.create_tier import parse_deid_args, validate_deid_stage_param, validate_tier_param, \
    validate_release_tag_param, create_datasets, get_dataset_name, create_tier, add_kwargs_to_args, \
    get_data_stage, DEID_STAGE_LIST, TIER_DEID_STAGE_TO_DATA_STAGE
from common import (CDR_SCOPES, CONTROLLED, CONTROLLED_PLUS, DE_IDENTIFIED,
                    PIPELINE_DATASET_SUFFIX, PIPELINE_TABLES, REGISTERED,
                    TIER_DATASET_PREFIX, TIER_LIST, ZIP3_SES_MAP)
from resources import get_pipeline_dataset_suffix, get_tier_dataset_prefix


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
            'owner': 'curation',
            'release_tag': self.release_tag,
            'data_tier': self.tier,
        }

        # Tools for mocking the client
        self.mock_bq_client = mock.MagicMock()

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
        args, _ = parse_deid_args(self.correct_parameter_list)
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
            self.assertRaises(TypeError, validate_tier_param, tier)

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
            self.assertRaises(TypeError, validate_deid_stage_param, ds)

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
        self.assertRaises(TypeError, get_dataset_name, incorrect_tier_param,
                          self.release_tag, self.deid_stage)
        self.assertRaises(argparse.ArgumentTypeError, get_dataset_name,
                          self.tier, incorrect_release_tag_param,
                          self.deid_stage)
        self.assertRaises(TypeError, get_dataset_name, self.tier,
                          self.release_tag, incorrect_deid_stage_param)

    def test_create_datasets(self):
        # Preconditions
        mocked_labels = [{
            'owner': 'curation',
            DE_IDENTIFIED: 'true',
            'phase': consts.CLEAN
        }, {
            'owner': 'curation',
            DE_IDENTIFIED: 'true',
            'phase': consts.STAGING
        }, {
            'owner': 'curation',
            DE_IDENTIFIED: 'false',
            'phase': consts.SANDBOX
        }]
        self.mock_bq_client.update_labels_and_tags.side_effect = mocked_labels

        datasets = {
            consts.CLEAN: self.dataset_name,
            consts.SANDBOX: f'{self.dataset_name[1:]}_{consts.SANDBOX}',
            consts.STAGING: f'{self.dataset_name}_{consts.STAGING}'
        }

        # Tests if incorrect parameters are given
        self.assertRaises(RuntimeError, create_datasets, None,
                          self.dataset_name, self.input_dataset, self.tier,
                          self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, self.mock_bq_client,
                          None, self.input_dataset, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, self.mock_bq_client,
                          self.dataset_name, None, self.tier, self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, self.mock_bq_client,
                          self.dataset_name, self.input_dataset, None,
                          self.release_tag)
        self.assertRaises(RuntimeError, create_datasets, self.mock_bq_client,
                          self.dataset_name, self.input_dataset, self.tier,
                          None)

        # Test
        actual = create_datasets(self.mock_bq_client, self.dataset_name,
                                 self.input_dataset, self.tier,
                                 self.release_tag)

        # Post conditions
        self.mock_bq_client.create_dataset.assert_called()

        self.assertEqual(actual, datasets)

        # Ensures datasets are created with the proper name, descriptions, and labels and tags
        self.assertEqual(self.mock_bq_client.define_dataset.call_count, 3)

        self.mock_bq_client.define_dataset.assert_has_calls([
            mock.call(datasets[consts.CLEAN], self.description,
                      self.labels_and_tags),
            mock.call(datasets[consts.STAGING], self.description,
                      self.labels_and_tags),
            mock.call(datasets[consts.SANDBOX], self.description,
                      self.labels_and_tags)
        ])

        # Ensures datasets are updated with the proper labels and tags (if dataset is de-identified or not)
        self.assertEqual(self.mock_bq_client.update_labels_and_tags.call_count,
                         3)

        self.mock_bq_client.update_labels_and_tags.assert_has_calls([
            mock.call(datasets[consts.CLEAN], self.labels_and_tags,
                      mocked_labels[0]),
            mock.call(datasets[consts.STAGING], self.labels_and_tags,
                      mocked_labels[1]),
            mock.call(datasets[consts.SANDBOX], self.labels_and_tags,
                      mocked_labels[2]),
        ])

    @mock.patch('tools.create_tier.clean_cdr.main')
    @mock.patch('tools.create_tier.add_kwargs_to_args')
    @mock.patch('tools.create_tier.create_datasets')
    @mock.patch('tools.create_tier.get_dataset_name')
    @mock.patch('tools.create_tier.BigQueryClient')
    @mock.patch('tools.create_tier.auth.get_impersonation_credentials')
    @mock.patch('tools.create_tier.validate_create_tier_args')
    def test_create_tier(self, mock_validate_args, mock_impersonate_credentials,
                         mock_client, mock_dataset_name, mock_create_datasets,
                         mock_add_kwargs, mock_cdr_main):
        final_dataset_name = f"{self.tier[0].upper()}{self.release_tag}_{self.deid_stage}"
        datasets = {
            consts.CLEAN: final_dataset_name,
            consts.STAGING: f'{final_dataset_name}_staging',
            consts.SANDBOX: f'{final_dataset_name}_sandbox'
        }
        controlled_tier_cleaning_args = [
            '-p', self.project_id, '-d', datasets[consts.STAGING], '-b',
            datasets[consts.SANDBOX], '--data_stage',
            f'{self.tier}_tier_{self.deid_stage}', '--run_as', self.run_as,
            '--console_log'
        ]
        mock_dataset_name.return_value = final_dataset_name
        mock_create_datasets.return_value = datasets
        mock_client.return_value = self.mock_bq_client
        cleaning_args = mock_add_kwargs.return_value = controlled_tier_cleaning_args
        kwargs = {}

        create_tier(self.credentials_filepath, self.project_id, self.tier,
                    self.input_dataset, self.release_tag, self.deid_stage,
                    self.run_as, **kwargs)

        mock_validate_args.assert_called_with(self.tier, self.deid_stage,
                                              self.release_tag)

        mock_impersonate_credentials.assert_called_with(
            self.run_as, CDR_SCOPES, self.credentials_filepath)

        mock_client.assert_called_with(
            self.project_id, credentials=mock_impersonate_credentials())

        mock_dataset_name.assert_called_with(self.tier, self.release_tag,
                                             self.deid_stage)

        mock_create_datasets.asserd_called_with(self.mock_bq_client,
                                                final_dataset_name,
                                                self.input_dataset, self.tier,
                                                self.release_tag)

        self.mock_bq_client.copy_dataset.assert_called_with(
            f'{self.project_id}.{self.input_dataset}',
            f'{self.project_id}.{datasets[consts.STAGING]}')

        mock_add_kwargs.assert_called_with(controlled_tier_cleaning_args,
                                           kwargs)
        mock_cdr_main.assert_called_with(args=cleaning_args)
        self.mock_bq_client.build_and_copy_contents.assert_called_with(
            datasets[consts.STAGING], final_dataset_name)

    @mock.patch('tools.add_cdr_metadata.get_etl_version')
    @mock.patch('tools.create_tier.clean_cdr.main')
    @mock.patch('tools.create_tier.add_kwargs_to_args')
    @mock.patch('tools.create_tier.auth.get_impersonation_credentials')
    @mock.patch('tools.create_tier.BigQueryClient')
    @mock.patch('tools.add_cdr_metadata.main')
    @mock.patch('tools.create_tier.create_datasets')
    @mock.patch('tools.create_tier.get_dataset_name')
    def test_qa_handoff_date_update(self, mock_dataset_name,
                                    mock_create_datasets,
                                    mock_add_cdr_metadata_main, mock_client,
                                    mock_impersonate_credentials,
                                    mock_add_kwargs, mock_cdr_main,
                                    mock_etl_version):
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

        mock_client.return_value = self.mock_bq_client
        mock_add_kwargs.return_value = controlled_tier_cleaning_args
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
        self.mock_bq_client.build_and_copy_contents.assert_called_with(
            datasets[consts.STAGING], final_dataset_name)


class CreateTierControlledPlusTest(unittest.TestCase):
    """
    Covers the tier-generic naming and data-stage resolution added for CT+ (DL-2462).
    """

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.release_tag = '2020q4r3'
        self.project_id = 'fake_project_id'
        self.input_dataset = 'fake_input'
        self.run_as = 'foo@bar.com'
        self.credentials_filepath = 'fake/file/path.json'
        self.mock_bq_client = mock.MagicMock()

    def test_every_tier_has_a_prefix_and_a_suffix(self):
        for tier in TIER_LIST:
            self.assertIn(tier, TIER_DATASET_PREFIX)
            self.assertIn(tier, PIPELINE_DATASET_SUFFIX)

    def test_prefix_lookup_rejects_unknown_tier(self):
        self.assertRaises(ValueError, get_tier_dataset_prefix, 'platinum')
        self.assertRaises(ValueError, get_pipeline_dataset_suffix, 'platinum')

    def test_controlled_plus_does_not_collide_with_controlled(self):
        """The tier[0].upper() bug this replaces gave both tiers the same name."""
        for deid_stage in DEID_STAGE_LIST:
            if (CONTROLLED_PLUS,
                    deid_stage) not in TIER_DEID_STAGE_TO_DATA_STAGE:
                continue
            names = {
                tier: get_dataset_name(tier, self.release_tag, deid_stage)
                for tier in TIER_LIST
            }
            self.assertEqual(len(set(names.values())), len(TIER_LIST),
                             f'dataset names collide at {deid_stage}: {names}')

    def test_controlled_plus_pipeline_output_is_not_the_published_name(self):
        """create_tier must not write the name regenerate_ct_plus_ids.py publishes."""
        name = get_dataset_name(CONTROLLED_PLUS, self.release_tag, 'deid_clean')
        self.assertEqual(name, f'CP{self.release_tag}_deid_clean_pre_rekey')
        self.assertNotEqual(name, f'CP{self.release_tag}_deid_clean')

    def test_registered_and_controlled_names_are_unchanged(self):
        """Guards the two live tiers against a regression from the prefix map."""
        for deid_stage in DEID_STAGE_LIST:
            self.assertEqual(
                get_dataset_name(REGISTERED, self.release_tag, deid_stage),
                f'R{self.release_tag}_{deid_stage}')
            self.assertEqual(
                get_dataset_name(CONTROLLED, self.release_tag, deid_stage),
                f'C{self.release_tag}_{deid_stage}')

    def test_every_tier_deid_stage_pair_resolves_to_a_real_data_stage(self):
        """Enumerates the pairs rather than sampling them."""
        stage_values = {stage.value for stage in consts.DataStage}
        for (tier,
             deid_stage), data_stage in TIER_DEID_STAGE_TO_DATA_STAGE.items():
            self.assertIn(tier, TIER_LIST)
            self.assertIn(deid_stage, DEID_STAGE_LIST)
            self.assertIn(data_stage, stage_values)
            self.assertIn(data_stage, DATA_STAGE_RULES_MAPPING)
            self.assertEqual(data_stage, get_data_stage(tier, deid_stage))

    def test_controlled_plus_has_no_fitbit_stage(self):
        """CT+ is a re-key of CT, so CT's fitbit deid pass is not repeated."""
        self.assertRaises(TypeError, get_data_stage, CONTROLLED_PLUS,
                          'fitbit_deid')

    @mock.patch('tools.add_cdr_metadata.get_etl_version')
    @mock.patch('tools.create_tier.clean_cdr.main')
    @mock.patch('tools.create_tier.auth.get_impersonation_credentials')
    @mock.patch('tools.create_tier.BigQueryClient')
    @mock.patch('tools.add_cdr_metadata.main')
    @mock.patch('tools.create_tier.create_datasets')
    def test_zip3_ses_map_is_copied_for_controlled_plus(
            self, mock_create_datasets, mock_add_cdr_metadata_main, mock_client,
            mock_impersonate_credentials, mock_cdr_main, mock_etl_version):
        """Breakage 4: the equality check on 'controlled' skipped CT+ silently."""
        for tier in (CONTROLLED, CONTROLLED_PLUS):
            with self.subTest(tier=tier):
                self.mock_bq_client.reset_mock()
                final_dataset_name = get_dataset_name(tier, self.release_tag,
                                                      'deid_base')
                datasets = {
                    consts.CLEAN: final_dataset_name,
                    consts.STAGING: f'{final_dataset_name}_staging',
                    consts.SANDBOX: f'{final_dataset_name}_sandbox'
                }
                mock_create_datasets.return_value = datasets
                mock_client.return_value = self.mock_bq_client
                mock_etl_version.return_value = ['test']

                create_tier(self.credentials_filepath, self.project_id, tier,
                            self.input_dataset, self.release_tag, 'deid_base',
                            self.run_as)

                self.mock_bq_client.copy_table.assert_called_with(
                    f'{self.project_id}.{PIPELINE_TABLES}.{ZIP3_SES_MAP}',
                    f'{self.project_id}.{datasets[consts.STAGING]}.{ZIP3_SES_MAP}'
                )
