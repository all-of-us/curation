import unittest

import cdr_cleaner.clean_cdr as cc
from constants.cdr_cleaner.clean_cdr import DataStage


class CleanCDRTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test project'
        self.dataset_id = 'test dataset'
        self.sandbox_dataset_id = 'test sandbox'
        self.mapping_dataset_id = 'test mapping'
        self.combined_dataset_id = 'test combined'
        self.dest_table = 'dest_table'

    def test_data_stage(self):
        actual_stages = [value for item, value in DataStage.__members__.items()]
        expected_stages = list([s for s in DataStage])
        self.assertEqual(actual_stages, expected_stages)

    def test_parser(self):
        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '--data_stage', 'ehr'
        ]
        parser = cc.get_parser()
        args = parser.parse_args(test_args)
        self.assertIn(args.data_stage, DataStage)

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '--data_stage',
            'unspecified'
        ]
        parser = cc.get_parser()
        self.assertRaises(SystemExit, parser.parse_args, test_args)

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '--data_stage',
            'unknown'
        ]
        parser = cc.get_parser()
        self.assertRaises(SystemExit, parser.parse_args, test_args)

    def test_fetch_args_kwargs(self):
        expected_kwargs = {
            'mapping_dataset_id': self.mapping_dataset_id,
            'combined_dataset_id': self.combined_dataset_id
        }

        expected_kwargs_list = []
        for k, v in expected_kwargs.items():
            expected_kwargs_list.extend([f'--{k}', v])

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '--data_stage', 'ehr'
        ] + expected_kwargs_list

        basic_parser = cc.get_parser()
        known_args, unknown_args = basic_parser.parse_known_args(test_args)
        dynamic_parser = cc.get_dynamic_parser(unknown_args)
        args = dynamic_parser.parse_args(test_args)
        actual_kwargs = {
            k: v
            for k, v in vars(args).items()
            if k not in vars(known_args).keys()
        }
        self.assertDictEqual(expected_kwargs, actual_kwargs)

        test_args_incorrect = test_args + ['-v', 'value']
        basic_parser = cc.get_parser()
        known_args, unknown_args = basic_parser.parse_known_args(
            test_args_incorrect)
        self.assertRaises(SystemExit, cc.get_dynamic_parser, unknown_args)

        test_args_incorrect = test_args + ['--v', '-value']
        basic_parser = cc.get_parser()
        known_args, unknown_args = basic_parser.parse_known_args(
            test_args_incorrect)
        self.assertRaises(SystemExit, cc.get_dynamic_parser, unknown_args)

        test_args_incorrect = test_args + ['--v', 'v', '-value']
        basic_parser = cc.get_parser()
        known_args, unknown_args = basic_parser.parse_known_args(
            test_args_incorrect)
        self.assertRaises(SystemExit, cc.get_dynamic_parser, unknown_args)
