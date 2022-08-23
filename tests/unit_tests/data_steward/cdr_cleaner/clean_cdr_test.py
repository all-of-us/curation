import unittest

from mock import patch

import cdr_cleaner.clean_cdr as cc
from constants.cdr_cleaner.clean_cdr import DataStage
from tests.test_util import FakeRuleClass, fake_rule_func


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
            '-p', self.project_id, '-d', self.dataset_id, '-b',
            self.sandbox_dataset_id, '--data_stage', 'ehr', '--run_as', None
        ]
        parser = cc.get_parser()
        args = parser.parse_args(test_args)
        self.assertIn(args.data_stage, DataStage)

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '-b',
            self.sandbox_dataset_id, '--data_stage', 'unspecified', '--run_as',
            None
        ]
        parser = cc.get_parser()
        self.assertRaises(SystemExit, parser.parse_args, test_args)

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '-b',
            self.sandbox_dataset_id, '--data_stage', 'unknown', '--run_as', None
        ]
        parser = cc.get_parser()
        self.assertRaises(SystemExit, parser.parse_args, test_args)

    def test_get_kwargs(self):
        expected = {'k1': 'v1', 'k2': 'v2'}

        # All spaces
        actual_result = cc._get_kwargs(['--k1', 'v1', '--k2', 'v2'])
        self.assertEqual(expected, actual_result)

        # Space and `=`
        actual_result = cc._get_kwargs(['--k1', 'v1', '--k2=v2'])
        self.assertEqual(expected, actual_result)

        # All `=`
        actual_result = cc._get_kwargs(['--k1=v1', '--k2=v2'])
        self.assertEqual(expected, actual_result)

        # key mismatch, correct count
        with self.assertRaises(RuntimeError):
            cc._get_kwargs(['--k1', '--k2', 'v1', 'v2'])

        # value mismatch, correct count
        with self.assertRaises(RuntimeError):
            cc._get_kwargs(['--k1', 'v1', 'v2', '--k2'])

        # bad count
        with self.assertRaises(RuntimeError):
            cc._get_kwargs(['--k1=v1', '--k2'])

        # `=` mismatched
        with self.assertRaises(RuntimeError):
            cc._get_kwargs(['--k1=v1=v2'])

        # Empty key
        with self.assertRaises(RuntimeError):
            cc._get_kwargs(['--', 'v1'])

    def test_to_kwarg_key(self):
        expected_result = 'k1'
        actual_result = cc._to_kwarg_key('--k1')
        self.assertEqual(expected_result, actual_result)
        with self.assertRaises(RuntimeError) as c:
            cc._to_kwarg_key('-k1')
        with self.assertRaises(RuntimeError) as c:
            cc._to_kwarg_key('k1')
        with self.assertRaises(RuntimeError) as c:
            cc._to_kwarg_key('--')

    def test_fetch_args_kwargs(self):
        expected_kwargs = {
            'mapping_dataset_id': self.mapping_dataset_id,
            'combined_dataset_id': self.combined_dataset_id
        }
        expected_kwargs_list = []
        for k, v in expected_kwargs.items():
            expected_kwargs_list.extend([f'--{k}', v])

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, '-b',
            self.sandbox_dataset_id, '--data_stage', 'ehr', '--run_as', None
        ]
        expected_args = {
            'project_id': self.project_id,
            'dataset_id': self.dataset_id,
            'sandbox_dataset_id': self.sandbox_dataset_id,
            'data_stage': DataStage.EHR,
            'console_log': False,
            'list_queries': False,
            'run_as': None
        }
        parser = cc.get_parser()
        actual_args, actual_kwargs = cc.fetch_args_kwargs(
            parser, test_args + expected_kwargs_list)
        self.assertDictEqual(actual_args.__dict__, expected_args)
        self.assertDictEqual(expected_kwargs, actual_kwargs)

        actual_args, actual_kwargs = cc.fetch_args_kwargs(
            parser, test_args + ['--v', '-1'])
        self.assertDictEqual(actual_args.__dict__, expected_args)
        self.assertDictEqual({'v': '-1'}, actual_kwargs)

        test_args_incorrect = test_args + ['-v', 'value']
        self.assertRaises(RuntimeError, cc.fetch_args_kwargs, parser,
                          test_args_incorrect)

        test_args_incorrect = test_args + ['--v', 'v', '--odd']
        self.assertRaises(RuntimeError, cc.fetch_args_kwargs, parser,
                          test_args_incorrect)

    def test_get_required_params(self):

        class Fake1(FakeRuleClass):
            pass

        class Fake2(FakeRuleClass):
            pass

        actual = cc.get_required_params([(Fake1,), (Fake2,), (fake_rule_func,)])
        expected = {
            'project_id': ['Fake1', 'Fake2', 'fake_rule_func'],
            'dataset_id': ['Fake1', 'Fake2', 'fake_rule_func'],
            'sandbox_dataset_id': ['Fake1', 'Fake2', 'fake_rule_func']
        }
        self.assertDictEqual(expected, actual)

        class Fake3(FakeRuleClass):

            def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                         required_param_1):
                pass

        actual = cc.get_required_params([(Fake1,), (Fake3,), (fake_rule_func,)])
        expected = {
            'project_id': ['Fake1', 'Fake3', 'fake_rule_func'],
            'dataset_id': ['Fake1', 'Fake3', 'fake_rule_func'],
            'sandbox_dataset_id': ['Fake1', 'Fake3', 'fake_rule_func'],
            'required_param_1': ['Fake3']
        }
        self.assertDictEqual(expected, actual)

        class Fake4(FakeRuleClass):

            def __init__(self,
                         project_id,
                         dataset_id,
                         sandbox_dataset_id,
                         optional_param='default_value'):
                pass

        actual = cc.get_required_params([(Fake1,), (Fake4,), (fake_rule_func,)])
        expected = {
            'project_id': ['Fake1', 'Fake4', 'fake_rule_func'],
            'dataset_id': ['Fake1', 'Fake4', 'fake_rule_func'],
            'sandbox_dataset_id': ['Fake1', 'Fake4', 'fake_rule_func']
        }
        self.assertDictEqual(expected, actual)

        # a legacy rule with extra required param
        def fake_1(project_id, dataset_id, sandbox_dataset_id,
                   required_param_1):
            pass

        actual = cc.get_required_params([(Fake1,), (fake_1,),
                                         (fake_rule_func,)])
        expected = {
            'project_id': ['Fake1', 'fake_1', 'fake_rule_func'],
            'dataset_id': ['Fake1', 'fake_1', 'fake_rule_func'],
            'sandbox_dataset_id': ['Fake1', 'fake_1', 'fake_rule_func'],
            'required_param_1': ['fake_1']
        }
        self.assertDictEqual(expected, actual)

        def fake_2(project_id,
                   dataset_id,
                   sandbox_dataset_id,
                   required_param_1='optional'):
            pass

        actual = cc.get_required_params([(Fake1,), (fake_1,), (fake_2,)])
        expected = {
            'project_id': ['Fake1', 'fake_1', 'fake_2'],
            'dataset_id': ['Fake1', 'fake_1', 'fake_2'],
            'sandbox_dataset_id': ['Fake1', 'fake_1', 'fake_2'],
            'required_param_1': ['fake_1']
        }
        self.assertDictEqual(expected, actual)

    def test_get_missing_custom_params(self):

        class Fake1(FakeRuleClass):
            pass

        class Fake2(FakeRuleClass):
            pass

        actual = cc.get_missing_custom_params([(Fake1,), (Fake2,),
                                               (fake_rule_func,)])
        expected = dict()
        self.assertDictEqual(expected, actual)

        class Fake3(FakeRuleClass):

            def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                         required_param_1):
                pass

        actual = cc.get_missing_custom_params([(Fake1,), (Fake3,),
                                               (fake_rule_func,)])
        expected = {'required_param_1': ['Fake3']}
        self.assertDictEqual(expected, actual)

        class Fake4(FakeRuleClass):

            def __init__(self,
                         project_id,
                         dataset_id,
                         sandbox_dataset_id,
                         optional_param='default_value'):
                pass

        actual = cc.get_missing_custom_params([(Fake1,), (Fake4,),
                                               (fake_rule_func,)])
        expected = dict()
        self.assertDictEqual(expected, actual)

        # a legacy rule with extra required param
        def fake_1(project_id, dataset_id, sandbox_dataset_id,
                   required_param_1):
            pass

        actual = cc.get_missing_custom_params([(Fake1,), (fake_1,),
                                               (fake_rule_func,)])
        expected = {'required_param_1': ['fake_1']}
        self.assertDictEqual(expected, actual)

        def fake_2(project_id,
                   dataset_id,
                   sandbox_dataset_id,
                   required_param_1='optional'):
            pass

        actual = cc.get_missing_custom_params([(Fake1,), (fake_1,), (fake_2,)])
        expected = {'required_param_1': ['fake_1']}
        self.assertDictEqual(expected, actual)

    @patch('cdr_cleaner.clean_cdr.clean_engine.clean_dataset')
    @patch('cdr_cleaner.clean_cdr.clean_engine.get_query_list')
    @patch('cdr_cleaner.clean_cdr.validate_custom_params')
    @patch('cdr_cleaner.clean_cdr.fetch_args_kwargs')
    def test_clean_cdr(self, mock_fetch_args, mock_validate_args,
                       mock_get_query_list, mock_clean_dataset):

        from argparse import Namespace

        # For run_as
        cdr_sa = 'cdr_email@prod_env.com'

        # Test clean_dataset() function call
        args = [
            '-p', self.project_id, '-d', self.dataset_id, '-b',
            self.sandbox_dataset_id, '--data_stage', 'ehr', '--run_as', cdr_sa
        ]
        # creates argparse namespace return value
        expected_args = Namespace(
            **{
                'project_id': self.project_id,
                'dataset_id': self.dataset_id,
                'sandbox_dataset_id': self.sandbox_dataset_id,
                'data_stage': DataStage.EHR,
                'console_log': False,
                'list_queries': False,
                'run_as': cdr_sa
            })

        expected_kargs = {}
        mock_fetch_args.return_value = expected_args, expected_kargs

        rules = cc.DATA_STAGE_RULES_MAPPING['ehr']

        cc.main(args)

        mock_validate_args.assert_called_once_with(rules, **expected_kargs)

        mock_clean_dataset.assert_called_once_with(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            rules=rules,
            table_namer=DataStage.EHR.value,
            run_as=cdr_sa,
            run_synthetic=False)

        # Test get_queries() function call
        args = [
            '-p', self.project_id, '-d', self.dataset_id, '-b',
            self.sandbox_dataset_id, '--data_stage', 'ehr', '--list_queries',
            True
        ]

        expected_args = Namespace(
            **{
                'project_id': self.project_id,
                'dataset_id': self.dataset_id,
                'sandbox_dataset_id': self.sandbox_dataset_id,
                'data_stage': DataStage.EHR,
                'console_log': False,
                'list_queries': True
            })

        expected_kargs = {}
        mock_fetch_args.return_value = expected_args, expected_kargs

        cc.main(args)

        mock_get_query_list.assert_called_once_with(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            rules=rules,
            table_namer=DataStage.EHR.value)
