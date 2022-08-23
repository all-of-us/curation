# Python imports
import inspect
from unittest import TestCase

# Project imports
from cdr_cleaner import clean_cdr_engine as ce
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

fake_rule_class_query = 'SELECT "FakeRuleClass"'
fake_rule_func_query = 'SELECT "fake_rule_func"'


class FakeRuleClass(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        super().__init__(
            issue_numbers=[''],
            description='',
            affected_datasets=[cdr_consts.UNIONED],
            affected_tables=[],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
        )

    def get_sandbox_tablenames(self):
        pass

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def get_query_specs(self, *args, **keyword_args):
        return [{cdr_consts.QUERY: fake_rule_class_query}]

    def validate_rule(self, client, *args, **keyword_args):
        pass


def fake_rule_func(project_id, dataset_id, sandbox_dataset_id='hello'):
    return [{cdr_consts.QUERY: fake_rule_func_query}]


class CleanCDREngineTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project = 'test-project'
        self.dataset_id = 'test-dataset'
        self.sandbox_id = 'test-sandbox'
        self.table_namer = ''
        self.kwargs = {}

    def test_get_custom_kwargs(self):
        kwargs = {'combined_dataset_id': 'combined'}

        actual_kwargs = ce.get_custom_kwargs(FakeRuleClass, **kwargs)
        expected_kwargs = {}
        self.assertDictEqual(actual_kwargs, expected_kwargs)

        actual_kwargs = ce.get_custom_kwargs(fake_rule_func, **kwargs)
        expected_kwargs = {}
        self.assertDictEqual(actual_kwargs, expected_kwargs)

        incorrect_kwargs = {'mapping_dataset_id': 'mapping_dataset'}

        class FakeNewClass(FakeRuleClass):

            def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                         combined_dataset_id):
                super().__init__(project_id, dataset_id, sandbox_dataset_id)

        actual_kwargs = ce.get_custom_kwargs(FakeNewClass, **kwargs)
        self.assertEqual(kwargs, actual_kwargs)

        self.assertRaises(ValueError, ce.get_custom_kwargs, FakeNewClass,
                          **incorrect_kwargs)

        actual_kwargs = ce.get_custom_kwargs(FakeRuleClass)
        self.assertDictEqual({}, actual_kwargs)

        def rule_with_combined(project_id, dataset_id, sandbox_dataset_id,
                               combined_dataset_id):
            pass

        actual_kwargs = ce.get_custom_kwargs(rule_with_combined, **kwargs)
        self.assertDictEqual(kwargs, actual_kwargs)

    def test_infer_rule(self):
        clazz = FakeRuleClass
        _, _, rule_info = ce.infer_rule(clazz,
                                        self.project,
                                        self.dataset_id,
                                        self.sandbox_id,
                                        self.table_namer,
                                        run_synthetic=False,
                                        **self.kwargs)
        self.assertTrue(
            inspect.ismethod(rule_info.pop(cdr_consts.QUERY_FUNCTION)))
        self.assertTrue(
            inspect.ismethod(rule_info.pop(cdr_consts.SETUP_FUNCTION)))
        expected_query_fn = FakeRuleClass.get_query_specs
        expected_rule_info = {
            cdr_consts.FUNCTION_NAME:
                expected_query_fn.__name__,
            cdr_consts.MODULE_NAME:
                inspect.getmodule(expected_query_fn).__name__,
            cdr_consts.LINE_NO:
                inspect.getsourcelines(expected_query_fn)[1]
        }
        self.assertDictEqual(rule_info, expected_rule_info)

        clazz = fake_rule_func
        _, _, rule_info = ce.infer_rule(clazz, self.project, self.dataset_id,
                                        self.sandbox_id, self.table_namer,
                                        **self.kwargs)
        self.assertTrue(
            inspect.isfunction(rule_info.pop(cdr_consts.QUERY_FUNCTION)))
        self.assertTrue(
            inspect.isfunction(rule_info.pop(cdr_consts.SETUP_FUNCTION)))
        expected_query_fn = fake_rule_func
        expected_rule_info = {
            cdr_consts.FUNCTION_NAME:
                expected_query_fn.__name__,
            cdr_consts.MODULE_NAME:
                inspect.getmodule(expected_query_fn).__name__,
            cdr_consts.LINE_NO:
                inspect.getsourcelines(expected_query_fn)[1]
        }
        self.assertDictEqual(rule_info, expected_rule_info)

    def test_query_list(self):
        actual_queries = ce.get_query_list(project_id=self.project,
                                           dataset_id=self.dataset_id,
                                           sandbox_dataset_id=self.sandbox_id,
                                           rules=[(FakeRuleClass,),
                                                  (fake_rule_func,)])
        expected_queries = [{
            'query': fake_rule_class_query
        }, {
            'query': fake_rule_func_query
        }]
        self.assertListEqual(actual_queries, expected_queries)

    def test_get_rule_args(self):
        expected_param_names = [
            'project_id', 'dataset_id', 'sandbox_dataset_id'
        ]
        actual_rule_args = ce.get_rule_args(fake_rule_func)
        actual_param_names = [arg['name'] for arg in actual_rule_args]
        self.assertListEqual(expected_param_names, actual_param_names)
