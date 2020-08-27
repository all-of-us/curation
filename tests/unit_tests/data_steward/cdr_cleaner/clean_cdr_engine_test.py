# Python imports
import inspect
from unittest import TestCase, mock

# Project imports
from cdr_cleaner import clean_cdr_engine as ce
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import CleanPPINumericFieldsUsingParameters
from constants.cdr_cleaner import clean_cdr as cdr_consts


class CleanCDREngineTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):

        self.project = 'test-project'
        self.dataset_id = 'test-dataset'
        self.sandbox_dataset_id = 'test-dataset_sandbox'

    def test_infer_rule(self):
        clazz = CleanPPINumericFieldsUsingParameters
        _, _, rule_info = ce.infer_rule(clazz, self.project, self.dataset_id,
                                        self.sandbox_dataset_id)
        self.assertTrue(
            inspect.ismethod(rule_info.pop(cdr_consts.QUERY_FUNCTION)))
        self.assertTrue(
            inspect.ismethod(rule_info.pop(cdr_consts.SETUP_FUNCTION)))
        expected_query_fn = CleanPPINumericFieldsUsingParameters.get_query_specs
        expected_rule_info = {
            cdr_consts.FUNCTION_NAME:
                expected_query_fn.__name__,
            cdr_consts.MODULE_NAME:
                inspect.getmodule(expected_query_fn).__name__,
            cdr_consts.LINE_NO:
                inspect.getsourcelines(expected_query_fn)[1]
        }
        self.assertDictEqual(rule_info, expected_rule_info)

        clazz = update_family_history.get_update_family_history_qa_queries
        _, _, rule_info = ce.infer_rule(clazz, self.project, self.dataset_id,
                                        self.sandbox_dataset_id)
        self.assertTrue(
            inspect.isfunction(rule_info.pop(cdr_consts.QUERY_FUNCTION)))
        self.assertTrue(
            inspect.isfunction(rule_info.pop(cdr_consts.SETUP_FUNCTION)))
        expected_query_fn = update_family_history.get_update_family_history_qa_queries
        expected_rule_info = {
            cdr_consts.FUNCTION_NAME:
                expected_query_fn.__name__,
            cdr_consts.MODULE_NAME:
                inspect.getmodule(expected_query_fn).__name__,
            cdr_consts.LINE_NO:
                inspect.getsourcelines(expected_query_fn)[1]
        }
        self.assertCountEqual(rule_info, expected_rule_info)

        query_function, _, rule_info = ce.infer_rule(
            update_family_history.get_update_family_history_qa_queries,
            self.project, self.dataset_id, self.sandbox_dataset_id)

        # ensure closure works
        project_id = 'incorrect_proj'
        dataset_id = 'incorrect_ds'
        query_list = query_function()
        for query in query_list:
            self.assertIn(self.project, query['query'])
            self.assertIn(self.dataset_id, query['query'])
            self.assertNotIn(project_id, query['query'])
            self.assertNotIn(dataset_id, query['query'])

    @mock.patch.object(CleanPPINumericFieldsUsingParameters, 'setup_rule')
    def test_query_list(self, patched_ppi_setup):
        queries = ce.get_query_list(
            project_id=self.project,
            dataset_id=self.dataset_id,
            rules=[(CleanPPINumericFieldsUsingParameters,),
                   (update_family_history.get_update_family_history_qa_queries,)
                  ])
        for query in queries:
            self.assertIsNotNone(query['query'])
        patched_ppi_setup.assert_not_called()
