# Python imports
import inspect
from unittest import TestCase, mock

# Third party imports
from googleapiclient.errors import HttpError

# Project imports
import bq_utils
from cdr_cleaner import clean_cdr_engine as ce
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import CleanPPINumericFieldsUsingParameters
from constants.cdr_cleaner import clean_cdr as cdr_consts
from constants.cdr_cleaner.clean_cdr_engine import FAILURE_MESSAGE_TEMPLATE
from constants import bq_utils as bq_consts


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
        self.dry_run = False
        self.statement_one = {
            cdr_consts.QUERY: 'query one',
            cdr_consts.DESTINATION_TABLE: 'dest_table_one',
            cdr_consts.DISPOSITION: bq_consts.WRITE_APPEND,
            cdr_consts.DESTINATION_DATASET: 'dest_dataset_one',
            cdr_consts.MODULE_NAME: 'cdr_cleaner.module_name_one',
            cdr_consts.FUNCTION_NAME: 'test_function_one',
            cdr_consts.LINE_NO: 1
        }

        self.statement_two = {
            cdr_consts.QUERY: 'query two',
            cdr_consts.DESTINATION_TABLE: 'dest_table_two',
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET: 'dest_dataset_two',
            cdr_consts.MODULE_NAME: 'cdr_cleaner.module_name_two',
            cdr_consts.FUNCTION_NAME: 'test_function_two',
            cdr_consts.LINE_NO: 2
        }

        self.statements = [self.statement_one, self.statement_two]
        self.exception_statement_one = b'an error has occurred!'

        self.job_id_success = 1
        self.job_results_success = {
            'jobReference': {
                'jobId': self.job_id_success
            }
        }

        self.job_id_failure = 2
        self.job_results_failure = {
            'jobReference': {
                'jobId': self.job_id_failure
            }
        }

    @mock.patch('cdr_cleaner.clean_cdr_engine.format_failure_message')
    @mock.patch('bq_utils.job_status_errored')
    @mock.patch('bq_utils.wait_on_jobs')
    @mock.patch('bq_utils.query')
    def test_clean_dataset(self, mock_bq_utils, mock_wait_on_jobs,
                           mock_job_status_errored,
                           mock_format_failure_message):

        mock_bq_utils.side_effect = [
            self.job_results_success, self.job_results_failure
        ]
        mock_wait_on_jobs.return_value = []
        mock_job_status_errored.side_effect = [(False, None),
                                               (True,
                                                self.exception_statement_one)]

        ce.clean_dataset(self.project, self.statements)

        self.assertEqual(mock_bq_utils.call_count, len(self.statements))
        self.assertEqual(mock_format_failure_message.call_count, 1)

        mock_bq_utils.assert_any_call(
            self.statement_one.get(cdr_consts.QUERY),
            use_legacy_sql=False,
            destination_table_id=self.statement_one.get(
                cdr_consts.DESTINATION_TABLE),
            retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
            write_disposition=self.statement_one.get(cdr_consts.DISPOSITION),
            destination_dataset_id=self.statement_one.get(
                cdr_consts.DESTINATION_DATASET),
            batch=None)

        mock_bq_utils.assert_any_call(
            self.statement_two.get(cdr_consts.QUERY),
            use_legacy_sql=False,
            destination_table_id=self.statement_two.get(
                cdr_consts.DESTINATION_TABLE),
            retry_count=bq_consts.BQ_DEFAULT_RETRY_COUNT,
            write_disposition=self.statement_two.get(cdr_consts.DISPOSITION),
            destination_dataset_id=self.statement_two.get(
                cdr_consts.DESTINATION_DATASET),
            batch=None)

    @mock.patch('cdr_cleaner.clean_cdr_engine.format_failure_message')
    @mock.patch('bq_utils.job_status_errored')
    @mock.patch('bq_utils.wait_on_jobs')
    @mock.patch('bq_utils.query')
    def test_clean_dataset_exceptions(self, mock_bq_utils, mock_wait_on_jobs,
                                      mock_job_status_errored,
                                      mock_format_failure_message):

        # Test the case where BigQuery throws an error
        mock_bq_utils.side_effect = HttpError(
            mock.Mock(return_value={'status': 404}),
            self.exception_statement_one)

        ce.clean_dataset(self.project, self.statements)

        self.assertEqual(mock_wait_on_jobs.call_count, 0)
        self.assertEqual(mock_job_status_errored.call_count, 0)
        self.assertEqual(mock_format_failure_message.call_count, 2)

        # Test the case where there is an incomplete job
        mock_bq_utils.reset_mock()
        mock_format_failure_message.reset_mock()

        mock_bq_utils.side_effect = [self.job_results_success]
        mock_wait_on_jobs.return_value = [self.job_id_success]

        with self.assertRaises(bq_utils.BigQueryJobWaitError):
            ce.clean_dataset(self.project, self.statements)

    def test_infer_rule(self):
        clazz = CleanPPINumericFieldsUsingParameters
        query_fn, setup_fn, rule_info = ce.infer_rule(clazz, self.project,
                                                      self.dataset_id,
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
        query_fn, setup_fn, rule_info = ce.infer_rule(clazz, self.project,
                                                      self.dataset_id,
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

    def test_query_list(self):
        queries = ce.get_query_list(
            project_id=self.project,
            dataset_id=self.dataset_id,
            rules=[(CleanPPINumericFieldsUsingParameters,),
                   (update_family_history.get_update_family_history_qa_queries,)
                  ])
        query_from_closure = queries[-1]
        self.assertIn(self.project, query_from_closure['query'])
        self.assertIn(self.dataset_id, query_from_closure['query'])

    def test_format_failure_message(self):

        expected_failure_message = FAILURE_MESSAGE_TEMPLATE.render(
            module_name=self.statement_one.get(cdr_consts.MODULE_NAME),
            function_name=self.statement_one.get(cdr_consts.FUNCTION_NAME),
            line_no=self.statement_one.get(cdr_consts.LINE_NO),
            project_id=self.project,
            destination_dataset_id=self.statement_one.get(
                cdr_consts.DESTINATION_DATASET),
            destination_table=self.statement_one.get(
                cdr_consts.DESTINATION_TABLE),
            disposition=self.statement_one.get(cdr_consts.DISPOSITION),
            query=self.statement_one.get(cdr_consts.QUERY),
            exception=self.exception_statement_one)

        actual_failure_message = ce.format_failure_message(
            self.project, self.statement_one, self.exception_statement_one)

        self.assertEqual(expected_failure_message, actual_failure_message)
