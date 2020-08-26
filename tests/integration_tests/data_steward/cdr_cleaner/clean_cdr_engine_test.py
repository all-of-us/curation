# Python imports
import os
from unittest import TestCase

# Third party imports
from google.cloud.bigquery import LoadJobConfig, TableReference

# Project imports
import sandbox
from tests import test_util
from utils import bq
from app_identity import get_application_id
import cdr_cleaner.clean_cdr_engine as ce
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import CleanPPINumericFieldsUsingParameters


class CleanCDREngineTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        def setUp(self):
        self.project_id = get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.client = bq.get_client(self.project_id)
        self.delete_sandbox()

    def test_clean_dataset(self):
        fake_rule_class_query = 'SELECT "FakeRuleClass"'
        fake_rule_func_query = 'SELECT "fake_rule_func"'

        class FakeRuleClass(BaseCleaningRule):

            def __init__(self, project_id, dataset_id, sandbox_dataset_id):
                super().__init__(issue_numbers=[''],
                                 description='',
                                 affected_datasets=[cdr_consts.UNIONED],
                                 affected_tables=[],
                                 project_id=project_id,
                                 dataset_id=dataset_id,
                                 sandbox_dataset_id=sandbox_dataset_id)

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

        def fake_rule_func(project_id, dataset_id):
            return [{cdr_consts.QUERY: fake_rule_func_query}]

        # get_query_list returns query dicts for class- and func-based rules
        # (theoretically this could be a unit test)
        expected_queries = {fake_rule_class_query, fake_rule_func_query}
        query_dicts = ce.get_query_list(project_id=self.project_id,
                                        dataset_id=self.dataset_id,
                                        rules=[(FakeRuleClass,),
                                               (fake_rule_func,)])
        actual_queries = set(
            query_dict[cdr_consts.QUERY] for query_dict in query_dicts)
        self.assertSetEqual(expected_queries, actual_queries)

        # clean_dataset returns jobs associated with all rules' queries
        jobs = ce.clean_dataset_v1(project_id=self.project_id,
                                   dataset_id=self.dataset_id,
                                   rules=[(FakeRuleClass,), (fake_rule_func,)])
        actual_job_queries = set(job.query for job in jobs)
        self.assertEqual(expected_queries, actual_job_queries)
        # the jobs are completed
        self.assertTrue(all(job.state == 'DONE' for job in jobs))
        self.assertTrue(all(job.ended for job in jobs))
        self.delete_sandbox()

        # specific errors associated with failed queries are raised
        fake_rule_func_err_query = f'SELECT 1 FROM {self.dataset_id}.test_not_found'

        def fake_rule_func_err(project_id, dataset_id):
            return [{cdr_consts.QUERY: fake_rule_func_err_query}]

        with self.assertRaises(NotFound) as c:
            ce.clean_dataset_v1(project_id=self.project_id,
                                dataset_id=self.dataset_id,
                                rules=[(FakeRuleClass,), (fake_rule_func_err,)])
        self.assertEqual(c.exception.query_job.query, fake_rule_func_err_query)

    def delete_sandbox(self):
        sandbox_dataset_id = sandbox.get_sandbox_dataset_id(self.dataset_id)
        self.client.delete_dataset(sandbox_dataset_id,
                                   delete_contents=True,
                                   not_found_ok=True)

    def tearDown(self) -> None:
        self.delete_sandbox()
