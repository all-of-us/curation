"""
Integration test for clean_height_weight
"""

# Python Imports
import os

# Third party imports
from google.api_core.exceptions import ClientError

# Project imports
import bq_utils
import constants.cdr_cleaner.clean_cdr as cdr_consts
from app_identity import PROJECT_ID
from cdr_cleaner.clean_cdr_engine import generate_job_config
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CleanHeightWeightTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = bq_utils.get_combined_deid_dataset_id()
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        rule_instance = CleanHeightAndWeight(project_id, dataset_id, sandbox_id)
        cls.rule_instance = rule_instance
        for table_name in rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')
        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.person',
            f'{project_id}.{dataset_id}.measurement',
            f'{project_id}.{dataset_id}.measurement_ext',
            f'{project_id}.{dataset_id}.condition_occurrence',
            f'{project_id}.{dataset_id}.concept',
            f'{project_id}.{dataset_id}.concept_ancestor',
        ]
        super().setUpClass()

    def setUp(self):
        super().setUp()

    def test_get_query_specs(self):
        """
        Tests that queries run successfully and sandbox tables are generated
        Note: This does NOT validate logic 
        """
        # Ensure the rule generates syntactically correct queries.
        for spec in self.rule_instance.get_query_specs():
            job_config = generate_job_config(self.project_id, spec)
            query = spec.get(cdr_consts.QUERY)
            try:
                self.client.query(query, job_config).result()
            except ClientError as e:
                self.fail(
                    f"The following client error was raised likely due to incorrect query syntax: "
                    f"{e.message}")
        # Ensure the sandbox tables were created
        table_ids = [
            table.table_id for table in self.client.list_tables(self.sandbox_id)
        ]
        for sandbox_table in self.rule_instance.get_sandbox_tablenames():
            self.assertIn(sandbox_table, table_ids)
