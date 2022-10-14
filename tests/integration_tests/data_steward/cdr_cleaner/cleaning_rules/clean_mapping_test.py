"""
Integration test for clean_mapping mmodule

DC-1528
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.clean_mapping import CleanMappingExtTables
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CleanMappingExtTablesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # Instantiate class
        cls.rule_instance = CleanMappingExtTables(cls.project_id,
                                                  cls.dataset_id,
                                                  cls.sandbox_id)

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store {} table name(s)
        affected_tables = cls.rule_instance.affected_tables
        for table_name in affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """
        super().setUp()

    def test_field_cleaning(self):
        """
        test
        """
        tables_and_counts = [{}]
        self.default_test(tables_and_counts)
