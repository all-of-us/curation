"""
Integration test for remove_invalid_procedure_source_records module

Original Issues: DC-1210
"""

#Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, PROCEDURE_OCCURRENCE
from cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records import RemoveInvalidProcedureSourceRecords
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RemoveInvalidProcedureSourceRecordsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # Instantiate class
        cls.rule_instance = RemoveInvalidProcedureSourceRecords(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store procedure_occurrence table name
        procedure_occurance_table_name = f'{cls.project_id}.{cls.dataset_id}.{PROCEDURE_OCCURRENCE}'
        cls.fq_table_names = [procedure_occurance_table_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()