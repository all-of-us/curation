"""
Integration test for truncate_era_tables module

Original Issue: DC-2786
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.truncate_era_tables import TruncateEraTables
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class TruncateEraTablesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)