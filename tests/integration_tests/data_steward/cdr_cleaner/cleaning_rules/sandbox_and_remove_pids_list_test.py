"""
Integration test for SandboxAndRemovePidsList module
"""

# Project Imports
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import JINJA_ENV


class SandboxAndRemovePidsListTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()
        pass

    def setUp(self):
        pass

    def test_sandbox_and_remove_pids_list(self):
        pass
