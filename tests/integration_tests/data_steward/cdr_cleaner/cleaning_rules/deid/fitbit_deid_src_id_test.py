"""
Original Issues: DC-3337
"""

# Project Imports
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class FitbitDeidSrcIDTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        pass

    def test_field_cleaning(self):
        pass
