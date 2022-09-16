"""
 Integration test for generalize_sex_gender_concepts.py module

 Original Issues: DC-1224
 """

# Python Imports
import os
from datetime import date, datetime

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION
from cdr_cleaner.cleaning_rules.generalize_sex_gender_concepts import GeneralizeSexGenderConcepts
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

GENERALIZED_CONCEPT_ID_TEST_QUERY_TEMPLATE = JINJA_ENV.from_string("""

""")


class GeneralizeSexGenderConceptsTest(BaseTest.CleaningRulesTestBase):

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
        cls.rule_instance = GeneralizeSexGenderConcepts(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
        )

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store observation table name
        observation_table_name = f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}'
        cls.fq_table_names = [observation_table_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """
        super().setUp()

        # Query to insert test records into sleep_level table
        generalized_concept_query = GENERALIZED_CONCEPT_ID_TEST_QUERY_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        #Load test data
        self.load_test_data([generalized_concept_query])

    def test_field_cleaning(self):
        """
        """
        pass