"""
Integration Test for the clean_height_weight module.

Normalizes all height and weight data into cm and kg and removes invalid/implausible data points (rows)

Original Issue: DC-701

The intent is to delete zero/null/implausible height/weight rows and inserting normalized rows (cm and kg)
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CleanHeightAndWeightTest(BaseTest.CleaningRulesTestBase):

    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        sandbox_id = dataset_id + '_sandbox'

        cls.query_class = CleanHeightAndWeight(project_id, dataset_id,
                                               sandbox_id)

        cls.fq_table_names = [f'{project_id}.{dataset_id}.measurement',
                             f'{project_id}.{dataset_id}.concept',
                             f'{project_id}.{dataset_id}.person',
                             f'{project_id}.{dataset_id}.measurement_ext',
                             f'{project_id}.{dataset_id}.occurrence',
                             f'{project_id}.{dataset_id}.concept_ancestor']

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

    def test_field_cleaning(self):
        """
        Tests that the specifications for all the queries perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        measurement_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.measurement`
            (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id)
            VALUES
                (""")

