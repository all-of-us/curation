"""
Integration test to ensure ppi responses are rounded to nearest integer

For the following observation_source_concept_ids, we have to make sure if the value_as_number field is an integer.
If it is not, It should be rounded to the nearest integer:

1585889
1585890
1585795
1585802
1585820
1585864
1585870
1585873
1586159
1586162
1333015
1333023

Original Issue: DC-538, DC-1276
"""

# Python Imports
import os

# Project Imports
from common import OBSERVATION
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.round_ppi_values_to_nearest_integer import RoundPpiValuesToNearestInteger
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RoundPpiValuesToNearestIntegerTest(BaseTest.CleaningRulesTestBase):

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
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.rule_instance = RoundPpiValuesToNearestInteger(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names
        for table_name in [OBSERVATION]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        # Create domain tables required for the test
        super().setUp()

        # Load the test data
        observation_data_template = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
            (
                observation_id, 
                person_id, 
                observation_concept_id, 
                observation_type_concept_id,
                value_as_concept_id,
                observation_source_concept_id,
                value_source_concept_id,
                qualifier_concept_id,
                unit_concept_id,
                observation_date,
                value_as_number
            )
            VALUES
              (1, 101, 0, 0, 0, 1585889, 0, 0, 0, '2020-01-01', 2.6),
              (2, 102, 0, 0, 0, 1585890, 0, 0, 0, '2020-01-01', 10.2),
              (3, 103, 0, 0, 0, 1585795, 0, 0, 0, '2020-01-01', 8.1),
              (4, 104, 0, 0, 0, 1585802, 0, 0, 0, '2020-01-01', 0),
              (5, 105, 0, 0, 0, 1585820, 0, 0, 0, '2020-01-01', 4.3),
              (6, 106, 0, 0, 0, 1585864, 0, 0, 0, '2020-01-01', 5.5),
              (7, 107, 0, 0, 0, 1585870, 0, 0, 0, '2020-01-01', 12.0),
              (8, 108, 0, 0, 0, 1585873, 0, 0, 0, '2020-01-01', 3),
              (9, 109, 0, 0, 0, 1586159, 0, 0, 0, '2020-01-01', 0.0),
              (10, 110, 0, 0, 0, 1586162, 0, 0, 0, '2020-01-01', 1.1),
              (11, 111, 0, 0, 0, 1333015, 0, 0, 0, '2020-01-01', 6.7),
              (12, 112, 0, 0, 0, 1333023, 0, 0, 0, '2020-01-01', 8.9)
              
            """)

        insert_observation_query = observation_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{insert_observation_query};'])

    def test_round_ppi_values_to_nearest_integer(self):
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.observation',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'fields': [
                'observation_id', 'person_id', 'observation_source_concept_id',
                'value_as_number'
            ],
            'cleaned_values': [(1, 101, 1585889, 3), (2, 102, 1585890, 10),
                               (3, 103, 1585795, 8), (4, 104, 1585802, 0),
                               (5, 105, 1585820, 4), (6, 106, 1585864, 6),
                               (7, 107, 1585870, 12), (8, 108, 1585873, 3),
                               (9, 109, 1586159, 0), (10, 110, 1586162, 1),
                               (11, 111, 1333015, 7), (12, 112, 1333023, 9)]
        }]

        self.default_test(tables_and_counts)
