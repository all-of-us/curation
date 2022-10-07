"""
Integration test for drop_extreme_measurements mmodule

DC-1211
"""

# Python Imports
import os


class DropExtremeMeasurementsTest(BaseTest.CleaningRulesTestBase):

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
        cls.rule_instance = DropExtremeMeasurements(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
        )

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store measurement table name
        measurement_table_name = f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}'
        cls.fq_table_names = [measurement_table_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """
        super().setUp()
        pass

    def test_field_cleaning(self):
        """
        """
        pass
