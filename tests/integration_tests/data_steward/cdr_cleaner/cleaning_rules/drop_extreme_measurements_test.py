"""
Integration test for drop_extreme_measurements mmodule

DC-1211
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, MEASUREMENT
from cdr_cleaner.cleaning_rules.drop_extreme_measurements import DropExtremeMeasurements
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

EXTREME_MEASUREMENTS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.measurement`
(measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime,
measurement_time, measurement_type_concept_id, operator_concept_id, value_as_number, value_as_concept_id,
unit_concept_id, range_low, range_high, provider_id, visit_occurrence_id,
visit_detail_id, measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value)

VALUES
    --FIRST SUBQUERY
    (NULL, 1, 903133, NULL, TIMESTAMP('2009-04-29'), 
    NULL, NULL, NULL, 19, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL),

    (NULL, 2, 903133, NULL, TIMESTAMP('2009-04-29'), 
    NULL, NULL, NULL, 230, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL),

    (NULL, 3, 903133, NULL, NULL, 
    NULL, NULL, NULL, 250, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL)

    --SECOND SUBQUERY
    (NULL, 4, 903124, NULL, TIMESTAMP('2009-04-29'), 
    NULL, NULL, NULL, 100, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL)

    (NULL, 5, 903124, NULL, TIMESTAMP('2010-07-13'), 
    NULL, NULL, NULL, 100, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL)

    --THIRD SUBQUERY
    (NULL, 6, 903133, NULL, TIMESTAMP('2011-08-21'), 
    NULL, NULL, NULL, 88, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL)
""")


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
