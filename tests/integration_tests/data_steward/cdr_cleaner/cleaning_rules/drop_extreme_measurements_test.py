"""
Integration test for drop_extreme_measurements mmodule

DC-1211
"""

# Python Imports
import os
from datetime import date

# Third party imports
from dateutil import parser

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

    (100, 1, 903133, '2009-04-29', TIMESTAMP('2009-04-29'), 
    NULL, 1000, NULL, 19, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL),

    (200, 2, 903133, '2009-04-29', TIMESTAMP('2009-04-29'), 
    NULL, 1000, NULL, 230, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL),

    (400, 4, 903124, '2009-04-29', TIMESTAMP('2009-04-29'), 
    NULL, 1000, NULL, 100, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL),

    (500, 5, 903124, '2010-07-13', TIMESTAMP('2010-07-13'), 
    NULL, 1000, NULL, 100, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL),
 
    (600, 6, 903133, '2011-08-21', TIMESTAMP('2011-08-21'), 
    NULL, 1000, NULL, 88, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903133, NULL, NULL),

    (300, 3, 903135, '2015-05-14', NULL, 
    NULL, 1000, NULL, 250, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903135, NULL, NULL),

    (700, 7, 903121, '2009-04-29', TIMESTAMP('2009-04-29'), 
    NULL, 1000, NULL, 10, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903121, NULL, NULL),

    (800, 8, 903124, '2009-04-29', TIMESTAMP('2009-04-29'), 
    NULL, 1000, NULL, 10, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL),

    (900, 9, 903121, '2010-04-10', TIMESTAMP('2010-04-10'), 
    NULL, 1000, NULL, 50, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903121, NULL, NULL),

    (1000, 10, 903121, '2015-02-11', TIMESTAMP('2015-02-11'), 
    NULL, 1000, NULL, 160, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903121, NULL, NULL),

    (1100, 11, 903124, '2014-02-11', TIMESTAMP('2014-02-11'), 
    NULL, 1000, NULL, 100, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL),

    (1200, 12, 903124, '2014-02-11', TIMESTAMP('2014-02-11'), 
    NULL, 1000, NULL, 130, NULL, 
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, 903124, NULL, NULL)
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

        # Query to insert test records into measurement table
        extreme_measurement_template = EXTREME_MEASUREMENTS_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([extreme_measurement_template])

    def test_field_cleaning(self):
        """
        person_ids 1,2,4,6,7,8, and 12 are sandboxed
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200
            ],
            'sandboxed_ids': [100, 200, 400, 600, 700, 800, 1200],
            'fields': [
                'measurement_id', 'person_id', 'measurement_concept_id',
                'measurement_date', 'measurement_datetime', 'measurement_time',
                'measurement_type_concept_id', 'operator_concept_id',
                'value_as_number', 'value_as_concept_id', 'unit_concept_id',
                'range_low', 'range_high', 'provider_id', 'visit_occurrence_id',
                'visit_detail_id', 'measurement_source_value',
                'measurement_source_concept_id', 'unit_source_value',
                'value_source_value'
            ],
            'cleaned_values': [
                (300, 3, 903135, date.fromisoformat('2015-05-14'), None, None,
                 1000, None, 250, None, None, None, None, None, None, None,
                 None, 903135, None, None),
                (500, 5, 903124, date.fromisoformat('2010-07-13'),
                 parser.parse('2010-07-13 00:00:00 UTC'), None, 1000, None, 100,
                 None, None, None, None, None, None, None, None, 903124, None,
                 None),
                (900, 9, 903121, date.fromisoformat('2010-04-10'),
                 parser.parse('2010-04-10 00:00:00 UTC'), None, 1000, None, 50,
                 None, None, None, None, None, None, None, None, 903121, None,
                 None),
                (1000, 10, 903121, date.fromisoformat('2015-02-11'),
                 parser.parse('2015-02-11 00:00:00 UTC'), None, 1000, None, 160,
                 None, None, None, None, None, None, None, None, 903121, None,
                 None),
                (1100, 11, 903124, date.fromisoformat('2014-02-11'),
                 parser.parse('2014-02-11 00:00:00 UTC'), None, 1000, None, 100,
                 None, None, None, None, None, None, None, None, 903124, None,
                 None)
            ]
        }]

        self.default_test(tables_and_counts)
