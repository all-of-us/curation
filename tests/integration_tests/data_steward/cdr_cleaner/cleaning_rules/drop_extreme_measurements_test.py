"""
Integration test for drop_extreme_measurements module

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
INSERT INTO `{{project_id}}.{{dataset_id}}.measurement`
(measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime,
measurement_type_concept_id, value_as_number, measurement_source_concept_id)
VALUES
    (100, 1, 903133, '2009-04-29', TIMESTAMP('2009-04-29'), 1000, 19, 903133),
    (200, 2, 903133, '2009-04-29', TIMESTAMP('2009-04-29'), 1000, 230, 903133),
    (400, 4, 903124, '2009-04-29', TIMESTAMP('2009-04-29'), 1000, 100, 903124),
    (500, 5, 903124, '2010-07-13', TIMESTAMP('2010-07-13'), 1000, 100, 903124),
    (600, 6, 903133, '2011-08-21', TIMESTAMP('2011-08-21'), 1000, 88, 903133),
    (300, 3, 903135, '2015-05-14', NULL, 1000, 250, 903135),
    (700, 7, 903121, '2009-04-29', TIMESTAMP('2009-04-29'), 1000, 10, 903121),
    (800, 8, 903124, '2009-04-29', TIMESTAMP('2009-04-29'), 1000, 10, 903124),
    (900, 9, 903121, '2010-04-10', TIMESTAMP('2010-04-10'), 1000, 50, 903121),
    (1000, 10, 903121, '2015-02-11', TIMESTAMP('2015-02-11'), 1000, 160, 903121),
    (1100, 11, 903124, '2014-02-11', TIMESTAMP('2014-02-11'), 1000, 100, 903124),
    (1200, 12, 903124, '2014-02-11', TIMESTAMP('2014-02-11'), 1000, 130, 903124),
    (1201, 12, 903124, '2014-02-12', TIMESTAMP('2014-02-12'), 1000, 20, 903124),
    (1202, 12, 903133, '2014-02-11', TIMESTAMP('2014-02-11'), 1000, 100, 903133),
    (1203, 12, 903133, '2014-02-12', TIMESTAMP('2014-02-12'), 1000, 100, 903133)
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
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200,
                1201, 1202, 1203
            ],
            'sandboxed_ids': [100, 200, 400, 600, 700, 800, 1200, 1202],
            'fields': [
                'measurement_id', 'person_id', 'measurement_concept_id',
                'measurement_date', 'measurement_datetime',
                'measurement_type_concept_id', 'value_as_number',
                'measurement_source_concept_id'
            ],
            'cleaned_values': [
                (300, 3, 903135, date.fromisoformat('2015-05-14'), None, 1000,
                 250, 903135),
                (500, 5, 903124, date.fromisoformat('2010-07-13'),
                 parser.parse('2010-07-13 00:00:00 UTC'), 1000, 100, 903124),
                (900, 9, 903121, date.fromisoformat('2010-04-10'),
                 parser.parse('2010-04-10 00:00:00 UTC'), 1000, 50, 903121),
                (1000, 10, 903121, date.fromisoformat('2015-02-11'),
                 parser.parse('2015-02-11 00:00:00 UTC'), 1000, 160, 903121),
                (1100, 11, 903124, date.fromisoformat('2014-02-11'),
                 parser.parse('2014-02-11 00:00:00 UTC'), 1000, 100, 903124),
                (1201, 12, 903124, date.fromisoformat('2014-02-12'),
                 parser.parse('2014-02-12 00:00:00 UTC'), 1000, 20, 903124),
                (1203, 12, 903133, date.fromisoformat('2014-02-12'),
                 parser.parse('2014-02-12 00:00:00 UTC'), 1000, 100, 903133)
            ]
        }]

        self.default_test(tables_and_counts)
