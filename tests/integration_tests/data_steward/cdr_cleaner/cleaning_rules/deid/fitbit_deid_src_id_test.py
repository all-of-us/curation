"""
Integration test for the fitbit_deid_src_id module. 
Update src_id values in fitbit tables with src_id values from the site_maskings table.

Original Issues: DC-3337
"""

# Python Imports
import os
from unittest import mock
from datetime import datetime

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from common import (ACTIVITY_SUMMARY, DEVICE, FITBIT_TABLES,
                    HEART_RATE_INTRADAY, HEART_RATE_SUMMARY, JINJA_ENV,
                    SITE_MASKING_TABLE_ID, SLEEP_DAILY_SUMMARY, SLEEP_LEVEL,
                    STEPS_INTRADAY)
from cdr_cleaner.cleaning_rules.deid.fitbit_deid_src_id import FitbitDeidSrcID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

TEMPLATE_DICT = {
    ACTIVITY_SUMMARY:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, activity_calories, date, src_id)
VALUES
    (1234, 100, date('2020-08-17'), 'pt'),
    (5678, 200, date('2020-08-17'), 'tp'),
    (2345, 500, date('2020-08-17'), 'pt'),
    (6789, 800, date('2020-08-17'), 'tp'),
    (3456, 1000, date('2020-08-17'), 'pt')
"""),
    HEART_RATE_INTRADAY:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, heart_rate_value, datetime, src_id)
VALUES
    (1234, 60, (DATETIME '2020-08-17 15:00:00'), 'pt'),
    (5678, 50, (DATETIME '2020-08-17 15:30:00'), 'tp'),
    (2345, 55, (DATETIME '2020-08-17 16:00:00'), 'pt'),
    (6789, 40, (DATETIME '2020-08-17 16:30:00'), 'tp'),
    (3456, 65, (DATETIME '2020-08-17 17:00:00'), 'pt')
"""),
    HEART_RATE_SUMMARY:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, date, calorie_count, src_id)
VALUES
    (1234, date('2020-08-17'), 100, 'pt'),
    (5678, date('2020-08-17'), 200, 'tp'),
    (2345, date('2020-08-17'), 500, 'pt'),
    (6789, date('2020-08-17'), 800, 'tp'),
    (3456, date('2020-08-17'), 1000, 'pt')
"""),
    STEPS_INTRADAY:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, steps, datetime, src_id)
VALUES
    (1234, 60, (DATETIME '2020-08-17 15:00:00'), 'pt'),
    (5678, 50, (DATETIME '2020-08-17 15:30:00'), 'tp'),
    (2345, 55, (DATETIME '2020-08-17 16:00:00'), 'pt'),
    (6789, 40, (DATETIME '2020-08-17 16:30:00'), 'tp'),
    (3456, 65, (DATETIME '2020-08-17 17:00:00'), 'pt')
"""),
    SLEEP_DAILY_SUMMARY:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, sleep_date, minute_in_bed, src_id)
VALUES
    (1234, date('2020-08-17'), 502, 'pt'),
    (5678, date('2020-08-17'), 443, 'tp'),
    (2345, date('2020-08-17'), 745, 'pt'),
    (6789, date('2020-08-17'), 605, 'tp'),
    (3456, date('2020-08-17'), 578, 'pt')
"""),
    SLEEP_LEVEL:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, sleep_date, duration_in_min, src_id)
VALUES
    (1234, date('2020-08-17'), 42, 'pt'),
    (5678, date('2020-08-17'), 15, 'tp'),
    (2345, date('2020-08-17'), 22, 'pt'),
    (6789, date('2020-08-17'), 56, 'tp'),
    (3456, date('2020-08-17'), 12, 'pt')
"""),
    DEVICE:
        JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, device_date, battery, src_id)
VALUES
    (1234, date('2020-08-17'), "Medium", 'pt'),
    (5678, date('2020-08-17'), "Medium", 'tp'),
    (2345, date('2020-08-17'), "Medium", 'pt'),
    (6789, date('2020-08-17'), "Medium", 'tp'),
    (3456, date('2020-08-17'), "Medium", 'pt')
""")
}

SITE_MASKINGS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{temp_site_maskings}}`
(hpo_id, src_id, state, value_source_concept_id)
VALUES
    ('tp', 'Participant Portal 1', 'state1', 1),
    ('pt', 'Participant Portal 2', 'state2', 2),
    ('rm', 'PPI/PM', 'state3', 3),
    ('aa', 'Site 1', 'state4', 4),
    ('bb', 'Site 2', 'state5', 5)
""")


class FitbitDeidSrcIDTest(BaseTest.CleaningRulesTestBase):

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

        # Instantiate class
        cls.rule_instance = FitbitDeidSrcID(cls.project_id, cls.dataset_id, '')

        # Generates list of fully qualified table names
        affected_table_names = cls.rule_instance.affected_tables
        for table_name in affected_table_names:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create tables and test data
        """
        super().setUp()

        # Create temp site_maskings table
        maskings_table = f'{self.project_id}.{self.dataset_id}.{SITE_MASKING_TABLE_ID}'
        schema = self.client.get_table_schema(SITE_MASKING_TABLE_ID)
        self.client.create_table(Table(maskings_table, schema), exists_ok=True)
        self.fq_table_names.append(maskings_table)

        # Insert temp masking records
        site_maskings_query = SITE_MASKINGS_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            temp_site_maskings=SITE_MASKING_TABLE_ID)

        # Insert test records into fitbit tables
        fitbit_test_queries = []
        for table in FITBIT_TABLES:
            template = TEMPLATE_DICT.get(table)
            if template:
                test_data_query = template.render(project_id=self.project_id,
                                                  dataset_id=self.dataset_id,
                                                  fitbit_table=table)
                fitbit_test_queries.append(test_data_query)

        # Load test data
        self.load_test_data([site_maskings_query] + fitbit_test_queries)

    def test_field_cleaning(self):
        """
        Test updated src_id values
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.dataset_id, ACTIVITY_SUMMARY]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'activity_calories', 'date', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, 100, datetime.fromisoformat('2020-08-17').date(),
                 'Participant Portal 2'),
                (5678, 200, datetime.fromisoformat('2020-08-17').date(),
                 'Participant Portal 1'),
                (2345, 500, datetime.fromisoformat('2020-08-17').date(),
                 'Participant Portal 2'),
                (6789, 800, datetime.fromisoformat('2020-08-17').date(),
                 'Participant Portal 1'),
                (3456, 1000, datetime.fromisoformat('2020-08-17').date(),
                 'Participant Portal 2')
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, HEART_RATE_INTRADAY]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'heart_rate_value', 'datetime', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, 60, datetime.fromisoformat('2020-08-17 15:00:00'),
                 'Participant Portal 2'),
                (5678, 50, datetime.fromisoformat('2020-08-17 15:30:00'),
                 'Participant Portal 1'),
                (2345, 55, datetime.fromisoformat('2020-08-17 16:00:00'),
                 'Participant Portal 2'),
                (6789, 40, datetime.fromisoformat('2020-08-17 16:30:00'),
                 'Participant Portal 1'),
                (3456, 65, datetime.fromisoformat('2020-08-17 17:00:00'),
                 'Participant Portal 2')
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, HEART_RATE_SUMMARY]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'date', 'calorie_count', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, datetime.fromisoformat('2020-08-17').date(), 100,
                 'Participant Portal 2'),
                (5678, datetime.fromisoformat('2020-08-17').date(), 200,
                 'Participant Portal 1'),
                (2345, datetime.fromisoformat('2020-08-17').date(), 500,
                 'Participant Portal 2'),
                (6789, datetime.fromisoformat('2020-08-17').date(), 800,
                 'Participant Portal 1'),
                (3456, datetime.fromisoformat('2020-08-17').date(), 1000,
                 'Participant Portal 2')
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, STEPS_INTRADAY]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'steps', 'datetime', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, 60, datetime.fromisoformat('2020-08-17 15:00:00'),
                 'Participant Portal 2'),
                (5678, 50, datetime.fromisoformat('2020-08-17 15:30:00'),
                 'Participant Portal 1'),
                (2345, 55, datetime.fromisoformat('2020-08-17 16:00:00'),
                 'Participant Portal 2'),
                (6789, 40, datetime.fromisoformat('2020-08-17 16:30:00'),
                 'Participant Portal 1'),
                (3456, 65, datetime.fromisoformat('2020-08-17 17:00:00'),
                 'Participant Portal 2')
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, SLEEP_DAILY_SUMMARY]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'sleep_date', 'minute_in_bed', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, datetime.fromisoformat('2020-08-17').date(), 502,
                 'Participant Portal 2'),
                (5678, datetime.fromisoformat('2020-08-17').date(), 443,
                 'Participant Portal 1'),
                (2345, datetime.fromisoformat('2020-08-17').date(), 745,
                 'Participant Portal 2'),
                (6789, datetime.fromisoformat('2020-08-17').date(), 605,
                 'Participant Portal 1'),
                (3456, datetime.fromisoformat('2020-08-17').date(), 578,
                 'Participant Portal 2')
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, SLEEP_LEVEL]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'sleep_date', 'duration_in_min', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, datetime.fromisoformat('2020-08-17').date(), 42,
                 'Participant Portal 2'),
                (5678, datetime.fromisoformat('2020-08-17').date(), 15,
                 'Participant Portal 1'),
                (2345, datetime.fromisoformat('2020-08-17').date(), 22,
                 'Participant Portal 2'),
                (6789, datetime.fromisoformat('2020-08-17').date(), 56,
                 'Participant Portal 1'),
                (3456, datetime.fromisoformat('2020-08-17').date(), 12,
                 'Participant Portal 2')
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, DEVICE]),
            'fq_sandbox_table_name':
                None,
            'fields': ['person_id', 'device_date', 'battery', 'src_id'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1234, datetime.fromisoformat('2020-08-17').date(), "Medium",
                 'Participant Portal 2'),
                (5678, datetime.fromisoformat('2020-08-17').date(), "Medium",
                 'Participant Portal 1'),
                (2345, datetime.fromisoformat('2020-08-17').date(), "Medium",
                 'Participant Portal 2'),
                (6789, datetime.fromisoformat('2020-08-17').date(), "Medium",
                 'Participant Portal 1'),
                (3456, datetime.fromisoformat('2020-08-17').date(), "Medium",
                 'Participant Portal 2')
            ]
        }]

        # mock the PIPELINE_TABLES
        with mock.patch(
                'cdr_cleaner.cleaning_rules.deid.fitbit_deid_src_id.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)
