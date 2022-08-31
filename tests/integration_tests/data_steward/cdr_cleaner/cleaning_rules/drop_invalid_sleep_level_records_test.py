"""
Integration test for drop_invalid_sleep_level_records module

Rule to sandbox and drop records in the sleep_level table where level is
not one of the following: awake, light, asleep, deep, restless, wake, rem, unknown.

Original Issues: DC-2605
"""

# Python Imports
import os
from datetime import date, datetime

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, SLEEP_LEVEL
from cdr_cleaner.cleaning_rules.drop_invalid_sleep_level_records import DropInvalidSleepLevelRecords
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

SLEEP_LEVEL_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{sleep_level_table}}`
(person_id, sleep_date, is_main_sleep, level, start_datetime, duration_in_min)
VALUES
(1, '2008-11-18','false', 'wake', '2008-11-18T00:00:00', 4.5),
(2, '2010-01-01','true', 'light', '2010-01-01T00:00:00', 3.5),
(3, '2012-01-01','true', 'invalid', '2010-01-01T00:00:00', 2.5),
(4, '2010-11-18','false', 'rem', '2008-11-18T05:00:00', 7.5),
(5, '2014-01-01','false', 'deep', '2014-01-01T05:00:00', 8.5),
(6, '2013-03-14','true', 'awake', '2013-03-14T05:00:00', 6.6),
(7, '2009-06-23','false', NULL, '2009-06-23T05:00:00', 3.8),
(8, '2011-09-12','true', 'restless', '2011-09-12T05:00:00', 5.3),
(9, '2015-01-21','true', 'unknown', '2015-01-21T05:00:00', 7.3),
(10, '2017-02-05','false', 'asleep', '2017-02-05T05:00:00', 3.8),
(11, '2010-07-18','false', 'invalid', '2010-07-18T05:00:00', 7.5)
 """)


class DropInvalidSleepLevelRecordsTest(BaseTest.CleaningRulesTestBase):

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
        cls.rule_instance = DropInvalidSleepLevelRecords(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
        )

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store sleep_level table name
        sleep_level_table_name = f'{cls.project_id}.{cls.dataset_id}.{SLEEP_LEVEL}'
        cls.fq_table_names = [sleep_level_table_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        # Query to insert test records into sleep_level table
        sleep_level_query = SLEEP_LEVEL_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sleep_level_table=SLEEP_LEVEL)

        #Load test data
        self.load_test_data([sleep_level_query])

    def test_field_cleaning(self):
        """
        person_ids 3, 7, and 11 contain values that are not
        one of the following: awake, light, asleep, deep, restless, wake, rem, unknown.
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            'sandboxed_ids': [3, 7, 11],
            'fields': [
                'person_id', 'sleep_date', 'is_main_sleep', 'level',
                'start_datetime', 'duration_in_min'
            ],
            'cleaned_values': [
                (1, date.fromisoformat('2008-11-18'), 'false', 'wake',
                 datetime.fromisoformat('2008-11-18T00:00:00'), 4.5),
                (2, date.fromisoformat('2010-01-01'), 'true', 'light',
                 datetime.fromisoformat('2010-01-01T00:00:00'), 3.5),
                (4, date.fromisoformat('2010-11-18'), 'false', 'rem',
                 datetime.fromisoformat('2008-11-18T05:00:00'), 7.5),
                (5, date.fromisoformat('2014-01-01'), 'false', 'deep',
                 datetime.fromisoformat('2014-01-01T05:00:00'), 8.5),
                (6, date.fromisoformat('2013-03-14'), 'true', 'awake',
                 datetime.fromisoformat('2013-03-14T05:00:00'), 6.6),
                (8, date.fromisoformat('2011-09-12'), 'true', 'restless',
                 datetime.fromisoformat('2011-09-12T05:00:00'), 5.3),
                (9, date.fromisoformat('2015-01-21'), 'true', 'unknown',
                 datetime.fromisoformat('2015-01-21T05:00:00'), 7.3),
                (10, date.fromisoformat('2017-02-05'), 'false', 'asleep',
                 datetime.fromisoformat('2017-02-05T05:00:00'), 3.8)
            ]
        }]

        self.default_test(tables_and_counts)
