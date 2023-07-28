"""
Original Issues: DC-3337
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, FITBIT_TABLES
from cdr_cleaner.cleaning_rules.deid.fitbit_deid_src_id import FitbitDeidSrcID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

ACTIVITY_SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id,activity_calories,date)
VALUES
    (1234, 100, date('2020-08-17')),
    (5678, 200, date('2020-08-17')),
    (2345, 500, date('2020-08-17')),
    (6789, 800, date('2020-08-17')),
    (3456, 1000, date('2020-08-17')),
    (3456, 2000, date('2020-08-18'))
""")

HEART_RATE_MINUTE_LEVEL_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id,heart_rate_value,datetime)
VALUES
    (1234, 60, (DATETIME '2020-08-17 15:00:00')),
    (5678, 50, (DATETIME '2020-08-17 15:30:00')),
    (2345, 55, (DATETIME '2020-08-17 16:00:00')),
    (6789, 40, (DATETIME '2020-08-17 16:30:00')),
    (3456, 65, (DATETIME '2020-08-17 17:00:00')),            
    (3456, 70, (DATETIME '2020-08-18 17:00:00'))
""")

HEART_RATE_SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id,date,calorie_count)
VALUES
    (1234, date('2020-08-17'), 100),
    (5678, date('2020-08-17'), 200),
    (2345, date('2020-08-17'), 500),
    (6789, date('2020-08-17'), 800),
    (3456, date('2020-08-17'), 1000)
""")

STEPS_INTRADAY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id,steps,datetime)
VALUES
    (1234, 60, (DATETIME '2020-08-17 15:00:00')),
    (5678, 50, (DATETIME '2020-08-17 15:30:00')),
    (2345, 55, (DATETIME '2020-08-17 16:00:00')),
    (6789, 40, (DATETIME '2020-08-17 16:30:00')),
    (3456, 65, (DATETIME '2020-08-17 17:00:00'))
""")

SLEEP_DAILY_SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id,sleep_date,minute_in_bed)
VALUES
    (1234, date('2020-08-17'), 502),
    (5678, date('2020-08-17'), 443),
    (2345, date('2020-08-17'), 745),
    (6789, date('2020-08-17'), 605),
    (3456, date('2020-08-17'), 578)
""")

SLEEP_LEVEL_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, sleep_date, is_main_sleep, level, start_datetime, duration_in_min)
VALUES
    (1234, '2008-11-18','false', 'wake', '2008-11-18T00:00:00', 4.5),
    (5678, '2010-01-01','true', 'light', '2010-01-01T00:00:00', 3.5),
    (2345, '2012-01-01','true', 'invalid', '2010-01-01T00:00:00', 2.5),
    (6789, '2010-11-18','false', 'rem', '2008-11-18T05:00:00', 7.5),
    (3456, '2014-01-01','false', 'deep', '2014-01-01T05:00:00', 8.5)
""")

DEVICE_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
(person_id, device_date, battery)
VALUES
    (1234, date('2020-08-17'), "Medium"),
    (5678, date('2020-08-17'), "Medium"),
    (2345, date('2020-08-17'), "Medium"),
    (6789, date('2020-08-17'), "Medium"),
    (3456, date('2020-08-17'), "Medium")
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
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # Instantiate class
        cls.rule_instance = FitbitDeidSrcID(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
        )

        # Generates list of fully qualified table names
        affected_table_names = cls.rule_instance.affected_tables
        for table_name in affected_table_names:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        pass

    def test_field_cleaning(self):
        pass
