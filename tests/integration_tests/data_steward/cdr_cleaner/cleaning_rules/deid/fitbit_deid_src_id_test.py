"""
Original Issues: DC-3337
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.deid.fitbit_deid_src_id import FitbitDeidSrcID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

ACTIVITY_SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{activity_summary_table}}`
 """)

HEART_RATE_MINUTE_LEVEL_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{heart_rate_minute_level_table}}`
""")

HEART_RATE_SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{heart_rate_summary_table}}`
""")

STEPS_INTRADAY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{steps_intraday_table}}`
""")

SLEEP_DAILY_SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{sleep_daily_summary_table}}`
""")

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
(11, '2010-07-18','false', 'invalid', '2010-07-18T05:00:00', 7.5),
(11, '2013-03-23','true', 'light', '2013-03-23T05:00:00', 4.5),
(12, '2009-06-15','true', 'Deep', '2009-06-15T05:00:00', 7.5)
 """)

DEVICE_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{device_table}}`
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
