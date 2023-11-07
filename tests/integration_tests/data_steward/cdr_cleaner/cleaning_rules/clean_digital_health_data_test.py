"""
Integration test for clean_digital_health_date module

Remove wearables data for participants without active digital health consent

Original Issue: DC-1910
"""

# Python Imports
import os
from unittest.mock import patch
from datetime import datetime

# Project Imports
from app_identity import PROJECT_ID
from common import FITBIT_TABLES, ACTIVITY_SUMMARY, HEART_RATE_INTRADAY, HEART_RATE_SUMMARY, STEPS_INTRADAY
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
import cdr_cleaner.cleaning_rules.clean_digital_health_data as clean_dhd

DIGITAL_HEALTH_JSON = [{
    'person_id': 111,
    'wearable': 'fitbit',
    'status': 'YES',
    'history': [{
        'status': 'YES',
        'authored_time': '2020-01-01T12:01:01Z'
    }],
    'authored_time': '2020-01-01T12:01:01Z'
}, {
    'person_id': 222,
    'wearable': 'fitbit',
    'status': 'YES',
    'history': [{
        'status': 'YES',
        'authored_time': '2021-01-01T12:01:01Z'
    }],
    'authored_time': '2021-01-01T12:01:01Z'
}, {
    'person_id': 333,
    'wearable': 'appleHealthKit',
    'status': 'NO',
    'history': [{
        'status': 'NO',
        'authored_time': '2022-02-01T12:01:01Z'
    }, {
        'status': 'YES',
        'authored_time': '2021-02-01T12:01:01Z'
    }, {
        'status': 'NO',
        'authored_time': '2020-06-01T12:01:01Z'
    }, {
        'status': 'YES',
        'authored_time': '2020-03-01T12:01:01Z'
    }],
    'authored_time': '2022-02-01T12:01:01Z'
}]

OLD_DIGITAL_HEALTH_JSON = [{
    'person_id': 333,
    'wearable': 'appleHealthKit',
    'status': 'YES',
    'history': [{
        'status': 'YES',
        'authored_time': '2021-02-01T12:01:01Z'
    }, {
        'status': 'NO',
        'authored_time': '2020-06-01T12:01:01Z'
    }, {
        'status': 'YES',
        'authored_time': '2020-03-01T12:01:01Z'
    }],
    'authored_time': '2021-02-01T12:01:01Z'
}]


class CleanDigitalHealthDataTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = f'{dataset_id}_sandbox'
        cls.sandbox_id = sandbox_id

        cls.kwargs = {'api_project_id': 'rdr_project_id'}

        cls.rule_instance = clean_dhd.CleanDigitalHealthStatus(
            project_id, dataset_id, sandbox_id, **cls.kwargs)

        sandbox_tables = cls.rule_instance.get_sandbox_tablenames()
        cls.fq_sandbox_table_names = [
            f'{project_id}.{sandbox_id}.{table}' for table in sandbox_tables
        ]

        cls.fq_digital_health_table = f'{cls.project_id}.{cls.dataset_id}.{clean_dhd.DIGITAL_HEALTH_SHARING_STATUS}'

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{table_id}'
            for table_id in FITBIT_TABLES
        ] + [cls.fq_digital_health_table]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    @patch(
        'cdr_cleaner.cleaning_rules.clean_digital_health_data.get_digital_health_information'
    )
    @patch(
        'cdr_cleaner.cleaning_rules.clean_digital_health_data.PIPELINE_TABLES',
        os.environ.get('COMBINED_DATASET_ID'))
    def test_clean_digital_health_data(self, mock_get_digital_health):
        """
        Tests perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        clean_dhd.store_digital_health_status_data(
            self.client, OLD_DIGITAL_HEALTH_JSON,
            f'{self.project_id}.{self.dataset_id}.{clean_dhd.DIGITAL_HEALTH_SHARING_STATUS}'
        )
        mock_get_digital_health.return_value = DIGITAL_HEALTH_JSON

        queries = []
        as_query = self.jinja_env.from_string("""
                    INSERT INTO `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
                    (person_id, date)
                    VALUES
                    (111, date('2018-11-26')),
                    (222, date('2019-11-26')),
                    (333, date('2020-11-26'))""").render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fitbit_table=ACTIVITY_SUMMARY)
        queries.append(as_query)

        hr_query = self.jinja_env.from_string("""
                    INSERT INTO `{{project_id}}.{{dataset_id}}.{{fitbit_table}}` 
                    (person_id, datetime)
                    VALUES
                    (111, '2018-11-26 00:00:00'),
                    (222, '2019-11-26 00:00:00'),
                    (333, '2020-11-26 00:00:00')""").render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fitbit_table=HEART_RATE_INTRADAY)
        queries.append(hr_query)

        hrs_query = self.jinja_env.from_string("""
                    INSERT INTO `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
                    (person_id, date)
                    VALUES
                    (111, date('2018-11-26')),
                    (222, date('2019-11-26')),
                    (333, date('2020-11-26'))""").render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fitbit_table=HEART_RATE_SUMMARY)
        queries.append(hrs_query)

        sid_query = self.jinja_env.from_string("""
                    INSERT INTO `{{project_id}}.{{dataset_id}}.{{fitbit_table}}` 
                    (person_id, datetime)
                    VALUES
                    (111, '2018-11-26 00:00:00'),
                    (222, '2019-11-26 00:00:00'),
                    (333, '2020-11-26 00:00:00')""").render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fitbit_table=STEPS_INTRADAY)
        queries.append(sid_query)

        self.load_test_data(queries)

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.dataset_id, ACTIVITY_SUMMARY]),
            'fq_sandbox_table_name': [
                sb_name for sb_name in self.fq_sandbox_table_names
                if ACTIVITY_SUMMARY in sb_name
            ][0],
            'fields': ['person_id', 'date'],
            'loaded_ids': [111, 222, 333],
            'sandboxed_ids': [333],
            'cleaned_values': [
                (111, datetime.fromisoformat('2018-11-26').date()),
                (222, datetime.fromisoformat('2019-11-26').date())
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, HEART_RATE_INTRADAY]),
            'fq_sandbox_table_name': [
                sb_name for sb_name in self.fq_sandbox_table_names
                if HEART_RATE_INTRADAY in sb_name
            ][0],
            'fields': ['person_id', 'datetime'],
            'loaded_ids': [111, 222, 333],
            'sandboxed_ids': [333],
            'cleaned_values': [
                (111, datetime.fromisoformat('2018-11-26 00:00:00')),
                (222, datetime.fromisoformat('2019-11-26 00:00:00'))
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, HEART_RATE_SUMMARY]),
            'fq_sandbox_table_name': [
                sb_name for sb_name in self.fq_sandbox_table_names
                if HEART_RATE_SUMMARY in sb_name
            ][0],
            'fields': ['person_id', 'date'],
            'loaded_ids': [111, 222, 333],
            'sandboxed_ids': [333],
            'cleaned_values': [
                (111, datetime.fromisoformat('2018-11-26').date()),
                (222, datetime.fromisoformat('2019-11-26').date())
            ]
        }, {
            'fq_table_name':
                '.'.join([self.dataset_id, STEPS_INTRADAY]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[3],
            'fields': ['person_id', 'datetime'],
            'loaded_ids': [111, 222, 333],
            'sandboxed_ids': [333],
            'cleaned_values': [
                (111, datetime.fromisoformat('2018-11-26 00:00:00')),
                (222, datetime.fromisoformat('2019-11-26 00:00:00'))
            ]
        }]

        self.default_test(tables_and_counts)
