# Python Imports
import os
from datetime import datetime

# Project Imports
from app_identity import PROJECT_ID
import cdr_cleaner.cleaning_rules.deid.fitbit_pid_rid_map as pr
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import ACTIVITY_SUMMARY, HEART_RATE_SUMMARY, HEART_RATE_MINUTE_LEVEL, STEPS_INTRADAY, SLEEP_DAILY_SUMMARY, SLEEP_LEVEL, DEVICE, DEID_MAP


class FitbitPIDtoRIDTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        # using unioned since we don't declare a deid dataset
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        mapping_table_id = DEID_MAP
        cls.mapping_dataset_id = mapping_dataset_id
        cls.kwargs.update({
            'mapping_dataset_id': mapping_dataset_id,
            'mapping_table_id': mapping_table_id
        })
        cls.fq_deid_map_table = f'{cls.project_id}.{mapping_dataset_id}.{mapping_table_id}'

        cls.rule_instance = pr.FitbitPIDtoRID(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id,
                                              mapping_dataset_id,
                                              mapping_table_id)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_id)}'
            for table_id in pr.FITBIT_TABLES
        ]

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table_id}'
            for table_id in pr.FITBIT_TABLES
        ] + [cls.fq_deid_map_table
            ] + [f'{cls.project_id}.{mapping_dataset_id}.person']

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common
        fully qualified (fq) dataset name string to load the data.
        """
        self.value_as_number = None

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_field_cleaning(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and CLEAN_PPI_NUMERIC_FIELDS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        queries = []
        as_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id,activity_calories,date)
        VALUES
            (1234, 100, date('2020-08-17')),
            (5678, 200, date('2020-08-17')),
            (2345, 500, date('2020-08-17')),
            (6789, 800, date('2020-08-17')),
            (3456, 1000, date('2020-08-17')),
            (3456, 2000, date('2020-08-18'))""").render(
            fq_dataset_name=self.fq_dataset_name, fitbit_table=ACTIVITY_SUMMARY)
        queries.append(as_query)

        hr_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id,heart_rate_value,datetime)
        VALUES
            (1234, 60, (DATETIME '2020-08-17 15:00:00')),
            (5678, 50, (DATETIME '2020-08-17 15:30:00')),
            (2345, 55, (DATETIME '2020-08-17 16:00:00')),
            (6789, 40, (DATETIME '2020-08-17 16:30:00')),
            (3456, 65, (DATETIME '2020-08-17 17:00:00')),            
            (3456, 70, (DATETIME '2020-08-18 17:00:00'))""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=HEART_RATE_MINUTE_LEVEL)
        queries.append(hr_query)

        hrs_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id,date,calorie_count)
        VALUES
            (1234, date('2020-08-17'), 100),
            (5678, date('2020-08-17'), 200),
            (2345, date('2020-08-17'), 500),
            (6789, date('2020-08-17'), 800),
            (3456, date('2020-08-17'), 1000)""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=HEART_RATE_SUMMARY)
        queries.append(hrs_query)

        sid_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id,steps,datetime)
        VALUES
            (1234, 60, (DATETIME '2020-08-17 15:00:00')),
            (5678, 50, (DATETIME '2020-08-17 15:30:00')),
            (2345, 55, (DATETIME '2020-08-17 16:00:00')),
            (6789, 40, (DATETIME '2020-08-17 16:30:00')),
            (3456, 65, (DATETIME '2020-08-17 17:00:00'))""").render(
            fq_dataset_name=self.fq_dataset_name, fitbit_table=STEPS_INTRADAY)
        queries.append(sid_query)

        sds_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id,sleep_date,minute_in_bed)
        VALUES
            (1234, date('2020-08-17'), 502),
            (5678, date('2020-08-17'), 443),
            (2345, date('2020-08-17'), 745),
            (6789, date('2020-08-17'), 605),
            (3456, date('2020-08-17'), 578)""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=SLEEP_DAILY_SUMMARY)
        queries.append(sds_query)

        sl_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id,sleep_date,duration_in_min)
        VALUES
            (1234, date('2020-08-17'), 42),
            (5678, date('2020-08-17'), 15),
            (2345, date('2020-08-17'), 22),
            (6789, date('2020-08-17'), 56),
            (3456, date('2020-08-17'), 12)""").render(
            fq_dataset_name=self.fq_dataset_name, fitbit_table=SLEEP_LEVEL)
        queries.append(sl_query)

        device_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
        (person_id, date, battery)
        VALUES
            (1234, date('2020-08-17'), "Medium"),
            (5678, date('2020-08-17'), "Medium"),
            (2345, date('2020-08-17'), "Medium"),
            (6789, date('2020-08-17'), "Medium"),
            (3456, date('2020-08-17'), "Medium")""").render(
            fq_dataset_name=self.fq_dataset_name, fitbit_table=DEVICE)
        queries.append(device_query)

        pid_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.person`
        (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
        VALUES
            (1234, 0, 1960, 0, 0),
            (5678, 0, 1970, 0, 0),
            (2345, 0, 1980, 0, 0),
            (6789, 0, 1990, 0, 0),
            (3456, 0, 1965, 0, 0)""").render(
            fq_dataset_name=f'{self.project_id}.{self.mapping_dataset_id}')
        queries.append(pid_query)

        map_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_table}}`
        (person_id,research_id,shift)
        VALUES
            (1234, 234, 256),
            (5678, 678, 250),
            (2345, 345, 255),
            (6789, 789, 256)""").render(fq_table=self.fq_deid_map_table)
        queries.append(map_query)

        self.load_test_data(queries)

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, ACTIVITY_SUMMARY]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': ['person_id', 'activity_calories', 'date'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, 100, datetime.fromisoformat('2020-08-17').date()),
                (678, 200, datetime.fromisoformat('2020-08-17').date()),
                (345, 500, datetime.fromisoformat('2020-08-17').date()),
                (789, 800, datetime.fromisoformat('2020-08-17').date())
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, HEART_RATE_MINUTE_LEVEL]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'fields': ['person_id', 'heart_rate_value', 'datetime'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, 60, datetime.fromisoformat('2020-08-17 15:00:00')),
                (678, 50, datetime.fromisoformat('2020-08-17 15:30:00')),
                (345, 55, datetime.fromisoformat('2020-08-17 16:00:00')),
                (789, 40, datetime.fromisoformat('2020-08-17 16:30:00'))
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, HEART_RATE_SUMMARY]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[2],
            'fields': ['person_id', 'date', 'calorie_count'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, datetime.fromisoformat('2020-08-17').date(), 100),
                (678, datetime.fromisoformat('2020-08-17').date(), 200),
                (345, datetime.fromisoformat('2020-08-17').date(), 500),
                (789, datetime.fromisoformat('2020-08-17').date(), 800)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, STEPS_INTRADAY]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[3],
            'fields': ['person_id', 'datetime', 'steps'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, datetime.fromisoformat('2020-08-17 15:00:00'), 60),
                (678, datetime.fromisoformat('2020-08-17 15:30:00'), 50),
                (345, datetime.fromisoformat('2020-08-17 16:00:00'), 55),
                (789, datetime.fromisoformat('2020-08-17 16:30:00'), 40)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, SLEEP_DAILY_SUMMARY]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[4],
            'fields': ['person_id', 'sleep_date', 'minute_in_bed'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, datetime.fromisoformat('2020-08-17').date(), 502),
                (678, datetime.fromisoformat('2020-08-17').date(), 443),
                (345, datetime.fromisoformat('2020-08-17').date(), 745),
                (789, datetime.fromisoformat('2020-08-17').date(), 605)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, SLEEP_LEVEL]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[5],
            'fields': ['person_id', 'sleep_date', 'duration_in_min'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, datetime.fromisoformat('2020-08-17').date(), 42),
                (678, datetime.fromisoformat('2020-08-17').date(), 15),
                (345, datetime.fromisoformat('2020-08-17').date(), 22),
                (789, datetime.fromisoformat('2020-08-17').date(), 56)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEVICE]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[6],
            'fields': ['person_id', 'date', 'battery'],
            'loaded_ids': [1234, 5678, 2345, 6789, 3456],
            'sandboxed_ids': [3456],
            'cleaned_values': [
                (234, datetime.fromisoformat('2020-08-17').date(), "Medium"),
                (678, datetime.fromisoformat('2020-08-17').date(), "Medium"),
                (345, datetime.fromisoformat('2020-08-17').date(), "Medium"),
                (789, datetime.fromisoformat('2020-08-17').date(), "Medium")
            ]
        }]

        self.default_test(tables_and_counts)
