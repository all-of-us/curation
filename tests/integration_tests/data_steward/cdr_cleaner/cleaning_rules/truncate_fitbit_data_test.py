"""
Integration test for the truncate_fitbit_data module

Original Issue: DC-1046

Ensures there is no data after the cutoff date for participants in
the fitbit tables by sandboxing the applicable records and then dropping them.
"""

# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from common import FITBIT_TABLES, ACTIVITY_SUMMARY,\
    HEART_RATE_SUMMARY, SLEEP_LEVEL, SLEEP_DAILY_SUMMARY,\
    HEART_RATE_MINUTE_LEVEL, STEPS_INTRADAY, DEVICE
from app_identity import PROJECT_ID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.truncate_fitbit_data import TruncateFitbitData


class TruncateFitbitDataTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        truncation_date = '2019-11-26'
        cls.kwargs.update({'truncation_date': truncation_date})

        cls.rule_instance = TruncateFitbitData(project_id,
                                               dataset_id,
                                               sandbox_id,
                                               truncation_date=truncation_date)

        # Generates list of fully qualified sandbox table names
        sb_date_table_names, sb_datetime_table_names = cls.rule_instance.get_sandbox_tablenames(
        )
        for table_name in sb_date_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')
        for table_name in sb_datetime_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        # Generates list of fully qualified FitBit table names
        for table in FITBIT_TABLES:
            cls.fq_table_names.append(f'{project_id}.{dataset_id}.{table}')

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
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_truncate_data(self):
        """
        Tests that the specifications for the SANDBOX_QUERY and TRUNCATE_FITBIT_DATA_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """
        queries = []
        as_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
            (person_id, date)
            VALUES
            (111, date('2018-11-26')),
            (222, date('2019-11-26')),
            (333, date('2020-11-26')),
            (444, date('2021-11-26'))""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=ACTIVITY_SUMMARY)
        queries.append(as_query)

        hr_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}` 
            (person_id, datetime)
            VALUES
            (111, (DATETIME '2018-11-26 00:00:00')),
            (222, (DATETIME '2019-11-26 00:00:00')),
            (333, (DATETIME '2020-11-26 00:00:00')),
            (444, (DATETIME '2021-11-26 00:00:00'))""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=HEART_RATE_MINUTE_LEVEL)
        queries.append(hr_query)

        hrs_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}`
            (person_id, date)
            VALUES
            (111, date('2018-11-26')),
            (222, date('2019-11-26')),
            (333, date('2020-11-26')),
            (444, date('2021-11-26'))""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=HEART_RATE_SUMMARY)
        queries.append(hrs_query)

        sid_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}` 
            (person_id, datetime)
            VALUES
            (111, (DATETIME '2018-11-26 00:00:00')),
            (222, (DATETIME '2019-11-26 00:00:00')),
            (333, (DATETIME '2020-11-26 00:00:00')),
            (444, (DATETIME '2021-11-26 00:00:00'))""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=STEPS_INTRADAY)
        queries.append(sid_query)

        device_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.{{fitbit_table}}` 
            (person_id, date, datetime)
            VALUES
                -- The date occurs on or after the cutoff. Sandboxed and dropped. --
            (111, date('2019-11-26'), (DATETIME '2018-11-26 00:00:00')),
            (222, date('2018-11-26'), (DATETIME '2018-11-26 00:00:00')),
                -- The datetime occurs after the cutoff. Sandboxed and dropped. --
            (333, date('2018-11-26'), (DATETIME '2020-11-26 00:00:00')),
                -- The date and datetime occur before the cutoff date. No affect.
            (444, date('2018-11-26'), (DATETIME '2018-11-26 00:00:00'))""").render(
            fq_dataset_name=self.fq_dataset_name,
            fitbit_table=DEVICE)
        queries.append(device_query)

        self.load_test_data(queries)

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, ACTIVITY_SUMMARY]),
            'fq_sandbox_table_name': [
                table for table in self.fq_sandbox_table_names
                if ACTIVITY_SUMMARY in table
            ][0],
            'fields': ['person_id', 'date'],
            'loaded_ids': [111, 222, 333, 444],
            'sandboxed_ids': [333, 444],
            'cleaned_values': [(111, parser.parse('2018-11-26').date()),
                               (222, parser.parse('2019-11-26').date())]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name,
                          HEART_RATE_MINUTE_LEVEL]),
            'fq_sandbox_table_name': [
                table for table in self.fq_sandbox_table_names
                if HEART_RATE_MINUTE_LEVEL in table
            ][0],
            'fields': ['person_id', 'datetime'],
            'loaded_ids': [111, 222, 333, 444],
            'sandboxed_ids': [333, 444],
            'cleaned_values': [(111, parser.parse('2018-11-26 00:00:00')),
                               (222, parser.parse('2019-11-26 00:00:00'))]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, HEART_RATE_SUMMARY]),
            'fq_sandbox_table_name': [
                table for table in self.fq_sandbox_table_names
                if HEART_RATE_SUMMARY in table
            ][0],
            'fields': ['person_id', 'date'],
            'loaded_ids': [111, 222, 333, 444],
            'sandboxed_ids': [333, 444],
            'cleaned_values': [(111, parser.parse('2018-11-26').date()),
                               (222, parser.parse('2019-11-26').date())]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, STEPS_INTRADAY]),
            'fq_sandbox_table_name': [
                table for table in self.fq_sandbox_table_names
                if STEPS_INTRADAY in table
            ][0],
            'fields': ['person_id', 'datetime'],
            'loaded_ids': [111, 222, 333, 444],
            'sandboxed_ids': [333, 444],
            'cleaned_values': [(111, parser.parse('2018-11-26 00:00:00')),
                               (222, parser.parse('2019-11-26 00:00:00'))]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEVICE]),
            'fq_sandbox_table_name': [
                table for table in self.fq_sandbox_table_names
                if DEVICE in table
            ][0],
            'fields': ['person_id','date', 'datetime'],
            'loaded_ids': [111, 222, 333, 444],
            'sandboxed_ids': [111, 222, 333],
            'cleaned_values': [(444,parser.parse('2019-11-26').date(), parser.parse('2018-11-26 00:00:00'))]
        }]

        self.default_test(tables_and_counts)
