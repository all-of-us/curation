"""
Integration Test for the fitbit_dateshift module.

Date shift any fields that are of type DATE, DATETIME, or TIMESTAMP.
"""
# Python imports
import os
from datetime import timedelta

# Third party imports
import pytz
from dateutil import parser
from google.cloud import bigquery
from mock import patch

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.fitbit_dateshift import FitbitDateShiftRule
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class FitbitDateShiftTest(BaseTest.DeidRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        combined_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.combined_dataset_id = combined_dataset_id

        cls.rule_instance = FitbitDateShiftRule(project_id, dataset_id,
                                                sandbox_id, combined_dataset_id)

        # can test the full functionality with one table
        cls.fq_table_names = [
            f"{project_id}.{dataset_id}.{cls.rule_instance.tables[0]}"
        ]

        # provide mapping table info
        cls.fq_mapping_tablename = f"{project_id}.{combined_dataset_id}._deid_map"

        # call super to set up the client, create datasets, and create
        # empty test tables
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """
        # create a false fitbit table to query
        # this schema is mocked later
        schema = [
            bigquery.SchemaField("person_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("p_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("p_datetime", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("p_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("p_name", "STRING", mode="REQUIRED"),
        ]

        for table_id in self.fq_table_names:
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)  # Make an API request.

        self.mock_schema = {
            self.fq_table_names[0].split('.')[-1]: [{
                "mode": "REQUIRED",
                "name": "person_id",
                "type": "INTEGER"
            }, {
                "mode": "REQUIRED",
                "name": "p_date",
                "type": "DATE"
            }, {
                "mode": "REQUIRED",
                "name": "p_datetime",
                "type": "DATETIME"
            }, {
                "mode": "REQUIRED",
                "name": "p_timestamp",
                "type": "TIMESTAMP"
            }, {
                "mode": "REQUIRED",
                "name": "p_name",
                "type": "STRING"
            }]
        }

        # create a mapping table
        self.create_mapping_table()

        # load statement for the test data to shift
        query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (person_id, p_date, p_datetime, p_timestamp, p_name)
        VALUES
          -- setting day to the 11th to make it easier to calculate the shifted date --
          (801, date(2016, 05, 11), datetime(2016, 05, 11, 12, 45, 00), timestamp('2016-05-11 12:45:00 UTC'), "john doe"),
          (802, date(2016, 05, 10), datetime(2016, 05, 10, 12, 45, 00), timestamp('2016-05-10 12:45:00 UTC'), "jane doe")
        """)

        # load statement for mapping table under test
        map_query = self.jinja_env.from_string("""
        INSERT INTO `{{map_table_name}}`
        (person_id, research_id, shift)
        -- setting the date shift to 10 days in the past for participant with research_id 801 --
        -- the research_id maps to the real person_id here --
        -- this assumes the pid/rid mapping has already occurred. --
        VALUES 
        -- a date shift of 10 days --
        (700, 801, 10),
        -- a date shift of 5 days --
        (500, 802, 5)
        """)
        load_statements = [
            query.render(fq_table_name=self.fq_table_names[0]),
            map_query.render(map_table_name=self.fq_mapping_tablename)
        ]
        self.load_test_data(load_statements)

    @patch(
        'cdr_cleaner.cleaning_rules.deid.fitbit_dateshift.FitbitDateShiftRule.get_tables_and_schemas'
    )
    def test_dateshifting(self, mock_schema):
        """
        Use the default drop rows test function.

        Mocks the table schema to limit this to a single test that tests all possible values.
        """
        # mock return value
        mock_schema.return_value = self.mock_schema

        shifted_date = parser.parse('2016-05-01').date()
        shifted_datetime = parser.parse('2016-05-01 12:45:00')
        shifted_timestamp = pytz.utc.localize(
            parser.parse('2016-05-01 12:45:00'))
        four_days = timedelta(days=4)

        # Using the 0 position because there is only one sandbox table and
        # one affected OMOP table
        tables_and_counts = [{
            'name':
                self.fq_table_names[0].split('.')[-1],
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                '',
            'fields': [
                'person_id', 'p_date', 'p_datetime', 'p_timestamp', 'p_name'
            ],
            'loaded_ids': [801, 802],
            'sandboxed_ids': [],
            'cleaned_values': [
                (801, shifted_date, shifted_datetime, shifted_timestamp,
                 'john doe'),
                (802, shifted_date + four_days, shifted_datetime + four_days,
                 shifted_timestamp + four_days, 'jane doe')
            ]
        }]

        self.default_test(tables_and_counts)
