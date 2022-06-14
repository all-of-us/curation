"""
Ensures there is no data past the deactivation date for deactivated participants.

Original Issue: DC-686, DC-1795

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program
This test will mock calling the PS API and provide a returned value.  Everything
within the bounds of our team will be tested.
"""
# Python imports
import os
from unittest import mock

# Third party imports
import pandas as pd

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.remove_participant_data_past_deactivation_date import (
    RemoveParticipantDataPastDeactivationDate, DEACTIVATED_PARTICIPANTS, DATE,
    DATETIME, START_DATE, START_DATETIME, END_DATE, END_DATETIME)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class RemoveParticipantDataPastDeactivationDateTest(
        BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = f"{dataset_id}_sandbox"
        cls.sandbox_id = sandbox_id

        cls.kwargs = {
            'table_namer': 'bar_ds',
            'api_project_id': 'foo-project-id'
        }
        cls.rule_instance = RemoveParticipantDataPastDeactivationDate(
            project_id, dataset_id, sandbox_id, **cls.kwargs)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        cls.fq_sandbox_table_names = [
            f'{project_id}.{sandbox_id}.{table_name}'
            for table_name in sb_table_names
        ]

        # append table name here to ensure proper cleanup
        cls.fq_sandbox_table_names.append(
            f"{project_id}.{sandbox_id}.{DEACTIVATED_PARTICIPANTS}")

        cls.fq_table_names = [
            f"{project_id}.{dataset_id}.{tablename}"
            for tablename in cls.rule_instance.affected_tables
        ]

        cls.fq_obs_table = [
            table for table in cls.fq_table_names if 'observation' in table
        ][0]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on.
        """
        insert_fake_data_tmpls = [
            self.jinja_env.from_string("""
        INSERT INTO `{{fq_table_name}}`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, observation_source_concept_id)
        VALUES
        -- Values to exist after running the cleaning rule --
        -- 801 is before the user deactivates --
        -- 802, the user doesn't deactivate --
          (801, 1, 1585899, date('2019-05-01'), 45905771, 111111),
          (802, 2, 1585899, date('2019-05-01'), 45905771, 222222),
        -- Values that should be removed by the cleaning rule --
        -- 804 is after person 1 deactivates --
        -- 805 is after user 3 deactivates --
          (804, 1, 1585899, date('2020-05-01'), 45905771, null),
          (805, 3, 1585899, date('2020-05-01'), 45905771, 45)
        """)
        ]

        self.load_statements = []
        # create the string(s) to load the data
        for tmpl in insert_fake_data_tmpls:
            query = tmpl.render(fq_table_name=self.fq_obs_table)
            self.load_statements.append(query)

        super().setUp()

    def test_get_dates_info(self):
        # preconditions
        data = {
            'table_catalog': ['project'] * 13,
            'table_schema': ['dataset'] * 13,
            'table_name': ['observation'] * 5 + ['location'] * 2 +
                          ['drug_exposure'] * 6,
            'column_name': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_datetime', 'location_id',
                'city', 'person_id', 'drug_exposure_start_date',
                'drug_exposure_start_datetime', 'drug_exposure_end_date',
                'drug_exposure_end_datetime', 'verbatim_date'
            ],
        }
        table_cols_df = pd.DataFrame(data,
                                     columns=[
                                         'table_catalog', 'table_schema',
                                         'table_name', 'column_name'
                                     ])

        expected_dict = {
            'observation': ['observation_date', 'observation_datetime'],
            'drug_exposure': [
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_exposure_end_date', 'drug_exposure_end_datetime',
                'verbatim_date'
            ]
        }
        actual_dict = self.rule_instance.get_table_dates_info(table_cols_df)

        self.assertDictEqual(actual_dict, expected_dict)

    def test_get_date_cols_dict(self):
        date_cols = ["visit_date", "measurement_date", "measurement_datetime"]
        expected = {DATE: "measurement_date", DATETIME: "measurement_datetime"}
        actual = self.rule_instance.get_date_cols_dict(date_cols)
        self.assertDictEqual(expected, actual)

        date_cols = [
            "verbatim_date", "condition_end_date", "condition_end_datetime",
            "condition_start_datetime", "condition_start_date"
        ]
        expected = {
            START_DATE: "condition_start_date",
            START_DATETIME: "condition_start_datetime",
            END_DATE: "condition_end_date",
            END_DATETIME: "condition_end_datetime"
        }
        actual = self.rule_instance.get_date_cols_dict(date_cols)
        self.assertDictEqual(expected, actual)

    @mock.patch(
        'utils.participant_summary_requests.get_deactivated_participants')
    @mock.patch('retraction.retract_utils.is_deid_label_or_id')
    def test_removing_data_past_deactivated_date(self, mock_deid, mock_func):
        """
        Validate deactivated participant records are dropped via cleaning rule.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.  Uses a mock to
        return a staged data frame object for this test instead of calling
        the PS API.
        """
        columns = ['deactivated_datetime', 'person_id', 'suspension_status']
        values = [
            ['2020-01-01 01:00:00 UTC', 1,
             'NO_CONTACT'],  # corresponds with record 804
            ['2020-01-01 01:00:00 UTC', 3,
             'NO_CONTACT']  # corresponds with record 805
        ]
        deactivated_df = pd.DataFrame(values, columns=columns)

        mock_func.return_value = deactivated_df
        mock_deid.return_value = False
        self.load_test_data(self.load_statements)

        # Using the 0 position because there is only one sandbox table and
        # one affected OMOP table
        obs_sandbox = [
            table for table in self.fq_sandbox_table_names
            if 'observation' in table
        ][0]
        tables_and_counts = [{
            'name': 'observation',
            'fq_table_name': self.fq_obs_table,
            'fq_sandbox_table_name': obs_sandbox,
            'fields': ['observation_id'],
            'loaded_ids': [801, 802, 804, 805],
            'sandboxed_ids': [804, 805],
            'cleaned_values': [(801,), (802,)]
        }]

        self.default_test(tables_and_counts)
