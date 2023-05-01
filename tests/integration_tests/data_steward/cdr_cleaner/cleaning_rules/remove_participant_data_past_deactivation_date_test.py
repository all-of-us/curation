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
from google.cloud.bigquery import TableReference

# Project imports
from common import (AOU_DEATH, JINJA_ENV, OBSERVATION, DRUG_EXPOSURE, DEATH,
                    PERSON, SURVEY_CONDUCT, HEART_RATE_MINUTE_LEVEL,
                    SLEEP_LEVEL, STEPS_INTRADAY)
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
            'table_namer': 'table_namer',
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
        TABLE_ROWS = {
            PERSON:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (person_id, gender_concept_id, year_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id)
        VALUES
        (1,8507,1989,'1989-07-25 01:00:00 UTC', 8527, 38003563),
        (2,8507,1975,'1975-03-14 02:00:00 UTC', 8527, 38003564),
        (3,8507,1981,'1981-11-18 05:00:00 UTC', 8527, 38003564),
        (4,8507,1991,'1991-11-25 08:00:00 UTC', 8527, 38003564),
        (5,8507,2001,'2001-09-20 11:00:00 UTC', 8527, 38003564),
        (6,8507,1979,'1979-10-06 11:00:00 UTC', 8527, 38003564)
        """),
            OBSERVATION:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (observation_id, person_id, observation_concept_id, observation_date, observation_datetime, observation_type_concept_id)
        VALUES
        (1001,1,0,'2008-07-25','2008-07-25 01:00:00 UTC',45905771),
        (1005,2,0,'2008-03-14','2008-03-14 02:00:00 UTC',45905771),
        (1002,3,0,'2009-11-18','2009-11-18 05:00:00 UTC',45905771),
        (1004,4,0,'2009-11-25','2009-11-25 08:00:00 UTC',45905771),
        (1003,5,0,'2010-09-20','2010-09-20 11:00:00 UTC',45905771),
        (1006,3,0,'2009-11-18','2009-11-18 00:30:00 UTC',45905771),
        (1007,4,0,'2009-11-25',NULL,45905771)
        """),
            DEATH:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (person_id, death_date, death_datetime, death_type_concept_id)
        VALUES
        (2,'2008-03-12','2008-03-12 05:00:00 UTC',8),
        (3,'2011-01-18','2011-01-18 05:00:00 UTC',6),
        (4,'2009-11-25','2009-11-25 00:30:00 UTC',6),
        (5,'2009-09-20',NULL,6)
        """),
            AOU_DEATH:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (aou_death_id, person_id, death_date, death_datetime, death_type_concept_id, src_id, primary_death_record)
        VALUES
        ('a2', 2, '2008-03-12', '2008-03-12 05:00:00 UTC', 8, 'hpo_a', True),
        ('b2', 2, '2018-03-12', '2018-03-12 05:00:00 UTC', 8, 'hpo_b', False),
        ('a3', 3, '2011-01-18', '2011-01-18 05:00:00 UTC', 6, 'rdr', True),
        ('a4', 4, '2009-11-25', '2009-11-25 00:30:00 UTC', 6, 'hpo_c', True),
        ('a5', 5, '2009-09-20', NULL, 6, 'hpo_a', True)
        """),
            DRUG_EXPOSURE:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, 
        drug_exposure_end_date, drug_exposure_end_datetime, verbatim_end_date, drug_type_concept_id)
        VALUES
        (2002,1,50,'2008-06-05','2008-06-05 01:00:00 UTC','2010-07-05','2008-06-05 01:00:00 UTC','2011-04-11',87),
        (2003,2,21,'2008-11-22','2008-11-22 02:00:00 UTC',NULL,NULL,'2010-06-18',51),
        (2004,3,5241,'2009-08-03','2009-08-03 05:00:00 UTC',NULL,NULL,'2009-11-26',2754),
        (2005,4,76536,'2010-02-17','2010-02-17 08:00:00 UTC',NULL,NULL,'2008-03-04',24),
        (2006,5,274,'2009-04-19','2009-04-19 11:00:00 UTC',NULL,'2010-11-19 01:00:00 UTC','2011-10-22',436),
        (2007,3,50,'2009-11-18','2009-11-18 00:30:00 UTC',NULL,NULL,'2009-11-18',87),
        (2008,4,50,'2009-11-25','2009-11-25 00:30:00 UTC','2009-11-25','2009-11-25 00:45:00 UTC','2009-11-24',87),
        (2009,6,50,'2009-10-06','2009-10-06 01:30:00 UTC',NULL,NULL,'2009-10-05',87),
        (2010,5,274,'2009-09-20','2009-09-20 11:00:00 UTC', '2009-09-20', NULL, NULL, 436)
        """),
            SURVEY_CONDUCT:
                JINJA_ENV.from_string("""
            INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
            (survey_conduct_id, person_id, survey_concept_id, survey_end_datetime,
             assisted_concept_id, respondent_type_concept_id, timing_concept_id,
              collection_method_concept_id, survey_source_concept_id, validated_survey_concept_id )
            VALUES
            (1, 1, 0, '2009-08-30 00:00:00 UTC', 0, 0, 0, 0, 0, 0),
            (2, 1, 0, '2009-08-30 19:33:53 UTC', 0, 0, 0, 0, 0, 0),
            (3, 2, 0, '2009-08-30 19:33:53 UTC', 0, 0, 0, 0, 0, 0),
            (4, 3, 0, '2009-08-30 19:33:53 UTC', 0, 0, 0, 0, 0, 0),
            (5, 4, 0, '2009-08-30 19:33:53 UTC', 0, 0, 0, 0, 0, 0)
                """),
            HEART_RATE_MINUTE_LEVEL:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (person_id, datetime, heart_rate_value)
        VALUES
        (1, '2009-01-01T00:00:00', 60),
        (1, '2010-01-01T00:00:00', 70)
        """),
            STEPS_INTRADAY:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (person_id, datetime, steps)
        VALUES
        (2, '2009-01-01T00:00:00', 100),
        (2, '2010-01-01T00:00:00', 150)
        """),
            SLEEP_LEVEL:
                JINJA_ENV.from_string("""
        INSERT INTO `{{table.project}}.{{table.dataset_id}}.{{table.table_id}}`
        (person_id, sleep_date, is_main_sleep, level, start_datetime, duration_in_min)
        VALUES
        (1, '2010-01-01','true', 'light', '2010-01-01T00:00:00', 3.5),
        (1, '2008-11-18','false', 'wake', '2008-11-18T05:00:00', 4.5)
        """)
        }

        self.load_statements = []
        # create the string(s) to load the data
        for table in TABLE_ROWS:
            fq_table = TableReference.from_string(
                f'{self.project_id}.{self.dataset_id}.{table}')
            query = TABLE_ROWS[table].render(table=fq_table)
            self.load_statements.append(query)

        super().setUp()

    def get_dates_info(self):
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

    def get_date_cols_dict(self):
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
    def test_removing_data_past_deactivated_date(self, mock_get_deact):
        """
        Validate deactivated participant records are dropped via cleaning rule.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.  Uses a mock to
        return a staged data frame object for this test instead of calling
        the PS API.
        """

        self.get_date_cols_dict()
        self.get_dates_info()
        mock_get_deact.return_value = pd.DataFrame(
            [(1, 'NO_CONTACT', '2009-07-25 01:00:00 UTC'),
             (2, 'NO_CONTACT', '2009-03-14 01:00:00 UTC'),
             (3, 'NO_CONTACT', '2009-11-18 01:00:00 UTC'),
             (4, 'NO_CONTACT', '2009-11-25 01:00:00 UTC'),
             (5, 'NO_CONTACT', '2009-09-20 01:00:00 UTC'),
             (6, 'NO_CONTACT', '2009-10-06 01:00:00 UTC')],
            columns=['person_id', 'suspension_status', 'deactivated_datetime'])

        self.load_test_data(self.load_statements)

        tables_and_counts = [{
            'name':
                OBSERVATION,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'fields': ['observation_id'],
            'loaded_ids': [1001, 1002, 1003, 1004, 1005, 1006, 1007],
            'sandboxed_ids': [1002, 1003, 1004, 1007],
            'cleaned_values': [(1001,), (1005,), (1006,)]
        }, {
            'name':
                DRUG_EXPOSURE,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DRUG_EXPOSURE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(DRUG_EXPOSURE)}',
            'fields': ['drug_exposure_id'],
            'loaded_ids': [
                2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010
            ],
            'sandboxed_ids': [2002, 2003, 2004, 2005, 2006, 2007, 2009, 2010],
            'cleaned_values': [(2008,)]
        }, {
            'name':
                SURVEY_CONDUCT,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SURVEY_CONDUCT}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(SURVEY_CONDUCT)}',
            'fields': ['survey_conduct_id'],
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [1, 2, 3],
            'cleaned_values': [(4,), (5,)]
        }, {
            'name':
                DEATH,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(DEATH)}',
            'fields': ['person_id'],
            'loaded_ids': [2, 3, 4, 5],
            'sandboxed_ids': [3, 5],
            'cleaned_values': [(2,), (4,)]
        }, {
            'name':
                AOU_DEATH,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(AOU_DEATH)}',
            'fields': ['aou_death_id'],
            'loaded_ids': ['a2', 'b2', 'a3', 'a4', 'a5'],
            'sandboxed_ids': ['b2', 'a3', 'a5'],
            'cleaned_values': [('a2',), ('a4',)]
        }, {
            'name':
                HEART_RATE_MINUTE_LEVEL,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{HEART_RATE_MINUTE_LEVEL}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(HEART_RATE_MINUTE_LEVEL)}',
            'fields': ['person_id', 'heart_rate_value'],
            'loaded_ids': [1, 1],
            'sandboxed_ids': [1],
            'cleaned_values': [(1, 60)]
        }, {
            'name':
                STEPS_INTRADAY,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{STEPS_INTRADAY}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(STEPS_INTRADAY)}',
            'fields': ['person_id', 'steps'],
            'loaded_ids': [2, 2],
            'sandboxed_ids': [2],
            'cleaned_values': [(2, 100)]
        }, {
            'name':
                SLEEP_LEVEL,
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{SLEEP_LEVEL}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(SLEEP_LEVEL)}',
            'fields': ['person_id', 'duration_in_min'],
            'loaded_ids': [1, 1],
            'sandboxed_ids': [1],
            'cleaned_values': [(1, 4.5)]
        }]

        self.default_test(tables_and_counts)
