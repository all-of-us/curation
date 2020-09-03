"""
Integration Test for the ensure_date_datetime_consistency module.

The intent is to make sure datetime fields are always set if available.
The date field is considered the authritative field because it is always
required.  If an unresolved timezone change causes a date difference between
the date and datetime field, the date field value is preferred.  If a time
is provided in the field, the time is maintained.  If the datetime field
was null, a default value of midnight is used for the time component.
"""
# Python imports
from datetime import date
import os

# Third party imports
from dateutil import parser
from jinja2 import Template

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency import EnsureDateDatetimeConsistency, TABLE_DATES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class EnsureDateDatetimeConsistencyTest(BaseTest.CleaningRulesTestBase):

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
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = EnsureDateDatetimeConsistency(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        for key in TABLE_DATES:
            cls.fq_table_names.append(f"{project_id}.{dataset_id}.{key}")

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and
        a common fully qualified dataset name string used to load
        the data.
        """
        self.start_date = parser.parse('2016-05-01').date()
        self.start_datetime = parser.parse('2016-05-01 11:00:00 UTC')
        self.start_default_datetime = parser.parse('2016-05-01 00:00:00 UTC')
        self.end_date = parser.parse('2016-05-02').date()
        self.end_datetime = parser.parse('2016-05-02 11:00:00 UTC')
        self.end_default_datetime = parser.parse('2016-05-02 00:00:00 UTC')

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_nullable_field_datetime_pair(self):
        """
        Tests possible values of a required date and nullable datetime field pair.
        """

        tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, observation_datetime)
        VALUES
          (801, 337361, 0, date('2016-05-01'), 0, null),
          (802, 129884, 0, date('2016-05-01'), 0, timestamp('2016-05-01 11:00:00')),
          (803, 337361, 0, date('2016-05-01'), 0, timestamp('2016-05-08 11:00:00')),
          (804, 129884, 0, date('2016-05-01'), 0, timestamp('2016-04-07 11:00:00'))
        """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [801, 802, 803, 804],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'observation_date', 'observation_datetime'
            ],
            'cleaned_values': [(801, self.start_date,
                                self.start_default_datetime),
                               (802, self.start_date, self.start_datetime),
                               (803, self.start_date, self.start_datetime),
                               (804, self.start_date, self.start_datetime)]
        }]

        self.default_test(tables_and_counts)

    def test_required_field_datetime_pairs(self):
        """
        Tests possible values for date and datetime field pairs.

        This tests having more than 1 date/datetime field pair in a single record.
        The datetime is required in one pair and may be nullable in the
        other comparison pair.
        """
        tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.condition_occurrence`
        (condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
         condition_start_datetime, condition_type_concept_id, condition_end_date, condition_end_datetime)
        VALUES
          -- condition_start_datetime cannot be null --
          -- (101, 111111, 0, date('2016-05-01'), null, 0, date('2016-05-02'), timestamp('2016-05-02 11:00:00')), --
          (102, 222222, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (103, 333333, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), 0, date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (104, 444444, 0, date('2016-05-01'), timestamp('2015-05-07 11:00:00'), 0, date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (105, 555555, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, date('2016-05-02'), null),
          (106, 666666, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (107, 777777, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, date('2016-05-02'), timestamp('2016-05-10 11:00:00')),
          (108, 888888, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, date('2016-05-02'), timestamp('2016-04-15 11:00:00'))
        """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'condition_occurrence']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [102, 103, 104, 105, 106, 107, 108],
            'sandboxed_ids': [],
            'fields': [
                'condition_occurrence_id', 'condition_start_date',
                'condition_start_datetime', 'condition_end_date',
                'condition_end_datetime'
            ],
            'cleaned_values': [(102, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (103, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (104, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (105, self.start_date, self.start_datetime,
                                self.end_date, self.end_default_datetime),
                               (106, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (107, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (108, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime)]
        }]

        self.default_test(tables_and_counts)
