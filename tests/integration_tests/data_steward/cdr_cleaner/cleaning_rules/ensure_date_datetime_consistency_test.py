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
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from common import AOU_DEATH
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

        obs_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, observation_datetime)
        VALUES
          (801, 337361, 0, date('2016-05-01'), 0, null),
          (802, 129884, 0, date('2016-05-01'), 0, timestamp('2016-05-01 11:00:00')),
          (803, 337361, 0, date('2016-05-01'), 0, timestamp('2016-05-08 11:00:00')),
          (804, 129884, 0, date('2016-05-01'), 0, timestamp('2016-04-07 11:00:00'))
        """).render(fq_dataset_name=self.fq_dataset_name)

        aou_death_query = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.{{aou_death}}`
        (aou_death_id, person_id, death_date, death_datetime, death_type_concept_id,
         src_id, primary_death_record)
        VALUES
          ('aaa', 1, date('2016-05-01'), null, 0, 'foo', True),
          ('bbb', 2, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, 'foo', True),
          ('ccc', 3, date('2016-05-01'), timestamp('2016-05-08 11:00:00'), 0, 'foo', True),
          ('ddd', 4, date('2016-05-01'), timestamp('2016-04-07 11:00:00'), 0, 'foo', True),
          ('eee', 4, null, null, 0, 'foo', False)
        """).render(fq_dataset_name=self.fq_dataset_name, aou_death=AOU_DEATH)

        self.load_test_data([obs_query, aou_death_query])

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
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, AOU_DEATH]),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': ['aaa', 'bbb', 'ccc', 'ddd', 'eee'],
            'sandboxed_ids': [],
            'fields': ['aou_death_id', 'death_date', 'death_datetime'],
            'cleaned_values': [('aaa', self.start_date,
                                self.start_default_datetime),
                               ('bbb', self.start_date, self.start_datetime),
                               ('ccc', self.start_date, self.start_datetime),
                               ('ddd', self.start_date, self.start_datetime),
                               ('eee', None, None)]
        }]

        self.default_test(tables_and_counts)

    def test_required_field_datetime_pairs(self):
        """
        Tests possible values for date and datetime field pairs.

        This tests having more than 1 date/datetime field pair in a single record.
        The datetime is required in one pair and may be nullable in the
        other comparison pair.
        """
        condition_tmpl = self.jinja_env.from_string("""
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

        note_tmpl = self.jinja_env.from_string("""
                INSERT INTO `{{fq_dataset_name}}.note`
                (note_id, person_id, note_date, note_datetime, note_type_concept_id, note_class_concept_id,
                 note_title, note_text, encoding_concept_id, language_concept_id)
                VALUES
                  (101, 222222, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 0, 0, '', '', 0, 0 ),
                  (102, 333333, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), 0, 0, '', '', 0, 0)
                """)
        condition_query = condition_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)
        note_query = note_tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([condition_query, note_query])

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
                                self.end_date,
                                parser.parse('2016-05-02 11:59:59 UTC')),
                               (106, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (107, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime),
                               (108, self.start_date, self.start_datetime,
                                self.end_date, self.end_datetime)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'note']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [101, 102],
            'sandboxed_ids': [],
            'fields': [
                'note_id',
                'note_date',
                'note_datetime',
            ],
            'cleaned_values': [(
                101,
                self.start_date,
                self.start_datetime,
            ), (102, self.start_date, self.start_datetime)]
        }]

        self.default_test(tables_and_counts)

    def test_end_datetime_reset(self):
        """
        Tests that end_datetime fields are assigned a time component of 11:59:59
        """
        self.maxDiff = None
        tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.drug_exposure`
        (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date,
         drug_exposure_start_datetime, drug_exposure_end_date, drug_exposure_end_datetime, drug_type_concept_id)
        VALUES
          (101, 1, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-09-05'), NULL, 0),
          (102, 2, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-09-05'), timestamp('2016-09-05 13:12:45'), 0),
          (103, 1, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-09-05'), timestamp('2016-09-06 13:12:45'), 0),
          (104, 3, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-09-05'), timestamp('2016-09-05 00:00:00'), 0)
        """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'drug_exposure']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [101, 102, 103, 104],
            'sandboxed_ids': [],
            'fields': [
                'drug_exposure_id', 'person_id', 'drug_concept_id',
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_exposure_end_date', 'drug_exposure_end_datetime',
                'drug_type_concept_id'
            ],
            'cleaned_values': [(101, 1, 0, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-09-05').date(),
                                parser.parse('2016-09-05 11:59:59 UTC'), 0),
                               (102, 2, 0, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-09-05').date(),
                                parser.parse('2016-09-05 13:12:45 UTC'), 0),
                               (103, 1, 0, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-09-05').date(),
                                parser.parse('2016-09-05 13:12:45 UTC'), 0),
                               (104, 3, 0, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-09-05').date(),
                                parser.parse('2016-09-05 00:00:00 UTC'), 0)]
        }]

        self.default_test(tables_and_counts)
