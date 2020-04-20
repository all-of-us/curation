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

        cls.insert_fake_participants_tmpls = [
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.death`
        (person_id, death_type_concept_id, death_date, death_datetime)
        VALUES
          (601, 0, date('2016-05-01'), null),
          (602, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00')),
          (603, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00')),
          (604, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.procedure_occurrence`
        (procedure_occurrence_id, person_id, procedure_concept_id, 
         procedure_type_concept_id, procedure_date, procedure_datetime)
        VALUES
        -- procedure_datetime cannot be null --
        -- (501, 111111, 0, 0, date('2016-05-01'), null), --
          (502, 222222, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00')),
          (503, 333333, 0, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00')),
          (504, 444444, 0, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.device_exposure`
        (device_exposure_id, person_id, device_concept_id, device_type_concept_id, device_exposure_start_date,
         device_exposure_start_datetime, device_exposure_end_date, device_exposure_end_datetime)
        VALUES
          -- device exposure start datetime cannot be null --
          -- (401, 111111, 0, 0, date('2016-05-01'), null, date('2016-05-02'), timestamp('2016-05-02 11:00:00')), --
          (402, 222222, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (403, 333333, 0, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (404, 444444, 0, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (405, 555555, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), null),
          (406, 666666, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (407, 777777, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-10 11:00:00')),
          (408, 888888, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-04-15 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.specimen`
        (specimen_id, person_id, specimen_concept_id, specimen_type_concept_id, specimen_date,
         specimen_datetime)
        VALUES
          (701, 111111, 0, 0, date('2016-05-01'), null),
          (702, 222222, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00')),
          (703, 333333, 0, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00')),
          (704, 444444, 0, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.measurement`
        (measurement_id, person_id, measurement_concept_id, measurement_type_concept_id, measurement_date,
         measurement_datetime)
        VALUES
          (301, 111111, 0, 0, date('2016-05-01'), null),
          (302, 222222, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00')),
          (303, 333333, 0, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00')),
          (304, 444444, 0, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.drug_exposure`
        (drug_exposure_id, person_id, drug_concept_id, drug_type_concept_id, drug_exposure_start_date,
         drug_exposure_start_datetime, drug_exposure_end_date, drug_exposure_end_datetime)
        VALUES
          -- drug_exposure_start_datetime cannot be null --
          -- (201, 111111, 0, 0, date('2016-05-01'), null, date('2016-05-02'), timestamp('2016-05-02 11:00:00')), --
          (202, 222222, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (203, 333333, 0, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (204, 444444, 0, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (205, 555555, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), null),
          (206, 666666, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (207, 777777, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-10 11:00:00')),
          (208, 888888, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-04-15 11:00:00'))
        """),
            cls.jinja_env.from_string("""
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
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, observation_datetime)
        VALUES
          (801, 337361, 0, date('2016-05-01'), 0, null),
          (802, 129884, 0, date('2016-05-01'), 0, timestamp('2016-05-01 11:00:00')),
          (803, 337361, 0, date('2016-05-01'), 0, timestamp('2016-05-08 11:00:00')),
          (804, 129884, 0, date('2016-05-01'), 0, timestamp('2016-04-07 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation_period`
        (observation_period_id, person_id, period_type_concept_id, observation_period_start_date,
         observation_period_start_datetime, observation_period_end_date, observation_period_end_datetime)
        VALUES
          (901, 111111, 0, date('2016-05-01'), null, date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (902, 222222, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (903, 333333, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (904, 444444, 0, date('2016-05-01'), timestamp('2016-04-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (905, 555555, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), null),
          (906, 666666, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (907, 777777, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-10 11:00:00')),
          (908, 888888, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-04-15 11:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.visit_occurrence`
        (visit_occurrence_id, person_id, visit_concept_id, visit_type_concept_id, 
         visit_start_date, visit_start_datetime, visit_end_date, visit_end_datetime)
        VALUES
          (111, 111111, 0, 0, date('2016-05-01'), null, date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (112, 222222, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (113, 333333, 0, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (114, 444444, 0, 0, date('2016-05-01'), timestamp('2015-05-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (115, 555555, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), null),
          (116, 666666, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 11:00:00')),
          (117, 777777, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-10 11:00:00')),
          (118, 888888, 0, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-04-15 11:00:00'))
        """)
        ]
        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        sandbox_id = dataset_id + '_sandbox'

        cls.query_class = EnsureDateDatetimeConsistency(project_id, dataset_id,
                                                        sandbox_id)

        sb_table_names = cls.query_class.get_sandbox_tablenames()
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
        Add data to the tables for the rule to run on.
        """
        load_statements = []
        # create the string(s) to load the data
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])
        for tmpl in self.insert_fake_participants_tmpls:
            query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
            load_statements.append(query)

        self.load_test_data(load_statements)

    def test(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """
        start_date = parser.parse('2016-05-01').date()
        start_datetime = parser.parse('2016-05-01 11:00:00 UTC')
        start_default_datetime = parser.parse('2016-05-01 00:00:00 UTC')
        end_date = parser.parse('2016-05-02').date()
        end_datetime = parser.parse('2016-05-02 11:00:00 UTC')
        end_default_datetime = parser.parse('2016-05-02 00:00:00 UTC')

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'specimen']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [701, 702, 703, 704],
            'sandboxed_ids': [],
            'fields': ['specimen_id', 'specimen_date', 'specimen_datetime'],
            'cleaned_values': [(701, start_date, start_default_datetime),
                               (702, start_date, start_datetime),
                               (703, start_date, start_datetime),
                               (704, start_date, start_datetime)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'measurement']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [301, 302, 303, 304],
            'sandboxed_ids': [],
            'fields': [
                'measurement_id', 'measurement_date', 'measurement_datetime'
            ],
            'cleaned_values': [(301, start_date, start_default_datetime),
                               (302, start_date, start_datetime),
                               (303, start_date, start_datetime),
                               (304, start_date, start_datetime)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'death']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [601, 602, 603, 604],
            'sandboxed_ids': [],
            'fields': ['person_id', 'death_date', 'death_datetime'],
            'cleaned_values': [(601, start_date, start_default_datetime),
                               (602, start_date, start_datetime),
                               (603, start_date, start_datetime),
                               (604, start_date, start_datetime)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'procedure_occurrence']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [502, 503, 504],
            'sandboxed_ids': [],
            'fields': [
                'procedure_occurrence_id', 'procedure_date',
                'procedure_datetime'
            ],
            'cleaned_values': [(502, start_date, start_datetime),
                               (503, start_date, start_datetime),
                               (504, start_date, start_datetime)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [801, 802, 803, 804],
            'sandboxed_ids': [],
            'fields': [
                'observation_id', 'observation_date', 'observation_datetime'
            ],
            'cleaned_values': [(801, start_date, start_default_datetime),
                               (802, start_date, start_datetime),
                               (803, start_date, start_datetime),
                               (804, start_date, start_datetime)]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation_period']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [901, 902, 903, 904, 905, 906, 907, 908],
            'sandboxed_ids': [],
            'fields': [
                'observation_period_id', 'observation_period_start_date',
                'observation_period_start_datetime',
                'observation_period_end_date', 'observation_period_end_datetime'
            ],
            'cleaned_values': [
                (901, start_date, start_default_datetime, end_date,
                 end_datetime),
                (902, start_date, start_datetime, end_date, end_datetime),
                (903, start_date, start_datetime, end_date, end_datetime),
                (904, start_date, start_datetime, end_date, end_datetime),
                (905, start_date, start_datetime, end_date,
                 end_default_datetime),
                (906, start_date, start_datetime, end_date, end_datetime),
                (907, start_date, start_datetime, end_date, end_datetime),
                (908, start_date, start_datetime, end_date, end_datetime)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'visit_occurrence']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [111, 112, 113, 114, 115, 116, 117, 118],
            'sandboxed_ids': [],
            'fields': [
                'visit_occurrence_id', 'visit_start_date',
                'visit_start_datetime', 'visit_end_date', 'visit_end_datetime'
            ],
            'cleaned_values': [
                (111, start_date, start_default_datetime, end_date,
                 end_datetime),
                (112, start_date, start_datetime, end_date, end_datetime),
                (113, start_date, start_datetime, end_date, end_datetime),
                (114, start_date, start_datetime, end_date, end_datetime),
                (115, start_date, start_datetime, end_date,
                 end_default_datetime),
                (116, start_date, start_datetime, end_date, end_datetime),
                (117, start_date, start_datetime, end_date, end_datetime),
                (118, start_date, start_datetime, end_date, end_datetime)
            ]
        }, {
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
            'cleaned_values': [
                (102, start_date, start_datetime, end_date, end_datetime),
                (103, start_date, start_datetime, end_date, end_datetime),
                (104, start_date, start_datetime, end_date, end_datetime),
                (105, start_date, start_datetime, end_date,
                 end_default_datetime),
                (106, start_date, start_datetime, end_date, end_datetime),
                (107, start_date, start_datetime, end_date, end_datetime),
                (108, start_date, start_datetime, end_date, end_datetime)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'drug_exposure']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [202, 203, 204, 205, 206, 207, 208],
            'sandboxed_ids': [],
            'fields': [
                'drug_exposure_id', 'drug_exposure_start_date',
                'drug_exposure_start_datetime', 'drug_exposure_end_date',
                'drug_exposure_end_datetime'
            ],
            'cleaned_values': [
                (202, start_date, start_datetime, end_date, end_datetime),
                (203, start_date, start_datetime, end_date, end_datetime),
                (204, start_date, start_datetime, end_date, end_datetime),
                (205, start_date, start_datetime, end_date,
                 end_default_datetime),
                (206, start_date, start_datetime, end_date, end_datetime),
                (207, start_date, start_datetime, end_date, end_datetime),
                (208, start_date, start_datetime, end_date, end_datetime)
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'device_exposure']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [402, 403, 404, 405, 406, 407, 408],
            'sandboxed_ids': [],
            'fields': [
                'device_exposure_id', 'device_exposure_start_date',
                'device_exposure_start_datetime', 'device_exposure_end_date',
                'device_exposure_end_datetime'
            ],
            'cleaned_values': [
                (402, start_date, start_datetime, end_date, end_datetime),
                (403, start_date, start_datetime, end_date, end_datetime),
                (404, start_date, start_datetime, end_date, end_datetime),
                (405, start_date, start_datetime, end_date,
                 end_default_datetime),
                (406, start_date, start_datetime, end_date, end_datetime),
                (407, start_date, start_datetime, end_date, end_datetime),
                (408, start_date, start_datetime, end_date, end_datetime)
            ]
        }]

        self.default_test(tables_and_counts)
