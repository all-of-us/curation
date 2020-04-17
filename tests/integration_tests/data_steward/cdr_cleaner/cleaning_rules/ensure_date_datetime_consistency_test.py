"""
Integration Test for the rdr_observation_source_concept_id_suppression module.

Remove three irrelevant observation_source_concept_ids from the RDR dataset.

Original Issue:  DC-734 implements integration tests for DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.  It should
also ensure that records that have null values or do not match the specified
ids are not removed.
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
        INSERT INTO `{{fq_dataset_name}}.drug_exposure`
        (drug_exposure_id, person_id, drug_concept_id, drug_type_concept_id, drug_exposure_start_date,
         drug_exposure_start_datetime, drug_exposure_end_date, drug_exposure_end_datetime)
        VALUES
          (202, 222222, 40164851, 0, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 00:00:00')),
          (203, 333333, 40164851, 0, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 00:00:00')),
          (204, 444444, 40164851, 0, date('2016-05-01'), timestamp('2016-04-01 11:00:00'), date('2016-05-02'), timestamp('2016-05-02 00:00:00')),
          (205, 555555, 40164851, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-05-02'), null),
          (206, 666666, 40164851, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-05-02'), timestamp('2016-05-02 08:00:00')),
          (207, 777777, 40164851, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-05-02'), timestamp('2016-05-10 08:00:00')),
          (208, 888888, 40164851, 0, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), date('2016-05-02'), timestamp('2016-04-15 08:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.condition_occurrence`
        (condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
         condition_start_datetime, condition_type_concept_id, condition_end_date, condition_end_datetime)
        VALUES
          (102, 222222, 4329041, date('2016-05-01'), timestamp('2016-05-01 11:00:00'), 44786629, date('2016-05-02'), timestamp('2016-05-02 00:00:00')),
          (103, 333333, 4329041, date('2016-05-01'), timestamp('2016-05-07 11:00:00'), 44786629, date('2016-05-02'), timestamp('2016-05-02 00:00:00')),
          (104, 444444, 4329041, date('2016-05-01'), timestamp('2015-05-07 11:00:00'), 44786629, date('2016-05-02'), timestamp('2016-05-02 00:00:00')),
          (105, 555555, 4329041, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), 44786629, date('2016-05-02'), null),
          (106, 666666, 4329041, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), 44786629, date('2016-05-02'), timestamp('2016-05-02 08:00:00')),
          (107, 777777, 4329041, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), 44786629, date('2016-05-02'), timestamp('2016-05-10 08:00:00')),
          (108, 888888, 4329041, date('2016-05-01'), timestamp('2016-05-01 00:00:00'), 44786629, date('2016-05-02'), timestamp('2016-04-15 08:00:00'))
        """),
            cls.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.observation`
        (observation_id, person_id, observation_concept_id, observation_date,
         observation_type_concept_id, observation_datetime)
        VALUES
          (801, 337361, 1585899, date('2016-05-01'), 45905771, null),
          (802, 129884, 1585899, date('2016-05-01'), 45905771, timestamp('2016-05-01 09:00:00')),
          (803, 337361, 1585899, date('2016-05-01'), 45905771, timestamp('2016-05-08 10:00:00')),
          (804, 129884, 1585899, date('2016-05-01'), 45905771, timestamp('2015-05-07 09:00:00'))
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
        self.maxDiff = None

    def test(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [801, 802, 803, 804],
            'sandboxed_ids': [],
            'fields': ['observation_id', 'observation_datetime'],
            'cleaned_values': [(801, parser.parse('2016-05-01 00:00:00 UTC')),
                               (802, parser.parse('2016-05-01 09:00:00 UTC')),
                               (803, parser.parse('2016-05-01 10:00:00 UTC')),
                               (804, parser.parse('2016-05-01 09:00:00 UTC'))]
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
            'cleaned_values': [(102, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 11:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (103, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 11:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (104, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 11:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (105, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (106, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 08:00:00 UTC')),
                               (107, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 08:00:00 UTC')),
                               (108, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 08:00:00 UTC'))]
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
            'cleaned_values': [(202, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 11:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (203, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 11:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (204, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 11:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (205, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 00:00:00 UTC')),
                               (206, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 08:00:00 UTC')),
                               (207, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 08:00:00 UTC')),
                               (208, parser.parse('2016-05-01').date(),
                                parser.parse('2016-05-01 00:00:00 UTC'),
                                parser.parse('2016-05-02').date(),
                                parser.parse('2016-05-02 08:00:00 UTC'))]
        }]

        self.default_test(tables_and_counts)
