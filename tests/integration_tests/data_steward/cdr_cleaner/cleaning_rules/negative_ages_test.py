"""
Integration test for negative_ages.py

Age should not be negative for the person at any dates/start dates.
Using rule 20, 21 in Achilles Heel for reference.
Also ensure ages are not beyond 150.

Original Issues: DC-393, DC-811, DC-1230
"""

# Python Imports
import os
from datetime import datetime

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.negative_ages import NegativeAges
from common import AOU_DEATH, DEATH, MEASUREMENT, OBSERVATION, PERSON
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class NegativeAgesTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = NegativeAges(project_id, dataset_id, sandbox_id)

        # adding person table for setup/cleanup utilities
        for table in cls.rule_instance.affected_tables + [PERSON]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table)}'
            )

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.
        
        Creates common expected parameter types from cleaned tables and a common fully qualified (fq)
        dataset name string used to load the data.
        
        Creates tables with test data for the rule to run on
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

        # test data for person table
        person_data_query = self.jinja_env.from_string("""
            INSERT INTO
                `{{project_id}}.{{dataset_id}}.person`
                    (person_id, birth_datetime, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES
                (1, '1988-12-29 15:00:00', 0, 1988, 0, 0),
                (2, '1980-11-20 15:00:00', 0, 1980, 0, 0),
                (3, '2020-09-17 15:00:00', 0, 2020, 0, 0),
                (4, '1861-09-17 15:00:00', 0, 1861, 0, 0),
                (5, '1930-03-15 15:00:00', 0, 1930, 0, 0),
                (5, '1940-04-09 15:00:00', 0, 1940, 0, 0)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for negative age at recorded time in table
        measurement_data_query = self.jinja_env.from_string("""
            INSERT INTO
                  `{{project_id}}.{{dataset_id}}.measurement` 
                  (measurement_id, person_id, measurement_date, measurement_type_concept_id, measurement_concept_id)
            VALUES
                  (123, 1, date('2020-01-17'), 0, 0),
                  (456, 2, date('2020-03-17'), 0, 0),
                  (789, 3, date('2019-08-17'), 0, 0)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for age > MAX_AGE (=150) at recorded time in table
        observation_data_query = self.jinja_env.from_string("""
            INSERT INTO
                `{{project_id}}.{{dataset_id}}.observation`
                (observation_id, person_id, observation_date,
                 observation_concept_id, observation_type_concept_id)
            VALUES
                (111, 1, date('2019-07-04'),0 ,0),
                (222, 2, date('2020-02-13'),0 ,0),
                (333, 3, date('2021-02-17'),0 ,0),
                (444, 4, date('2021-01-17'),0 ,0)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for negative age at death
        death_data_query = self.jinja_env.from_string("""
            INSERT INTO
                `{{project_id}}.{{dataset_id}}.death`
                (person_id, death_date, death_type_concept_id)
            VALUES
                (5, date('1915-05-05'), 0),
                (6, date('2020-08-15'), 0)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for negative age at aou_death
        aou_death_data_query = self.jinja_env.from_string("""
            INSERT INTO
                `{{project_id}}.{{dataset_id}}.aou_death`
                (aou_death_id, person_id, death_date, death_type_concept_id, src_id, primary_death_record)
            VALUES
                ('a5', 5, date('1915-05-05'), 0, 'rdr', False),
                ('b5', 5, date('2020-08-15'), 0, 'hpo_b', False),
                ('c5', 5, date('1900-05-05'), 0, 'hpo_c', True),
                ('d5', 5, date('2022-08-15'), 0, 'hpo_d', False),
                ('a6', 6, date('2020-08-15'), 0, 'rdr', False)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([
            person_data_query, death_data_query, measurement_data_query,
            observation_data_query, aou_death_data_query
        ])

    def test_negative_ages_cleaning(self):
        """
        Tests that the records with negative age or beyond 150 are sandboxed and removed

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, MEASUREMENT]),
            'fields': ['measurement_id', 'person_id', 'measurement_date'],
            'loaded_ids': [123, 456, 789],
            'cleaned_values': [
                (123, 1, datetime.fromisoformat('2020-01-17').date()),
                (456, 2, datetime.fromisoformat('2020-03-17').date())
            ]
        }, {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fields': ['observation_id', 'person_id', 'observation_date'],
            'loaded_ids': [111, 222, 333, 444],
            'cleaned_values': [
                (111, 1, datetime.fromisoformat('2019-07-04').date()),
                (222, 2, datetime.fromisoformat('2020-02-13').date()),
                (333, 3, datetime.fromisoformat('2021-02-17').date())
            ]
        }, {
            'fq_table_name': '.'.join([self.fq_dataset_name, DEATH]),
            'fields': ['person_id', 'death_date'],
            'loaded_ids': [5, 6],
            'cleaned_values': [(6, datetime.fromisoformat('2020-08-15').date())]
        }, {
            'fq_table_name': '.'.join([self.fq_dataset_name, AOU_DEATH]),
            'fields': ['aou_death_id'],
            'loaded_ids': ['a5', 'b5', 'c5', 'd5', 'a6'],
            'cleaned_values': [('b5',), ('d5',), ('a6',)]
        }]

        self.default_test(tables_and_counts)
