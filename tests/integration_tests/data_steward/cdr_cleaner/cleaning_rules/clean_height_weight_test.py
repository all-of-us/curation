"""
Integration Test for the clean_height_weight module.

Normalizes all height and weight data into cm and kg and removes invalid/implausible data points (rows)

Original Issue: DC-701

The intent is to delete zero/null/implausible height/weight rows and inserting normalized rows (cm and kg)
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.clean_height_weight import CleanHeightAndWeight
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CleanHeightAndWeightTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        sandbox_id = dataset_id + '_sandbox'

        cls.query_class = CleanHeightAndWeight(project_id, dataset_id,
                                               sandbox_id)

        sb_table_names = cls.query_class.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.measurement',
            f'{project_id}.{dataset_id}.concept',
            f'{project_id}.{dataset_id}.person',
            f'{project_id}.{dataset_id}.measurement_ext',
            f'{project_id}.{dataset_id}.condition_occurrence',
            f'{project_id}.{dataset_id}.concept_ancestor'
        ]

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

    def test_field_cleaning(self):
        """
        Tests that the specifications for all the queries perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        measurement_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.measurement`
            (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id)
            VALUES
                (123, 111111, 3036277, date('2020-07-01'), 321),
                (234, 222222, 3023540, date('2020-07-01'), 432),
                (345, 333333, 3019171, date('2020-07-01'), 543),
                (456, 444444, 1234567, date('2020-07-01'), 654),
                (567, 555555, 3025315, date('2020-07-01'), 765),
                (678, 666666, 3013762, date('2020-07-01'), 876),
                (789, 777777, 3023166, date('2020-07-01'), 987)""")

        concept_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.concept`
            (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code,
                valid_start_date, valid_end_date)
            VALUES
                (123, 'foo', 'foo', 'foo', 'foo', '0', date('2020-06-30'), date('2020-07-01')),
                (234, 'bar', 'bar', 'bar', 'bar', '0', date('2020-06-30'), date('2020-07-01')),
                (345, 'baz', 'baz', 'baz', 'baz', '0', date('2020-06-30'), date('2020-07-01')),
                (456, 'fizz', 'fizz', 'fizz', 'fizz', '0', date('2020-06-30'), date('2020-07-01'))"""
                                                 )

        person_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.person`
            (person_id, gender_concept_id, year_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id)
            VALUES
                (123, 1111, 1986, timestamp('1986-08-15 11:00:00'), 0, 0),
                (234, 2222, 1979, timestamp('1979-04-15 11:00:00'), 0, 0),
                (345, 3333, 1951, timestamp('1951-09-07 11:00:00'), 0, 0),
                (456, 4444, 1950, timestamp('1952-03-28 11:00:00'), 0, 0)""")

        measurement_ext_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.measurement_ext`
            (measurement_id, src_id)
            VALUES
                (111111, 'foo'),
                (222222, 'bar'),
                (333333, 'baz')""")

        condition_occurrence_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.condition_occurrence`
            (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime,
                condition_type_concept_id)
            VALUES
                (1111, 123, 434005, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (2222, 234, 37018860, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (3333, 345, 439141, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (4444, 456, 4074213, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (5555, 567, 45771307, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (6666, 678, 4100857, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (7777, 789, 42539192, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0),
                (8888, 890, 1234567, date('2015-01-01'), timestamp('2015-01-01 11:00:00'), 0)"""
                                                              )

        concept_ancestor_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.concept_ancestor`
        (ancestor_concept_id, descendant_concept_id, min_levels_of_separation, max_levels_of_separation)
        VALUES
            (1234, 4321, 10, 20),
            (2345, 5432, 10, 20),
            (3456, 6543, 10, 20),
            (4567, 7654, 10, 20)""")

        measurement_query = measurement_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)
        concept_query = concept_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)
        person_query = person_tmpl.render(fq_dataset_name=self.fq_dataset_name)
        measurement_ext_query = measurement_ext_tmpl.render(fq_dataset_name=self.fq_dataset_name)
        condition_occurrence_query = condition_occurrence_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)
        concept_ancestor_query = concept_ancestor_tmpl.render(
            fq_dataset_name=self.fq_dataset_name)

        self.load_test_data([
            measurement_query, concept_query, person_query, measurement_ext_query,
            condition_occurrence_query, concept_ancestor_query
        ])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': [
                self.fq_sandbox_table_names[0], self.fq_sandbox_table_names[1],
                self.fq_sandbox_table_names[2], self.fq_sandbox_table_names[3]
            ],
            'loaded_ids': [123, 234, 345, 456, 567, 678, 789],
            'sandboxed_ids': [123, 234, 345, 567, 678, 789],
            'fields': [
                'measurement_id', 'measurement_concept_id',
                'measurement_type_concept_id'
            ],
            'cleaned_values': [(456, 1234567, 654)]
        }]

        self.default_test(tables_and_counts)
