"""
Integration test for drop_multiple_measurements module

Removes all but the most recent of each Physical Measurement for all participants.

Original Issues: DC-847

It is possible for a participant to have multiple records of Physical Measurements. This typically occurs when earlier
entries are incorrect. Data quality would improve if these earlier entries were removed.  A cleaning rule was developed
to remove all but the most recent of each Physical Measurement for all participants.  This rule groups measurements from
a set list of measurement_source_concept_ids by person_id and order by measurement_datetime, keeping only the newest
record for each person and measurement.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drop_multiple_measurements import DropMultipleMeasurements
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class DropMultipleMeasurementsTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = DropMultipleMeasurements(project_id, dataset_id,
                                                     sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [f'{project_id}.{dataset_id}.measurement']

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

    def test_field_cleaning(self):
        """
        Tests that the specifications for the SANDBOX_INVALID_MULT_MEASUREMENTS_QUERY and REMOVE_INVALID_MULT_MEASUREMENTS_QUERY
        perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.measurement` (
                measurement_id,
                person_id,
                measurement_concept_id,
                measurement_date,
                measurement_datetime,
                measurement_type_concept_id,
                measurement_source_concept_id,
                measurement_source_value,
                value_source_value)
            VALUES
            (123, 1111111, 3022281, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 44818701, 903131,
            'pre-pregnancy-weight', '83.5 kg'),
            (234, 1111111, 3022281, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903131,
            'pre-pregnancy-weight', '81.5 kg'),
            (345, 2222222, 3038553, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903124,
            'bmi', '101.1 kg/m2'),
            (456, 2222222, 3038553, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 44818701, 903124,
            'bmi', '83.2 kg/m2'),
            (567, 3333333, 3036277, DATE('2015-08-15'), TIMESTAMP('2015-08-15 15:30:00 UTC'), 44818701, 903133,
            'height','154 cm'),
            (678, 3333333, 3036277, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 44818701, 903133,
            'height','155 cm'),
            (789, 3333333, 3036277, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903133,
            'height','156 cm'),
            (890, 4444444, 903111, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 44818701, 903111,
            'wheelchair-user-status','wheelchair-user'),
            (901, 4444444, 903111, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 44818701, 903111,
            'wheelchair-user-status','wheelchair-user')
        """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'measurement']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [123, 234, 345, 456, 567, 678, 789, 890, 901],
            'sandboxed_ids': [234, 345, 678, 789, 890],
            'fields': ['measurement_id', 'value_source_value'],
            'cleaned_values': [(123, '83.5 kg'), (456, '83.2 kg/m2'),
                               (567, '154 cm'), (901, 'wheelchair-user')]
        }]

        self.default_test(tables_and_counts)
