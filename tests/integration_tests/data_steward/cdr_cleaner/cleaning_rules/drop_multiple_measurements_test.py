"""
Integration test for drop_multiple_measurements module
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
        sandbox_id = f"{dataset_id}_sandbox"
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

    def test_drop_multiple_measurements(self):
        """
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
            (11, 1, 3022281, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 44818701, 903131, 'pre-pregnancy-weight', '83.5 kg'),
            (12, 1, 3022281, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903131, 'pre-pregnancy-weight', '81.5 kg'),
            (21, 2, 903111, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 44818701, 903111, 'wheelchair-user-status', 'wheelchair-user'),
            (22, 2, 903111, DATE('2015-09-15'), TIMESTAMP('2015-09-15'), 44818701, 903111, 'wheelchair-user-status', 'wheelchair-user'),
            (31, 3, 3036277, DATE('2015-08-15'), TIMESTAMP('2015-08-15 15:30:00 UTC'), 44818701, 903133, 'height', '154 cm'),
            (32, 3, 3036277, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 44818701, 903133, 'height', '155 cm'),
            (33, 3, 3036277, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903133, 'height', '156 cm'),
            (34, 9, 3036277, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903133, 'height', '156 cm'),
            -- Test case for in-person PM / self-report PM --
            (41, 4, 3038553, DATE('2015-07-15'), TIMESTAMP('2015-07-15'), 44818701, 903124, 'bmi', '20 kg/m2'),
            (42, 4, 3038553, DATE('2015-08-15'), TIMESTAMP('2015-08-15'), 44818701, 903124, 'bmi', '21 kg/m2'),
            (43, 4, 3038553, DATE('2015-07-16'), TIMESTAMP('2015-07-16'), 32865, 903124, 'bmi', '22 kg/m2'),
            (44, 4, 3038553, DATE('2015-08-16'), TIMESTAMP('2015-08-16'), 32865, 903124, 'bmi', '23 kg/m2'),
            (45, 4, 3025315, DATE('2015-07-17'), TIMESTAMP('2015-07-16'), 32865, 903121, 'weight', '60 kg'),
            (46, 4, 3025315, DATE('2015-08-17'), TIMESTAMP('2015-08-16'), 32865, 903121, 'weight', '61 kg')
        """)

        query = tmpl.render(fq_dataset_name=self.fq_dataset_name)
        self.load_test_data([query])

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'measurement']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                11, 12, 21, 22, 31, 32, 33, 34, 41, 42, 43, 44, 45, 46
            ],
            'sandboxed_ids': [12, 21, 32, 33, 41, 43, 45],
            'fields': ['measurement_id', 'value_source_value'],
            'cleaned_values': [(11, '83.5 kg'), (22, 'wheelchair-user'),
                               (31, '154 cm'), (34, '156 cm'), (42, '21 kg/m2'),
                               (44, '23 kg/m2'), (46, '61 kg')]
        }]

        self.default_test(tables_and_counts)
