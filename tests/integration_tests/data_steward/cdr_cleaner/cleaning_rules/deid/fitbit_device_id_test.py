# Python Imports
import os

# Project Imports
import cdr_cleaner.cleaning_rules.deid.fitbit_pid_rid_map as pr
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DEVICE


class FitbitDeviceIdTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        # using unioned since we don't declare a deid dataset
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')

        def setUp(self):
            """
            Create common information for tests.

            Creates common expected parameter types from cleaned tables and a common
            fully qualified (fq) dataset name string to load the data.
            """
            self.value_as_number = None

            fq_dataset_name = self.fq_table_names[0].split('.')
            self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

            super().setUp()

    def test_field_cleaning(self):

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        map_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_table}}`
            (person_id, device_id, research_device_id)
            VALUES
                (21, 19, 54),
                (22, 18, 53),
                (23, 17, 52),
                (24, 16, 51)""").render(fq_table=self.fq_deid_map_table)

        device_query = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.device`
        (person_id, device_id)
        VALUES
            (21, 19),
            (22, 18),
            (23, 17),
            (24, 16)""").render(project_id=self.project_id,
                                dataset_id=self.dataset_id)

        self.load_test_data([map_query, device_query])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEVICE]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[5],
            'fields': ['person_id', 'device_id'
                       'research_device_id'],
            'loaded_ids': [21, 22, 23, 24],
            'cleaned_values': [
                (21, 19, 19),
                (22, 18, 18),
                (23, 17, 17),
                (
                    24,
                    16,
                    16,
                ),
            ]
        }]

        super().setUp()

    def test_field_cleaning(self):

        pass

        #self.default_test(tables_and_counts)
