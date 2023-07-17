# Python Imports
import os

# Project Imports
import cdr_cleaner.cleaning_rules.deid.fitbit_pid_rid_map as pr
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DEVICE


class FitbitDeviceIdTest():

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        self.project_id = os.getenv('PROJECT_ID')
        self.dataset_id = os.environ['FITBIT_DATASET_ID']

        super().initialize_class_vars()
        super().setUpClass()

    def setUp(self):

        map_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_table}}`
            (person_id, device_id, research_device_id)
            VALUES
                (21, 19, 54),
                (22, 18, 53),
                (23, 17, 52),
                (24, 16, 51)""").render(fq_table=self.fq_deid_map_table)

        self.client.query(map_query)

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

        super().setUp()

    def test_field_cleaning(self):

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

        self.default_test(tables_and_counts)
