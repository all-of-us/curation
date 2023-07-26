# Python Imports
import os

# Project Imports
from common import DEVICE, WEARABLES_DEVICE_ID_MASKING
from app_identity import PROJECT_ID

import cdr_cleaner.cleaning_rules.deid.fitbit_device_id as fit_dev_id
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


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

        cls.rule_instance = fit_dev_id.DeidFitbitDeviceId(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Store affected table names
        cls.affected_tables = [DEVICE]

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.device',
            f'{cls.project_id}.pipeline_tables.wearables_device_id_masking'
        ]

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

    def test_deid_device_id(self):

        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        map_query = self.jinja_env.from_string("""
            INSERT INTO `{{fq_table}}`
            (person_id, device_id, research_device_id, wearable_type, import_date)
            VALUES
                (21, '19', '54', 'fitbit', '2021-01-10'),
                (22, '18', '53', 'fitbit', '2021-01-09'),
                (23, '17', '52', 'fitbit', '2021-01-08'),
                (24, '16', '51', 'fitbit', '2021-01-07'),
                (24, '15', '50', 'fitbit', '2021-01-07'),
                (25, '14', '49', 'fitbit', '2021-01-06')""").render(
            fq_table=
            f'{self.project_id}.pipeline_tables.wearables_device_id_masking')

        device_query = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.device`
        (person_id, device_id)
        VALUES
            (21, '19'),
            (22, '18'),
            (23, '17'),
            (24, '16'),
            (24, '15'),
            (25, '14')
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([map_query, device_query])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEVICE]),
            'fields': ['person_id', 'device_id'],
            'loaded_ids': [21, 22, 23, 24, 24, 25],
            'cleaned_values': [(21, '54'), (22, '53'), (23, '52'), (24, '51'),
                               (24, '50'), (25, '49')]
        }]

        self.default_test(tables_and_counts)
