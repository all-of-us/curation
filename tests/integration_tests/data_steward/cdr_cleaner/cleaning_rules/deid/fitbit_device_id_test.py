# Python Imports
import os

# Project Imports
from common import DEVICE, WEARABLES_DEVICE_ID_MASKING
from app_identity import PROJECT_ID

import cdr_cleaner.cleaning_rules.deid.fitbit_device_id as fdi
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

        mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        mapping_table_id = WEARABLES_DEVICE_ID_MASKING
        cls.mapping_dataset_id = mapping_dataset_id
        cls.kwargs.update({
            'mapping_dataset_id': mapping_dataset_id,
            'mapping_table_id': mapping_table_id
        })
        cls.fq_deid_map_table = f'{cls.project_id}.{mapping_dataset_id}.{mapping_table_id}'

        cls.rule_instance = fdi.DeidFitbitDeviceId(cls.project_id,
                                                  cls.dataset_id,
                                                  cls.sandbox_id)

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_id)}'
            for table_id in fdi.FITBIT_TABLES
        ]

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table_id}'
            for table_id in fdi.FITBIT_TABLES
        ] + [cls.fq_deid_map_table
            ] + [f'{cls.project_id}.{mapping_dataset_id}.person']

        super().setUpClass()

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

        self.default_test(tables_and_counts)
