# Python Imports
import os
from datetime import datetime

# Project Imports
from app_identity import PROJECT_ID
import cdr_cleaner.cleaning_rules.deid.fitbit_pid_rid_map as pr
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DEVICE


class FitbitDeviceIdTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        self.project_id='aou-res-curation-test'
        self.dataset_id='??'

        super().initialize_class_vars()
        super().setUpClass()

    def setUp(self):

        query = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.device`
        (person_id, research_device_id, device_id, wearable_type)
        VALUES
            (1, 234, 432, 'fitbit'),
            (2, 678, 876, 'fitbit'),
            (3, 345, 543, 'fitbit'),
            (4, 789, 987, 'fitbit'),""").render(
            project_id=self.project_id,
            dataset_id=self.dataset_id
            )

        super().setUp()

    def test_field_cleaning(self):

        # Expected results list
        tables_and_counts = [
        {
            'fq_table_name':
                '.'.join([self.fq_dataset_name, DEVICE]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[5],
            'fields': ['person_id', 'research_id', 'device_id'],
            'loaded_ids': [1, 2, 3, 4],
            'cleaned_values': [
                (1, 234, 234),
                (2, 678, 678),
                (3, 345, 345),
                (4, 789, 789,)
            ]
        }]

        self.default_test(tables_and_counts)
