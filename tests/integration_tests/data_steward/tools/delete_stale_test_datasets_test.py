"""
Unit test for delete_stale_test_buckets module
"""

# Python imports
import os
from unittest import TestCase
from unittest.mock import patch, Mock
from datetime import datetime, timedelta, timezone

from google.api_core.exceptions import NotFound

# Third party imports

# Project imports
from tools import delete_stale_test_datasets
from utils import bq


class DeleteStaleTestDatasetsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def test_check_project(self):
        """Integration test: bq.get_client() works and it's in TEST environment.
        """
        bq_client = bq.get_client(os.environ.get('GOOGLE_CLOUD_PROJECT'))

        self.assertIsNone(delete_stale_test_datasets._check_project(bq_client))

    def test_filter_stale_datasets(self):
        """Integration test: 
        delete_stale_test_datasets() returns only stale datasets, and
        delete_stale_test_datasets() returns at most first_n datasets
        """
        bq_client = bq.get_client(os.environ.get('GOOGLE_CLOUD_PROJECT'))

        result = delete_stale_test_datasets._filter_stale_datasets(
            bq_client, 10)

        # Assert: Returns at most first_n datasets
        self.assertLessEqual(len(result), 10)

        now = datetime.now(timezone.utc)
        for dataset_name in result:
            dataset_created = bq_client.get_dataset(dataset_name).created

            # Assert: Returns only stale datasets (1: 90 days or older)
            self.assertGreaterEqual((now - dataset_created).days, 90)

            # Assert: Returns only stale datasets (2: Empty(=no tables))
            self.assertIsNone(next(bq_client.list_tables(dataset_name), None))

    def test_run_deletion(self):
        """Integration test: 
        _run_deletion is deletes the specified dataset and nothing else.
        """
        bq_client = bq.get_client(os.environ.get('GOOGLE_CLOUD_PROJECT'))

        dummy_dataset = 'test_dataset_dc_1901'

        bq_client.create_dataset(dummy_dataset, exists_ok=True)

        # This will throw error if the dummy dataset is not created.
        bq_client.get_dataset(dummy_dataset)

        num_datasets_before_run = len(list(bq_client.list_datasets()))

        result = delete_stale_test_datasets._run_deletion(
            bq_client, dummy_dataset)

        num_datasets_after_run = len(list(bq_client.list_datasets()))

        self.assertIsNone(result)

        # Assert the dummy dataset is deleted.
        with self.assertRaises(NotFound):
            bq_client.get_dataset(dummy_dataset)

        # Assert only one dataset is deleted.
        self.assertEqual(num_datasets_before_run - num_datasets_after_run, 1)
