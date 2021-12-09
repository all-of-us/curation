"""
Unit test for delete_stale_test_buckets module
"""

# Python imports
import os
from unittest import TestCase
from unittest.mock import patch, Mock
from datetime import datetime, timedelta, timezone

# Third party imports

# Project imports
from tools import delete_stale_test_datasets


class DeleteStaleTestDatasetsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.mock_old_dataset_list_item_1 = Mock()
        self.mock_old_dataset_list_item_1.dataset_id = 'all_of_us_dummy_old_dataset_1'

        self.mock_old_dataset_list_item_2 = Mock()
        self.mock_old_dataset_list_item_2.dataset_id = 'all_of_us_dummy_old_dataset_2'

        self.mock_new_dataset_list_item = Mock()
        self.mock_new_dataset_list_item.dataset_id = 'all_of_us_dummy_new_dataset'

        self.dataset_list_items = [
            self.mock_old_dataset_list_item_1,
            self.mock_old_dataset_list_item_2, self.mock_new_dataset_list_item
        ]

        self.mock_old_dataset_1 = Mock()
        self.mock_old_dataset_1.created = datetime.now(
            timezone.utc) - timedelta(days=365)

        self.mock_old_dataset_2 = Mock()
        self.mock_old_dataset_2.created = datetime.now(
            timezone.utc) - timedelta(days=180)

        self.mock_new_dataset = Mock()
        self.mock_new_dataset.created = datetime.now(
            timezone.utc) - timedelta(days=1)

        self.datasets = [
            self.mock_old_dataset_1, self.mock_old_dataset_2,
            self.mock_new_dataset
        ]

        self.mock_table = Mock()

    @patch('tools.delete_stale_test_datasets.bq')
    def test_check_project(self, mock_bq):
        mock_bq.get_client.project = os.environ.get('GOOGLE_CLOUD_PROJECT')

        self.assertIsNone(
            delete_stale_test_datasets._check_project(mock_bq.get_client))

    @patch('tools.delete_stale_test_datasets.bq')
    def test_check_project_error(self, mock_bq):
        mock_bq.get_client.project = 'aou-wrong-project-name'

        with self.assertRaises(ValueError):
            delete_stale_test_datasets._check_project(mock_bq.get_client)

    @patch('tools.delete_stale_test_datasets.bq')
    def test_filter_stale_datasets_all_not_empty(self, mock_bq):
        """Test case: All datasets are NOT empty.
        """
        mock_bq.get_client.list_datasets.return_value = self.dataset_list_items
        mock_bq.get_client.get_dataset.side_effect = self.datasets
        mock_bq.get_client.list_tables.return_value = [
            self.mock_table for _ in range(0, 3)
        ]

        result = delete_stale_test_datasets._filter_stale_datasets(
            mock_bq.get_client, 100)

        self.assertEqual(result, [])

    @patch('tools.delete_stale_test_datasets.bq')
    def test_filter_stale_datasets_all_empty(self, mock_bq):
        """Test case: All datasets are empty.
        """
        mock_bq.get_client.list_datasets.return_value = self.dataset_list_items
        mock_bq.get_client.get_dataset.side_effect = self.datasets
        mock_bq.get_client.list_tables.return_value = []

        result = delete_stale_test_datasets._filter_stale_datasets(
            mock_bq.get_client, 100)

        self.assertEqual(result, [
            self.mock_old_dataset_list_item_1.dataset_id,
            self.mock_old_dataset_list_item_2.dataset_id
        ])

    @patch('tools.delete_stale_test_datasets.bq')
    def test_filter_stale_datasets_first_n_not_given(self, mock_bq):
        """Test case: All buckets are empty. first_n not given.
        """
        mock_bq.get_client.list_datasets.return_value = self.dataset_list_items
        mock_bq.get_client.get_dataset.side_effect = self.datasets
        mock_bq.get_client.list_tables.return_value = []

        result = delete_stale_test_datasets._filter_stale_datasets(
            mock_bq.get_client)

        self.assertEqual(result, [
            self.mock_old_dataset_list_item_1.dataset_id,
            self.mock_old_dataset_list_item_2.dataset_id
        ])

    @patch('tools.delete_stale_test_datasets.bq')
    def test_filter_stale_datasets_first_n_given(self, mock_bq):
        """Test case: All buckets are empty. first_n given. first_n < # of stale buckets.
        """
        mock_bq.get_client.list_datasets.return_value = self.dataset_list_items
        mock_bq.get_client.get_dataset.side_effect = self.datasets
        mock_bq.get_client.list_tables.return_value = []

        result = delete_stale_test_datasets._filter_stale_datasets(
            mock_bq.get_client, 1)

        self.assertEqual(result, [self.mock_old_dataset_list_item_1.dataset_id])

    @patch('tools.delete_stale_test_datasets.bq')
    def test_run_deletion(self, mock_bq):
        """Test case: Unit test for _run_deletion. This test is technically not testing anything.
        _run_deletion is tested properly in the integration test.
        """
        result = delete_stale_test_datasets._run_deletion(
            mock_bq.get_client, 'dummy_dataset')

        self.assertIsNone(result)
