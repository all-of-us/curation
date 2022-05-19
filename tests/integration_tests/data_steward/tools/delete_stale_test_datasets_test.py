"""
Unit test for delete_stale_test_buckets module
"""

# Python imports
import os
from unittest import TestCase
from unittest.mock import patch
from datetime import datetime, timezone

# Project imports
from tools import delete_stale_test_datasets
from gcloud.bq import BigQueryClient


class DeleteStaleTestDatasetsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.first_n = 3
        self.bq_client = BigQueryClient(os.environ.get('GOOGLE_CLOUD_PROJECT'))
        self.now = datetime.now(timezone.utc)

    @patch('google.cloud.bigquery.Client.delete_dataset')
    def test_main(self, mock_delete_dataset):

        datasets_to_delete = delete_stale_test_datasets.main(self.first_n)

        for dataset_name in datasets_to_delete:
            dataset_created = self.bq_client.get_dataset(dataset_name).created

            # Assert: Dataset is stale (1: 90 days or older)
            self.assertGreaterEqual((self.now - dataset_created).days, 90)

            # Assert: Dataset is stale (2: Empty(=no tables))
            self.assertEqual(
                len(list(self.bq_client.list_tables(dataset_name))), 0)
