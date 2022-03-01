"""
Test the Google Cloud Big Query Client and associated helper functions
"""
# Python stl imports
from unittest import TestCase
from unittest.mock import patch
import os

# Project imports
import app_identity
from gcloud.bq import BigQueryClient


class BqClientTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.bq_client = BigQueryClient(self.project_id)
        unioned_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.dataset_id = f'{unioned_dataset_id}_bq_test'
        self.description = f'Dataset for {__name__} integration tests'
        self.label_or_tag = {'test': 'bq'}

    def test_define_dataset(self):
        self.assertRaises(RuntimeError, self.bq_client.define_dataset, False,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, self.bq_client.define_dataset,
                          self.dataset_id, ' ', self.label_or_tag)
        self.assertRaises(RuntimeError, self.bq_client.define_dataset,
                          self.dataset_id, self.description, None)
        dataset = self.bq_client.define_dataset(self.dataset_id,
                                                self.description,
                                                self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)
