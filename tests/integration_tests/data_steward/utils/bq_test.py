# Python imports
import unittest
import os

# Third party imports
from google.cloud import bigquery
import pandas as pd

# Project Imports
from utils import bq
import app_identity
from constants.utils import bq as consts


class BQTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        # this ensures the dataset is scoped appropriately in test and also
        # can be dropped in teardown (tests should not delete env resources)
        unioned_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.dataset_id = f'{unioned_dataset_id}_bq_test'
        self.description = f'Dataset for {__name__} integration tests'
        self.label_or_tag = {'test': 'bq'}
        self.client = bq.get_client(self.project_id)
        self.dataset_ref = bigquery.dataset.DatasetReference(
            self.project_id, self.dataset_id)

    def test_create_dataset(self):
        dataset = bq.create_dataset(self.project_id, self.dataset_id,
                                    self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

        # Try to create same dataset, which now already exists
        self.assertRaises(RuntimeError, bq.create_dataset, self.project_id,
                          self.dataset_id, self.description, self.label_or_tag)

        dataset = bq.create_dataset(self.project_id,
                                    self.dataset_id,
                                    self.description,
                                    self.label_or_tag,
                                    overwrite_existing=True)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def test_define_dataset(self):
        self.assertRaises(RuntimeError, bq.define_dataset, None,
                          self.dataset_id, self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, '', self.dataset_id,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          False, self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          self.dataset_id, ' ', self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          self.dataset_id, self.description, None)
        dataset = bq.define_dataset(self.project_id, self.dataset_id,
                                    self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def tearDown(self):
        self.client.delete_dataset(self.dataset_ref,
                                   delete_contents=True,
                                   not_found_ok=True)
