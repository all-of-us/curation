# Python imports
import os
import unittest

import app_identity
# Project Imports
from utils import sandbox
from utils.bq import get_client, list_datasets, delete_dataset


class SandboxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.sandbox_id = sandbox.get_sandbox_dataset_id(self.dataset_id)
        # Removing any existing datasets that might interfere with the test
        self.client = get_client(self.project_id)
        self.client.delete_dataset(f'{self.project_id}.{self.sandbox_id}',
                                   delete_contents=True,
                                   not_found_ok=True)

    def test_create_sandbox_dataset(self):
        # Create sandbox dataset
        sandbox_dataset = sandbox.create_sandbox_dataset(
            self.project_id, self.dataset_id)
        all_datasets_obj = list_datasets(self.project_id)
        all_datasets = [d.dataset_id for d in all_datasets_obj]

        self.assertTrue(sandbox_dataset in all_datasets)

        # Try to create same sandbox, which now already exists
        self.assertRaises(RuntimeError, sandbox.create_sandbox_dataset,
                          self.project_id, self.dataset_id)

        # Remove fake dataset created in project
        delete_dataset(self.project_id, sandbox_dataset)
