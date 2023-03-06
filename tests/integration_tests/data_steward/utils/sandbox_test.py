# Python imports
import os
import unittest

# Third party imports
from google.cloud.exceptions import Conflict

# Project Imports
import app_identity
from utils import sandbox
from gcloud.bq import BigQueryClient


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
        self.fq_sandbox_id = f'{self.project_id}.{self.sandbox_id}'
        # Removing any existing datasets that might interfere with the test
        self.bq_client = BigQueryClient(self.project_id)
        self.bq_client.delete_dataset(self.fq_sandbox_id,
                                      delete_contents=True,
                                      not_found_ok=True)

    def test_create_sandbox_dataset(self):
        # pre-conditions
        pre_test_datasets_obj = list(
            self.bq_client.list_datasets(self.project_id))
        pre_test_datasets = [d.dataset_id for d in pre_test_datasets_obj]

        # Create sandbox dataset
        sandbox_dataset = sandbox.create_sandbox_dataset(
            self.bq_client, self.dataset_id)

        # Post condition checks
        post_test_datasets_obj = list(
            self.bq_client.list_datasets(self.project_id))
        post_test_datasets = [d.dataset_id for d in post_test_datasets_obj]

        # make sure the dataset didn't already exist
        self.assertTrue(sandbox_dataset not in pre_test_datasets)
        # make sure it was actually created
        self.assertTrue(sandbox_dataset in post_test_datasets)
        # Try to create same sandbox, which now already exists
        self.assertRaises(Conflict, sandbox.create_sandbox_dataset,
                          self.bq_client, self.dataset_id)

    def tearDown(self):
        # Remove fake dataset created in project
        self.bq_client.delete_dataset(self.fq_sandbox_id,
                                      delete_contents=True,
                                      not_found_ok=True)
