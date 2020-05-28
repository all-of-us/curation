# Python imports
import unittest

# Project Imports
import sandbox
from utils.bq import list_datasets, delete_dataset
import app_identity


class SandboxTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = 'fake_dataset'

    def test_create_sandbox_dataset(self):
        # Create sandbox dataset
        dataset = sandbox.create_sandbox_dataset(self.project_id,
                                                 self.dataset_id)
        all_datasets_obj = list_datasets(self.project_id)
        all_datasets = [d.dataset_id for d in all_datasets_obj]

        self.assertTrue(dataset in all_datasets)

        # Try to create same sandbox, which now already exists
        self.assertRaises(RuntimeError, sandbox.create_sandbox_dataset,
                          self.project_id, self.dataset_id)

        # Remove fake dataset created in project
        delete_dataset(self.project_id, dataset)
