#Python imports
import unittest

# Project Imports
from data_steward import sandbox
from utils.bq import list_datasets, delete_dataset


class RetractDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'aou-res-curation-test'
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

        # Remove fake dataset created in aou-res-curation-test
        delete_dataset(self.project_id, dataset)
