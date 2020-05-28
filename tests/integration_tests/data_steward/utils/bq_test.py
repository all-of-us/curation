# Python imports
import unittest

# Project Imports
from utils.bq import create_dataset, define_dataset, delete_dataset
import app_identity


class BQTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = 'fake_dataset'
        self.description = 'Test dataset created for testing BQ'
        self.label_or_tag = {'test': 'bq'}
        # Remove dataset if it already exists
        delete_dataset(self.project_id, self.dataset_id)

    def test_create_dataset(self):
        dataset = create_dataset(self.project_id, self.dataset_id,
                                 self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

        # Try to create same dataset, which now already exists
        self.assertRaises(RuntimeError, create_dataset, self.project_id,
                          self.dataset_id, self.description, self.label_or_tag)

        dataset = create_dataset(self.project_id,
                                 self.dataset_id,
                                 self.description,
                                 self.label_or_tag,
                                 overwrite_existing=True)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def test_define_dataset(self):
        self.assertRaises(RuntimeError, define_dataset, None, self.dataset_id,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, '', self.dataset_id,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, self.project_id, False,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, ' ', self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, self.description, None)
        dataset = define_dataset(self.project_id, self.dataset_id,
                                 self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def tearDown(self):
        # Remove dataset created in project
        delete_dataset(self.project_id, self.dataset_id)
