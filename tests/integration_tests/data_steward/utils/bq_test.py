"""
Integration Test for the bq module

Ensures the create_dataset function has the proper parameters passed to it

Original Issue: DC-757

The intent is to check whether the dataset exists or not. If the dataset does
exist, a runtime error is raised stating as such. If the dataset does not
exist, it needs to be created with the proper parameters: project_id, dataset_id,
labels, and description.
"""

# Python imports
import unittest

# Project imports
from utils.bq import create_dataset, delete_dataset, get_dataset


class BqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.project_id = 'aou-res-curation-test'
        self.existing_dataset_id = 'bar_dataset'
        self.new_dataset_id = 'baz_dataset'
        self.description = 'fake description'
        self.label = {'fake_label': 'label'}

    def test_create_dataset(self):
        # Tests failure of create_dataset due to dataset already existing
        self.assertRaises(RuntimeError, create_dataset, self.existing_dataset_id,
                          self.description, self.label, self.project_id)

        # Tests if project_id is given
        self.assertRaises(TypeError, create_dataset, self.existing_dataset_id,
                          self.description, self.label)

        # Tests if dataset_id is given
        self.assertRaises(TypeError, create_dataset, self.description,
                          self.label, self.project_id)

        # Tests if description is given
        self.assertRaises(TypeError, create_dataset, self.project_id,
                          self.existing_dataset_id, self.label)

        # Tests if label is given
        self.assertRaises(TypeError, create_dataset, self.project_id,
                          self.existing_dataset_id, self.description)

        # Tests creation of dataset "baz_dataset" is successful
        create_dataset(self.new_dataset_id, self.description,
                       self.label, self.project_id)

        # Post conditions
        self.assertTrue(get_dataset(self.project_id, self.new_dataset_id))

    def tearDown(self):
        # Deletes dataset "baz_dataset"
        delete_dataset(self.project_id, self.new_dataset_id)
