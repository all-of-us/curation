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
from utils.bq import create_dataset, delete_dataset, get_dataset, dataset_exists


class BqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.project_id = 'aou-res-curation-test'
        self.dataset_id = "foo_dataset"
        self.dataset_id_label_only = "bar_dataset"
        self.dataset_id_tag_only = "baz_dataset"
        self.description = 'fake description'
        self.label = {'fake_label': 'label'}
        self.no_label = None
        self.tag = {'fake_tag': ''}
        self.no_tag = None

    def test_dataset_exists(self):
        """Only tests whether a dataset_id and or project_id are supplied"""
        # Tests if project_id is given
        self.assertRaises(TypeError, dataset_exists, self.dataset_id)

        # Tests if dataset_id is given
        self.assertRaises(TypeError, dataset_exists, self.project_id)

    def test_create_dataset(self):
        # Tests if description is given
        self.assertRaises(TypeError, create_dataset, self.dataset_id,
                          self.label, self.tag, self.project_id)

        # Tests if no label or tag is given
        self.assertRaises(TypeError, create_dataset, self.dataset_id,
                          self.description, self.project_id)

        # Tests creation of dataset "foo_dataset" with both label and tag
        create_dataset(self.dataset_id, self.description, self.label, self.tag,
                       self.project_id)

        # Tests creation of dataset "bar_dataset" with only label supplied
        create_dataset(self.dataset_id_label_only, self.description, self.label,
                       self.no_tag, self.project_id)

        # Tests creation of dataset "baz_dataset" with only tag supplied
        create_dataset(self.dataset_id_tag_only, self.description,
                       self.no_label, self.tag, self.project_id)

        # Tests failure of create_dataset since the dataset "foo_dataset" already exists
        self.assertRaises(RuntimeError, create_dataset, self.dataset_id,
                          self.description, self.label, self.tag,
                          self.project_id)

        # Post conditions
        self.assertTrue(get_dataset(self.project_id, self.dataset_id))
        self.assertTrue(get_dataset(self.project_id,
                                    self.dataset_id_label_only))
        self.assertTrue(get_dataset(self.project_id, self.dataset_id_tag_only))

    def tearDown(self):
        # Deletes the above created datasets
        delete_dataset(self.project_id, self.dataset_id)
        delete_dataset(self.project_id, self.dataset_id_label_only)
        delete_dataset(self.project_id, self.dataset_id_tag_only)
