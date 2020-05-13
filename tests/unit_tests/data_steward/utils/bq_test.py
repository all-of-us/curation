"""
Unit Test for the bq module

Ensures the define_dataset and update_labels_and_tag functions have the proper parameters passed
    to them. Ensures update_labels_and_tags function returns a dictionary of either the existing
    labels and or tags or the labels and or tags that need to be updated.

Original Issues: DC-757, DC-758

The intent is to check the proper parameters are passed to define_dataset and update_labels_and_tags
    function as well as to check to make sure the right labels and tags are returned in the
    update_labels_and_tags function.
"""

# Python imports
import unittest

# Third-party imports
from google.cloud import bigquery

# Project imports
from utils.bq import define_dataset, update_labels_and_tags


class BqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'bar_project'
        self.dataset_id = 'foo_dataset'
        self.description = 'fake_description'
        self.existing_labels_or_tags = {'label': 'value', 'tag': ''}
        self.new_labels_or_tags = {'label': 'new_value', 'new_tag': ''}
        self.updated = {'tag': '', 'label': 'new_value', 'new_tag': ''}

    def test_define_dataset(self):
        # Tests if project_id is given
        self.assertRaises(RuntimeError, define_dataset, None, self.dataset_id,
                          self.description, self.existing_labels_or_tags)

        # Tests if dataset_id is given
        self.assertRaises(RuntimeError, define_dataset, self.project_id, None,
                          self.description, self.existing_labels_or_tags)

        # Tests if description is given
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, (None or ''),
                          self.existing_labels_or_tags)

        # Tests if no label or tag is given
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, self.description, None)

        # Pre-conditions
        results = define_dataset(self.project_id, self.dataset_id,
                                 self.description, self.existing_labels_or_tags)

        # Post conditions
        self.assertIsInstance(results, bigquery.Dataset)
        self.assertEqual(results.labels, self.existing_labels_or_tags)

    def test_update_labels_and_tags(self):
        # Tests if dataset_id param is provided
        self.assertRaises(RuntimeError, update_labels_and_tags, None,
                          self.existing_labels_or_tags, self.new_labels_or_tags)

        # Tests if new_labels_or_tags param is provided
        self.assertRaises(RuntimeError, update_labels_and_tags, self.dataset_id,
                          self.existing_labels_or_tags, None)

        # Pre-conditions
        results = update_labels_and_tags(self.dataset_id,
                                         self.existing_labels_or_tags,
                                         self.new_labels_or_tags, True)

        # Post conditions
        self.assertEqual(results, self.updated)
        with self.assertRaises(RuntimeError):
            update_labels_and_tags(self.dataset_id,
                                   existing_labels_or_tags={'label': 'apples'},
                                   new_labels_or_tags={'label': 'oranges'},
                                   overwrite_ok=False)
