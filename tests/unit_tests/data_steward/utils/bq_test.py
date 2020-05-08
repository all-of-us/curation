"""
Unit Test for the bq module

Ensures the update_labels_and_tags function has the proper parameters passed to it and
    no new labels are added that already exist.

Original Issue: DC-758

The intent is to check that dataset_id and new_labels_or_tag are passed to the
 update_labels_and_tags. Also to make sure an error is raised if overwrite_ok is
 false and any new labels or tags are provided that already exists
 in existing_labels_or_tags dictionary
"""

# Python imports
import unittest

# Project imports
from utils.bq import update_labels_and_tags


class BqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.dataset_id = 'foo_dataset'
        self.existing_labels_or_tags = {
            'existing_label': 'existing_value',
            'existing_tag': ''
        }
        self.new_labels_or_tags = {'new_label': 'new_value', 'new_tag': ''}

    def test_update_labels_and_tags(self):
        # Tests if dataset_id param is provided
        self.assertRaises(TypeError, update_labels_and_tags,
                          self.existing_labels_or_tags, self.new_labels_or_tags)

        # Tests if new_labels_or_tags param is provided
        self.assertRaises(TypeError, update_labels_and_tags, self.dataset_id,
                          self.existing_labels_or_tags)

        # Tests if the same labels and tags are provided
        self.assertRaises(TypeError, update_labels_and_tags,
                          self.existing_labels_or_tags,
                          self.existing_labels_or_tags)

        # Pre-conditions
        results = update_labels_and_tags(self.dataset_id,
                                         self.existing_labels_or_tags,
                                         self.new_labels_or_tags, True)

        # Post conditions
        self.assertEqual(results, self.new_labels_or_tags)
