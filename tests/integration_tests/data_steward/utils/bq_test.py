"""
Integration Test for the bq module

Ensures the define_dataset function has the proper parameters passed to it
Ensures the update_labels_and_tags function has the proper parameters passed to it and
    sets, replaces, or updates labels and tags properly

Original Issues: DC-757, DC-758

The intent is to check that the proper parameters (project_id, dataset_id,
 label_or_tag, and or description) are passed to the define_dataset and
 update_labels_and_tags functions. Also to make sure the labels and tags can be either set,
 replaced, or updated with different labels or tags in the update_labels_and_tags function.
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
        # input parameters expected by the class
        self.project_id = 'aou-res-curation-test'
        self.dataset_id = "foo_dataset"
        self.description = 'fake description'
        self.label_or_tag = {'fake_label': 'label', 'fake_tag': ''}
        self.overwritten_label_or_tag = {
            'new_fake_label': 'label',
            "new_fake_tag": ''
        }
        self.updated_label_or_tag = {
            'fake_label': 'label',
            'fake_tag': '',
            'new_fake_label': 'label',
            "new_fake_tag": ''
        }
        self.set_or_update_true = True
        self.set_or_update_false = False

    def test_define_dataset(self):
        # Tests if project_id is given
        self.assertRaises(TypeError, define_dataset, self.dataset_id,
                          self.description, self.label_or_tag)

        # Tests if dataset_id is given
        self.assertRaises(TypeError, define_dataset, self.project_id,
                          self.description, self.label_or_tag)

        # Tests if description is given
        self.assertRaises(TypeError, define_dataset, self.project_id,
                          self.dataset_id, self.label_or_tag)

        # Tests if no label or tag is given
        self.assertRaises(TypeError, define_dataset, self.project_id,
                          self.dataset_id, self.description)

        # Pre-conditions
        results = define_dataset(self.project_id, self.dataset_id,
                                 self.description, self.label_or_tag)

        # Post conditions
        self.assertIsInstance(results, bigquery.Dataset)
        self.assertEqual(results.labels, self.label_or_tag)

    def test_update_labels_and_tags(self):
        # Tests if project_id is given
        self.assertRaises(TypeError, update_labels_and_tags, self.dataset_id,
                          self.label_or_tag)

        # Tests if dataset_id is given
        self.assertRaises(TypeError, update_labels_and_tags, self.project_id,
                          self.label_or_tag)

        # Tests if no label or tag is given
        self.assertRaises(TypeError, update_labels_and_tags, self.project_id,
                          self.dataset_id)

        # Pre-conditions
        results = define_dataset(self.project_id, self.dataset_id,
                                 self.description, self.label_or_tag)

        self.client = bigquery.Client()
        self.dataset = self.client.create_dataset(results)

        update_false_expected = update_labels_and_tags(
            self.project_id, self.dataset_id, self.overwritten_label_or_tag,
            self.set_or_update_false)

        update_true_expected = update_labels_and_tags(self.project_id,
                                                      self.dataset_id,
                                                      self.updated_label_or_tag,
                                                      self.set_or_update_true)

        # Post conditions
        self.assertIsInstance(results, bigquery.Dataset)
        self.assertEqual(update_true_expected.labels, self.updated_label_or_tag)
        self.assertEqual(update_false_expected.labels,
                         self.overwritten_label_or_tag)

    def tearDown(self):
        # Deletes bar_dataset
        self.client = bigquery.Client()
        self.client.delete_dataset(f'{self.project_id}.{self.dataset_id}',
                                   delete_contents=True,
                                   not_found_ok=True)
