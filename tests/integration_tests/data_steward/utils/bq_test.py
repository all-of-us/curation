"""
Integration Test for the bq module

Ensures the define_dataset function has the proper parameters passed to it.

Original Issue: DC-757

The intent is to check that the proper parameters (project_id, dataset_id,
 label_or_tag, and or description) are passed to the define_dataset function.
"""

# Python imports
import unittest

# Third-party imports
from google.cloud import bigquery

# Project imports
from utils.bq import define_dataset


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
