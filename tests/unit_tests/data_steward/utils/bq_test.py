"""
Unit Test for bq module

Ensures the create_dataset function has the proper parameters passed to it

Original Issue: DC-757
"""

# Python imports
import unittest
import logging

# Third-party imports
from mock import patch

# Project imports
from utils.bq import create_dataset

LOGGER = logging.getLogger(__name__)


class BqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.description = 'fake description'
        self.label = {'fake_label': 'label'}
        self.tag = {'fake_tag': ''}

    @patch('utils.bq.get_client')
    def test_create_dataset(self, mock_get_client):
        # Tests if project_id is given
        self.assertRaises(RuntimeError, create_dataset, self.dataset_id,
                          self.description, self.label, self.tag)

        # Tests if dataset_id is given
        self.assertRaises(RuntimeError, create_dataset, self.project_id,
                          self.description, self.label, self.tag)

        # Tests if description is given
        self.assertRaises(RuntimeError, create_dataset, self.project_id,
                          self.dataset_id, self.label, self.tag)

        # Tests if label is given
        self.assertRaises(RuntimeError, create_dataset, self.project_id,
                          self.dataset_id, self.description, self.tag)

        # Tests if tag is given
        self.assertRaises(RuntimeError, create_dataset, self.project_id,
                          self.dataset_id, self.description, self.label)

        # Tests if correct parameters are given
        create_dataset(self.dataset_id, self.description, self.label, self.tag,
                       self.project_id)

        # Post conditions
        mock_get_client.assert_called_once_with(self.project_id)
        self.assertEqual(mock_get_client.call_count, 1)
