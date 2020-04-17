"""
Unit Test for bq module

Ensures the created dataset has the required tags and or labels

Original Issue: DC-757
"""

# Python imports
import unittest
import logging

# Third-party imports
from google.cloud import bigquery

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
        self.no_label = {}
        # is this defined properly?
        self.tag = {'fake_tag': ''}
        self.no_tag = {}
        self.exists_ok = False

    def test_create_dataset(self):
        # Tests if tags and or labels do not exist

        # Tests if tags and or labels exist
        result = create_dataset(self.dataset_id,
                                self.description,
                                self.label,
                                self.tag,
                                self.project_id,
                                self.exists_ok)

        # Post conditions
        self.assertEqual(result, LOGGER.info('Created datsaet %s.%s', self.project_id,
                                             self.dataset_id))
