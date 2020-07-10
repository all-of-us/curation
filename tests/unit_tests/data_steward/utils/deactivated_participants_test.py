"""
Unit Test for the deactivated_participants module

Ensures that get_token function fetches the access token properly and get_deactivated_participants
    fetches all deactivated participants information.

Original Issues: DC-797, DC-971 (sub-task), DC-972 (sub-task)

The intent of this module is to check that GCR access token is generated properly and the list of
    deactivated participants returned contains `participantID`, `suspensionStatus`, and `suspensionTime`.
"""

# Python imports
import unittest
import requests
import pandas
import os

# Project imports
from utils.deactivated_participants import get_access_token, get_deactivated_participants

# Third-party imports
from google.oauth2 import service_account
from google.cloud import storage

class BqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'foo_dataset'
        # self.project_id = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    def test_get_access_token(self):
        results = get_access_token(self.project_id)
        self.assertIn('ya', results)

    def test_get_deactivated_participants(self):
        results = get_deactivated_participants(self.project_id)
        print(results)
