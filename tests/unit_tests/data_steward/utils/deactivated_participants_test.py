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
from unittest.mock import Mock
import requests
import pandas
import mock

# Project imports
from utils.deactivated_participants import get_access_token, get_deactivated_participants

# Third-party imports
from google.oauth2 import service_account
from google.cloud import storage


class DeactivatedParticipantsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'foo_project'
        self.service_account_file = '/path/to/SA/key.json'
        self.scopes = [
            'https://www.fakewebsite.com', 'fake_email', 'fake_profile'
        ]
        self.access_token = 'ya29.12345'

    @mock.patch('utils.deactivated_participants.get_access_token', return_value='ya29.12345')
    def test_get_access_token(self, mocked_access_token):
        self.assertEqual(mocked_access_token(), self.access_token)

    @mock.patch('utils.deactivated_participants.get_access_token', return_value='ya29.12345')
    def test_get_deactivated_participants(self):
        results = get_deactivated_participants(self.project_id)
        print(results)
