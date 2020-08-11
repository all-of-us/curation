"""
Integration Test for the deactivated_participants module

Ensures that get_token function fetches the access token properly, get_deactivated_participants
    fetches all deactivated participants information, and store_participant_data properly stores all
    the fetched deactivated participant data

Original Issues: DC-797, DC-971 (sub-task), DC-972 (sub-task)

The intent of this module is to check that GCR access token is generated properly, the list of
    deactivated participants returned contains `participantID`, `suspensionStatus`, and `suspensionTime`,
    and that the fetched deactivated participants data is stored properly in a BigQuery dataset.
"""

# Python imports
import mock
import os

# Third party imports
import pandas
import pandas.testing
import google.auth.transport.requests as req
from google.auth import default
from dateutil import parser

# Project imports
import utils.participant_summary_requests as psr
from app_identity import PROJECT_ID
from utils import auth
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class ParticipantSummaryRequests(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')

        cls.tablename = '_deactivated_participants'
        cls.destination_table = cls.dataset_id + '.' + cls.tablename

        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{cls.tablename}"
        ]
        super().setUpClass()

    def setUp(self):
        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']
        self.deactivated_participants = [[
            111, 'NO_CONTACT',
            pandas.Timestamp('2018-12-07T08:21:14')
        ], [222, 'NO_CONTACT',
            pandas.Timestamp('2018-12-07T08:21:14')]]

        self.fake_dataframe = pandas.DataFrame(self.deactivated_participants,
                                               columns=self.columns)

        self.url = 'www.fake_site.com'
        self.headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer ya29.12345'
        }

        self.participant_data = [{
            'fullUrl':
                'https//foo_project.appspot.com/rdr/v1/Participant/P111/Summary',
            'resource': {
                'participantId': 'P111',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14'
            }
        }, {
            'fullUrl':
                'https//foo_project.appspot.com/rdr/v1/Participant/P222/Summary',
            'resource': {
                'participantId': 'P222',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14'
            }
        }]

        self.json_response_entry = {
            'entry': [{
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P111/Summary',
                'resource': {
                    'participantId': 'P111',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14'
                }
            }, {
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P222/Summary',
                'resource': {
                    'participantId': 'P222',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14'
                }
            }]
        }

        super().setUp()

    def test_get_access_token(self):
        # pre conditions
        scopes = [
            'https://www.googleapis.com/auth/cloud-platform', 'email', 'profile'
        ]
        credentials, _ = default()
        credentials = auth.delegated_credentials(credentials, scopes=scopes)
        request = req.Request()
        credentials.refresh(request)
        expected_access_token = credentials.token

        # test
        actual_access_token = psr.get_access_token()

        # post conditions
        self.assertEqual(actual_access_token, expected_access_token)

    @mock.patch('utils.participant_summary_requests.requests.get')
    def test_get_participant_data(self, mock_get):
        """
        Mocks calling the participant summary api.
        """
        # pre conditions
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        # test
        expected_response = psr.get_participant_data(self.url, self.headers)

        # post conditions
        self.assertEqual(expected_response, self.participant_data)

    def test_get_deactivated_participants_parameters(self):
        """
        Ensures error checking is working.
        """
        # Parameter check tests
        self.assertRaises(RuntimeError, psr.get_deactivated_participants, None,
                          self.dataset_id, self.tablename, self.columns)
        self.assertRaises(RuntimeError, psr.get_deactivated_participants,
                          self.project_id, None, self.tablename, self.columns)
        self.assertRaises(RuntimeError, psr.get_deactivated_participants,
                          self.project_id, self.dataset_id, None, self.columns)
        self.assertRaises(RuntimeError, psr.get_deactivated_participants,
                          self.project_id, self.dataset_id, self.tablename,
                          None)

    @mock.patch('utils.participant_summary_requests.requests.get')
    def test_get_deactivated_participants(self, mock_get):
        # Pre conditions
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        # Tests
        psr.get_deactivated_participants(self.project_id, self.dataset_id,
                                         self.tablename, self.columns)

        # Post conditions
        values = [(111, 'NO_CONTACT', parser.parse('2018-12-07T08:21:14 UTC')),
                  (222, 'NO_CONTACT', parser.parse('2018-12-07T08:21:14 UTC'))]
        self.assertTableValuesMatch(
            '.'.join([self.project_id, self.destination_table]), self.columns,
            values)

    def test_store_participant_data(self):
        # Parameter check test
        self.assertRaises(RuntimeError, psr.store_participant_data,
                          self.fake_dataframe, None, self.destination_table)

        # Test
        psr.store_participant_data(self.fake_dataframe, self.project_id,
                                   self.destination_table)

        # Post conditions
        values = [(111, 'NO_CONTACT', parser.parse('2018-12-07T08:21:14 UTC')),
                  (222, 'NO_CONTACT', parser.parse('2018-12-07T08:21:14 UTC'))]
        self.assertTableValuesMatch(
            '.'.join([self.project_id, self.destination_table]), self.columns,
            values)
