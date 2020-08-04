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
import unittest
import mock

# Third party imports
import pandas
import pandas.testing
import pandas_gbq
import google.auth.transport.requests as req
from google.cloud import bigquery
from google.auth import default

# Project imports
import utils.participant_summary_requests as psr
import app_identity
from utils import auth


class ParticipantSummaryRequests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = 'pipeline_tables'
        self.tablename = '_deactivated_participants'
        self.destination_table = self.dataset_id + '.' + self.tablename

        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']
        self.deactivated_participants = [[
            'P111', 'NO_CONTACT', '2018-12-07T08:21:14'
        ], ['P222', 'NO_CONTACT', '2018-12-07T08:21:14']]

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
        # pre conditions
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        # test
        expected_response = psr.get_participant_data(self.url, self.headers)

        # post conditions
        self.assertEqual(expected_response, self.participant_data)

    @mock.patch('utils.participant_summary_requests.get_participant_data')
    def test_get_deactivated_participants(self, mock_get_participant_data):
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

        # Pre conditions
        mock_get_participant_data.return_value = self.participant_data

        # Tests
        dataframe_response = psr.get_deactivated_participants(
            self.project_id, self.dataset_id, self.tablename, self.columns)
        dataset_response = psr.store_participant_data(self.fake_dataframe,
                                                      self.project_id,
                                                      self.destination_table)

        # Post conditions
        pandas.testing.assert_frame_equal(
            dataframe_response,
            pandas.DataFrame(self.deactivated_participants,
                             columns=self.columns))
        self.assertEqual(dataset_response)

    def test_store_participant_data(self):
        # Parameter check test
        self.assertRaises(RuntimeError, psr.store_participant_data,
                          self.fake_dataframe, None, self.destination_table)

        # Test
        results = psr.store_participant_data(self.fake_dataframe,
                                             self.project_id,
                                             self.destination_table)

        # Post conditions
