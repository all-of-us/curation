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
import mock

# Third Party imports
import pandas
import pandas.testing

# Project imports
from utils.participant_summary_requests import get_access_token, get_deactivated_participants, \
    get_participant_data


class ParticipantSummaryRequests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'foo_project'
        self.fake_scopes = ['www.fake_site.com', 'fake_email', 'fake_profile']
        self.access_token = 'ya29.12345'

        self.fake_url = 'www.fake_site.com'
        self.fake_headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer ya29.12345'
        }

        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']
        self.deactivated_participants = [[
            'P111', 'NO_CONTACT', '2018-12-07T08:21:14'
        ], ['P222', 'NO_CONTACT', '2018-12-07T08:21:14']]
        self.fake_dataframe = pandas.DataFrame(self.deactivated_participants,
                                               columns=self.columns)

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

    @mock.patch('google.auth.default')
    @mock.patch('utils.auth.delegated_credentials')
    def test_get_access_token(self, mock_credentials, mock_defaults):

        mock_credentials.return_value.token = 'ya29.12345'

        expected_response = get_access_token()

        self.assertEqual(expected_response, self.access_token)

    @mock.patch('utils.participant_summary_requests.requests.get')
    def test_get_participant_data(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        expected_response = get_participant_data(self.fake_url,
                                                 self.fake_headers)

        self.assertEqual(expected_response, self.participant_data)

    @mock.patch('utils.participant_summary_requests.get_participant_data')
    @mock.patch('utils.participant_summary_requests.get_access_token',
                return_value='ya29.12345')
    @mock.patch(
        'utils.participant_summary_requests.get_deactivated_participants')
    def test_get_deactivated_participants(self,
                                          mock_get_deactivated_participants,
                                          mock_access_token,
                                          mock_get_participant_data):

        mock_get_participant_data.return_value = self.participant_data
        mock_get_deactivated_participants.return_value = self.fake_dataframe

        response = get_deactivated_participants(self.project_id, self.columns)

        pandas.testing.assert_frame_equal(
            response,
            pandas.DataFrame(self.deactivated_participants,
                             columns=self.columns))
