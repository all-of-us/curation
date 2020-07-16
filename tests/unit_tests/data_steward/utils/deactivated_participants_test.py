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
import pandas
import mock
import pandas.testing

# Project imports
from utils.deactivated_participants import get_deactivated_participants


class DeactivatedParticipantsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'foo_project'
        self.access_token = 'ya29.12345'
        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']
        self.deactivated_participants = [[
            'P111', 'NO_CONTACT', '2018-12-07T08:21:14'
        ], ['P222', 'NO_CONTACT', '2018-12-07T08:21:14']]
        self.fake_dataframe = pandas.DataFrame(self.deactivated_participants,
                                               columns=self.columns)

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

    @mock.patch('utils.deactivated_participants.get_access_token',
                return_value='ya29.12345')
    def test_get_access_token(self, mock_access_token):
        self.assertEqual(mock_access_token(), self.access_token)

    @mock.patch('utils.deactivated_participants.get_deactivated_participants')
    @mock.patch('utils.deactivated_participants.requests.get')
    def test_get_deactivated_participants(self, mock_get,
                                          mock_get_deactivated_participants):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        mock_get_deactivated_participants.return_value = self.fake_dataframe
        response = get_deactivated_participants(self.project_id)

        pandas.testing.assert_frame_equal(
            response,
            pandas.DataFrame(self.deactivated_participants,
                             columns=self.columns))
