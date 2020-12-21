"""
Unit Test for the deactivated_participants module

Ensures that get_token function fetches the access token properly, get_deactivated_participants
    fetches all deactivated participants information, and store_participant_data properly stores all
    the fetched deactivated participant data

Original Issues: DC-797, DC-971 (sub-task), DC-972 (sub-task), DC-1213

The intent of this module is to check that GCR access token is generated properly, the list of
    deactivated participants returned contains `participantID`, `suspensionStatus`, and `suspensionTime`,
    and that the fetched deactivated participants data is stored properly in a BigQuery dataset.
"""

# Python imports
import unittest
import mock

# Third Party imports
import re
import pandas
import pandas.testing

# Project imports
import utils.participant_summary_requests as psr


class ParticipantSummaryRequestsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.tablename = 'baz_table'
        self.fake_hpo = 'foo_hpo'
        self.destination_table = 'bar_dataset.foo_table'

        self.fake_url = 'www.fake_site.com'
        self.fake_headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer ya29.12345'
        }

        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']

        self.deactivated_participants = [[
            'P111', 'NO_CONTACT', '2018-12-07T08:21:14'
        ], ['P222', 'NO_CONTACT', '2018-12-07T08:21:14']]

        self.updated_deactivated_participants = [[
            111, 'NO_CONTACT', '2018-12-07T08:21:14'
        ], [222, 'NO_CONTACT', '2018-12-07T08:21:14']]

        self.updated_site_participant_information = [[
            333, 'foo_first', 'foo_middle', 'foo_last', 'foo_street_address',
            'foo_street_address_2', 'foo_city', 'foo_state', '12345',
            '1112223333', 'foo_email', '1900-01-01', 'SexAtBirth_Male'
        ], [444, 'bar_first', 'bar_last']]

        self.fake_dataframe = pandas.DataFrame(
            self.updated_deactivated_participants, columns=self.columns)

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

        self.site_participant_info_data = [{
            'fullUrl':
                'https//foo_project.appspot.com/rdr/v1/Participant/P333/Summary',
            'resource': {
                'participantId': 'P333',
                'firstName': 'foo_first',
                'middleName': 'foo_middle',
                'lastName': 'foo_last',
                'streetAddress': 'foo_street_address',
                'streetAddress2': 'foo_street_address_2',
                'city': 'foo_city',
                'state': 'foo_state',
                'zipCode': '12345',
                'phoneNumber': '1112223333',
                'email': 'foo_email',
                'dateOfBirth': '1900-01-01',
                'sex': 'SexAtBirth_Male'
            },
        }, {
            'fullUrl':
                'https//foo_project.appspot.com/rdr/v1/Participant/P444/Summary',
            'resource': {
                'participantId': 'P444',
                'firstName': 'bar_first',
                'lastName': 'bar_last'
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

    @mock.patch('utils.participant_summary_requests.default')
    @mock.patch('utils.participant_summary_requests.auth')
    @mock.patch('utils.participant_summary_requests.req')
    def test_get_access_token(self, mock_req, mock_auth, mock_default):
        # pre conditions
        scopes = [
            'https://www.googleapis.com/auth/cloud-platform', 'email', 'profile'
        ]
        creds = mock.MagicMock()
        mock_default.return_value = (creds, None)
        req = mock.MagicMock()
        mock_req.Request.return_value = req

        # test
        actual_token = psr.get_access_token()

        # post conditions
        mock_default.assert_called_once_with()
        mock_auth.delegated_credentials.assert_called_once_with(creds,
                                                                scopes=scopes)
        mock_req.Request.assert_called_once_with()
        # assert the credential refresh still happens
        mock_auth.delegated_credentials().refresh.assert_called_once_with(req)

        self.assertEqual(mock_auth.delegated_credentials().token, actual_token)

    @mock.patch('utils.participant_summary_requests.requests.get')
    def test_get_participant_data(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        expected_response = psr.get_participant_data(self.fake_url,
                                                     self.fake_headers)

        self.assertEqual(expected_response, self.participant_data)

    @mock.patch('utils.participant_summary_requests.store_participant_data')
    @mock.patch(
        'utils.participant_summary_requests.get_deactivated_participants')
    def test_get_deactivated_participants(self,
                                          mock_get_deactivated_participants,
                                          mock_store_participant_data):

        # pre conditions
        mock_get_deactivated_participants.return_value = self.fake_dataframe

        # tests
        dataframe_response = psr.get_deactivated_participants(
            self.project_id, self.dataset_id, self.tablename, self.columns)

        dataset_response = psr.store_participant_data(dataframe_response,
                                                      self.project_id,
                                                      self.destination_table)
        expected_response = mock_store_participant_data(dataframe_response,
                                                        self.project_id,
                                                        self.destination_table)

        # post conditions
        pandas.testing.assert_frame_equal(
            dataframe_response,
            pandas.DataFrame(self.updated_deactivated_participants,
                             columns=self.columns))

        self.assertEqual(expected_response, dataset_response)

    @mock.patch('utils.participant_summary_requests.get_access_token')
    @mock.patch('utils.participant_summary_requests.get_participant_data')
    def test_get_site_participant_information(self, mock_get_participant_data,
                                              mock_token):

        # Pre conditions
        updated_fields = {
            'participantId': 'person_id',
            'firstName': 'first_name',
            'middleName': 'middle_name',
            'lastName': 'last_name',
            'streetAddress': 'street_address',
            'streetAddress2': 'street_address2',
            'city': 'city',
            'state': 'state',
            'zipCode': 'zip_code',
            'phoneNumber': 'phone_number',
            'email': 'email',
            'dateOfBirth': 'date_of_birth',
            'sex': 'sex'
        }

        mock_get_participant_data.return_value = self.site_participant_info_data

        expected_dataframe = pandas.DataFrame(
            self.updated_site_participant_information,
            columns=psr.FIELDS_OF_INTEREST_FOR_VALIDATION)

        expected_dataframe = expected_dataframe.rename(columns=updated_fields)

        # Tests
        actual_dataframe = psr.get_site_participant_information(
            self.project_id, self.fake_hpo)

        # Post conditions
        pandas.testing.assert_frame_equal(expected_dataframe, actual_dataframe)

    def test_participant_id_to_int(self):
        # pre conditions
        columns = ['suspensionStatus', 'participantId', 'suspensionTime']
        deactivated_participants = [[
            'NO_CONTACT', 'P111', '2018-12-07T08:21:14'
        ]]
        updated_deactivated_participants = [[
            'NO_CONTACT', 111, '2018-12-07T08:21:14'
        ]]

        dataframe = pandas.DataFrame(deactivated_participants, columns=columns)

        # test
        dataframe['participantId'] = dataframe['participantId'].apply(
            psr.participant_id_to_int)

        expected = psr.participant_id_to_int('P12345')

        # post conditions
        pandas.testing.assert_frame_equal(
            dataframe,
            pandas.DataFrame(updated_deactivated_participants, columns=columns))

        self.assertEqual(expected, 12345)

    @mock.patch('utils.participant_summary_requests.fields_for')
    @mock.patch('utils.participant_summary_requests.pandas_gbq.to_gbq')
    def test_store_participant_data(self, mock_to_gbq, mock_fields_for):
        # pre conditions
        table_schema = [{
            "type": "INTEGER",
            "name": "participantId"
        }, {
            "type": "STRING",
            "name": "suspensionStatus"
        }, {
            "type": "TIMESTAMP",
            "name": "suspensionTime"
        }]
        mock_fields_for.return_value = table_schema

        # parameter check test
        self.assertRaises(RuntimeError, psr.store_participant_data,
                          self.fake_dataframe, None, self.destination_table)

        # test
        results = psr.store_participant_data(self.fake_dataframe,
                                             self.project_id,
                                             self.destination_table)

        # post condition
        self.assertEqual(
            mock_to_gbq(self.fake_dataframe,
                        self.destination_table,
                        self.project_id,
                        if_exists='append',
                        table_schema=mock_fields_for), results)
