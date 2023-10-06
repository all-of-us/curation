"""
Unit Test for the deactivated_participants module

Ensures that get_token function fetches the access token properly, get_deactivated_participants
    fetches all deactivated participants information, and store_participant_data properly stores all
    the fetched deactivated participant data

Original Issues: DC-797, DC-971 (sub-task), DC-972 (sub-task), DC-1213, DC-1795

The intent of this module is to check that GCR access token is generated properly, the list of
    deactivated participants returned contains `participantID`, `suspensionStatus`, and `suspensionTime`,
    and that the fetched deactivated participants data is stored properly in a BigQuery dataset.
"""

# Python imports
from unittest import TestCase
from unittest.mock import patch, MagicMock
from requests import Session

# Third Party imports
import pandas
import pandas.testing
import numpy as np

# Project imports
import utils.participant_summary_requests as psr
from common import PS_API_VALUES
from tests.test_util import FakeHTTPResponse


class ParticipantSummaryRequestsTest(TestCase):

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
        self.destination_table = 'bar_dataset._deactivated_participants'
        self.client = 'test_client'

        self.fake_token = 'fake_token'
        self.fake_url = 'www.fake_site.com'
        self.fake_headers = {
            'content-type': 'application/json',
            'Authorization': f'Bearer {self.fake_token}'
        }

        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']

        self.deactivated_participants = [[
            'P111', 'NO_CONTACT', '2018-12-07T08:21:14Z'
        ], ['P222', 'NO_CONTACT', '2018-12-07T08:21:14Z']]

        self.updated_deactivated_participants = [[
            111, 'NO_CONTACT', '2018-12-07T08:21:14Z'
        ], [222, 'NO_CONTACT', '2018-12-07T08:21:14Z']]

        self.updated_site_participant_information = [[
            333, 'foo_first', 'foo_middle', 'foo_last', 'foo_street_address',
            'foo_street_address_2', 'foo_city', 'foo_state', '12345',
            '1112223333', 'foo_email', '1900-01-01', 'SexAtBirth_Male'
        ], [444, 'bar_first', np.nan, 'bar_last']]

        self.updated_org_participant_information = [[
            333, 'foo_first', 'foo_middle', 'foo_last', 'foo_street_address',
            'foo_street_address_2', 'foo_city', 'foo_state', '12345',
            '1112223333', 'foo_email', '1900-01-01', 'SexAtBirth_Male'
        ], [444, 'bar_first', np.nan, 'bar_last']]

        self.fake_dataframe = pandas.DataFrame(
            self.updated_deactivated_participants, columns=self.columns)

        self.participant_data = [{
            'fullUrl':
                f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P111/Summary',
            'resource': {
                'participantId': 'P111',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14Z'
            }
        }, {
            'fullUrl':
                f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P222/Summary',
            'resource': {
                'participantId': 'P222',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14Z'
            }
        }]

        self.site_participant_info_data = [{
            'fullUrl':
                f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P333/Summary',
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
                f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P444/Summary',
            'resource': {
                'participantId': 'P444',
                'firstName': 'bar_first',
                'lastName': 'bar_last'
            }
        }]

        self.json_response_entry = {
            'entry': [{
                'fullUrl':
                    f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P111/Summary',
                'resource': {
                    'participantId': 'P111',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14Z'
                }
            }, {
                'fullUrl':
                    f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P222/Summary',
                'resource': {
                    'participantId': 'P222',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14Z'
                }
            }]
        }
        # Used in test_process_digital_health_data_to_df. Mimics data from the RDR PS API.
        self.api_digital_health_data = [{
            'fullUrl':
                f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P123/Summary',
            'resource': {
                'participantId': 'P123',
                'digitalHealthSharingStatus': {
                    'fitbit': {
                        'status': 'YES',
                        'history': [{
                            'status': 'YES',
                            'authoredTime': '2020-01-01T12:01:01Z'
                        }],
                        'authoredTime': '2020-01-01T12:01:01Z'
                    }
                }
            }
        }, {
            'fullUrl':
                f'https//{self.project_id}.appspot.com/rdr/v1/Participant/P234/Summary',
            'resource': {
                'participantId': 'P234',
                'digitalHealthSharingStatus': {
                    'fitbit': {
                        'status': 'YES',
                        'history': [{
                            'status': 'YES',
                            'authoredTime': '2021-01-01T12:01:01Z'
                        }],
                        'authoredTime': '2021-01-01T12:01:01Z'
                    },
                    'appleHealthKit': {
                        'status': 'YES',
                        'history': [{
                            'status': 'YES',
                            'authoredTime': '2021-02-01T12:01:01Z'
                        }, {
                            'status': 'NO',
                            'authoredTime': '2020-06-01T12:01:01Z'
                        }, {
                            'status': 'YES',
                            'authoredTime': '2020-03-01T12:01:01Z'
                        }],
                        'authoredTime': '2021-02-01T12:01:01Z'
                    }
                }
            }
        }]
        # Used in test_process_digital_health_data_to_df
        self.stored_digital_health_data = [{
            'person_id': 123,
            'wearable': 'fitbit',
            'status': 'YES',
            'history': [{
                'status': 'YES',
                'authored_time': '2020-01-01T12:01:01Z'
            }],
            'authored_time': '2020-01-01T12:01:01Z'
        }, {
            'person_id': 234,
            'wearable': 'fitbit',
            'status': 'YES',
            'history': [{
                'status': 'YES',
                'authored_time': '2021-01-01T12:01:01Z'
            }],
            'authored_time': '2021-01-01T12:01:01Z'
        }, {
            'person_id': 234,
            'wearable': 'appleHealthKit',
            'status': 'YES',
            'history': [{
                'status': 'YES',
                'authored_time': '2021-02-01T12:01:01Z'
            }, {
                'status': 'NO',
                'authored_time': '2020-06-01T12:01:01Z'
            }, {
                'status': 'YES',
                'authored_time': '2020-03-01T12:01:01Z'
            }],
            'authored_time': '2021-02-01T12:01:01Z'
        }]

    @patch('utils.participant_summary_requests.auth')
    @patch('utils.participant_summary_requests.req')
    def test_get_access_token(self, mock_req, mock_auth):
        # pre conditions
        scopes = [
            'https://www.googleapis.com/auth/cloud-platform', 'email', 'profile'
        ]
        req = MagicMock()
        client = MagicMock()
        mock_req.Request.return_value = req
        mock_email = 'test@test.com'

        # test
        client._credentials.service_account_email = mock_email
        actual_token = psr.get_access_token(client)

        # post conditions
        mock_auth.get_impersonation_credentials.assert_called_once_with(
            mock_email, target_scopes=scopes)
        mock_req.Request.assert_called_once_with()
        # assert the credential refresh still happens
        mock_auth.get_impersonation_credentials(
        ).refresh.assert_called_once_with(req)

        self.assertEqual(mock_auth.get_impersonation_credentials().token,
                         actual_token)

    @patch('utils.participant_summary_requests.BASE_URL',
           'www.fake_site.appspot.com')
    @patch('utils.participant_summary_requests.MAX_TIMEOUT', 1)
    @patch('utils.participant_summary_requests.MAX_RETRIES', 3)
    @patch('utils.participant_summary_requests.BACKOFF_FACTOR', 0.2)
    @patch('utils.participant_summary_requests.get_access_token')
    @patch.object(Session, 'get')
    def test_fake_website(self, mock_get, mock_token):
        mock_token.return_value = self.fake_token

        status_code = 500
        error_msg = 'Error: API request failed because <Response [{status_code}]>'
        mock_get.return_value = FakeHTTPResponse(status_code=status_code)
        with self.assertRaises(RuntimeError) as e:
            _ = psr.get_participant_data(self.client, self.fake_url,
                                         self.fake_headers)
        self.assertEqual(str(e.exception),
                         error_msg.format(status_code=status_code))
        self.assertEqual(mock_get.call_count, 1)

        status_code = 404
        mock_get.return_value = FakeHTTPResponse(status_code=status_code)
        with self.assertRaises(RuntimeError) as e:
            _ = psr.get_participant_data(self.client, self.fake_url,
                                         self.fake_headers)
        self.assertEqual(str(e.exception),
                         error_msg.format(status_code=status_code))
        self.assertEqual(mock_get.call_count, 2)

    @patch('utils.participant_summary_requests.get_access_token')
    @patch('utils.participant_summary_requests.Session')
    def test_get_participant_data(self, mock_get_session, mock_token):
        mock_token.return_value = self.fake_token
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = self.json_response_entry

        actual_response = psr.get_participant_data(self.client, self.fake_url,
                                                   self.fake_headers)

        self.assertEqual(actual_response, self.participant_data)

    def test_camel_case_to_snake_case(self):
        expected = 'participant_id'
        test = 'participantId'
        actual = psr.camel_to_snake_case(test)
        self.assertEqual(expected, actual)

        expected = 'sample_order_status1_p_s08_time'
        test = 'sampleOrderStatus1PS08Time'
        actual = psr.camel_to_snake_case(test)
        self.assertEqual(expected, actual)

        expected = 'street_address2'
        test = 'streetAddress2'
        actual = psr.camel_to_snake_case(test)
        self.assertEqual(expected, actual)

    @patch('utils.participant_summary_requests.store_participant_data')
    @patch('utils.participant_summary_requests.get_deactivated_participants')
    def test_get_deactivated_participants(self,
                                          mock_get_deactivated_participants,
                                          mock_store_participant_data):

        # pre conditions
        mock_get_deactivated_participants.return_value = self.fake_dataframe
        mock_bq_client = MagicMock()

        # tests
        dataframe_response = psr.get_deactivated_participants(
            self.client, self.project_id, self.columns)

        dataset_response = psr.store_participant_data(dataframe_response,
                                                      mock_bq_client,
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

    @patch('utils.participant_summary_requests.get_access_token')
    @patch('utils.participant_summary_requests.get_participant_data')
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

        expected_dataframe.fillna(value=np.nan, inplace=True)

        expected_dataframe.rename(columns=updated_fields, inplace=True)

        # Tests
        actual_dataframe = psr.get_site_participant_information(
            self.project_id, self.fake_hpo)

        # Post conditions
        pandas.testing.assert_frame_equal(expected_dataframe, actual_dataframe)

    @patch('utils.participant_summary_requests.get_access_token')
    @patch('utils.participant_summary_requests.get_participant_data')
    def test_get_org_participant_information(self, mock_get_participant_data,
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
            self.updated_org_participant_information,
            columns=psr.FIELDS_OF_INTEREST_FOR_VALIDATION)

        expected_dataframe = expected_dataframe.rename(columns=updated_fields)

        # Tests
        actual_dataframe = psr.get_org_participant_information(
            self.project_id, self.fake_hpo)

        # Post conditions
        pandas.testing.assert_frame_equal(expected_dataframe, actual_dataframe)

    def test_participant_id_to_int(self):
        # pre conditions
        columns = ['suspensionStatus', 'participantId', 'suspensionTime']
        deactivated_participants = [[
            'NO_CONTACT', 'P111', '2018-12-07T08:21:14Z'
        ]]
        updated_deactivated_participants = [[
            'NO_CONTACT', 111, '2018-12-07T08:21:14Z'
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

    @patch('utils.participant_summary_requests.LoadJobConfig')
    def test_store_participant_data(self, mock_load_job_config):
        fake_job_id = 'fake_job_id'

        mock_load_job = MagicMock()
        mock_load_job.result = MagicMock(return_value=None)
        mock_load_job.job_id = fake_job_id

        mock_bq_client = MagicMock()
        mock_bq_client.load_table_from_dataframe = MagicMock(
            return_value=mock_load_job)

        mock_load_config = MagicMock()
        mock_load_job_config.return_value = mock_load_config

        mock_table_schema = MagicMock()

        # parameter check test
        self.assertRaises(RuntimeError, psr.store_participant_data,
                          self.fake_dataframe, None, self.destination_table)
        # test
        actual_job_id = psr.store_participant_data(self.fake_dataframe,
                                                   mock_bq_client,
                                                   self.destination_table,
                                                   schema=mock_table_schema)

        mod_fake_dataframe = psr.set_dataframe_date_fields(
            self.fake_dataframe, mock_table_schema)
        pandas.testing.assert_frame_equal(
            mock_bq_client.load_table_from_dataframe.call_args[0][0],
            mod_fake_dataframe)
        mock_load_job_config.assert_called_once_with(
            schema=mock_table_schema, write_disposition='WRITE_EMPTY')
        mock_load_job.result.assert_called_once_with()
        self.assertEqual(actual_job_id, fake_job_id)

    @patch('utils.participant_summary_requests.get_access_token')
    @patch('utils.participant_summary_requests.get_participant_data')
    def test_get_deactivated_participants_parameters(self, mock_data,
                                                     mock_token):
        """
        Ensures error checking is working.
        """
        # Parameter check tests
        self.assertRaises(RuntimeError, psr.get_deactivated_participants,
                          self.client, None, self.columns)
        self.assertRaises(RuntimeError, psr.get_deactivated_participants,
                          self.client, self.project_id, None)

    def test_process_digital_health_data_to_df(self):
        column_map = {'participant_id': 'person_id'}

        actual = psr.process_digital_health_data_to_json(
            self.api_digital_health_data,
            psr.FIELDS_OF_INTEREST_FOR_DIGITAL_HEALTH, column_map)

        self.assertCountEqual(actual, self.stored_digital_health_data)
