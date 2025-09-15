"""
Unit Test for the deactivated_participants module

Ensures that get_token function fetches the access token properly, get_deactivated_participants
    fetches all deactivated participants information, and store_participant_data properly stores all
    the fetched deactivated participant data

Original Issues: DC-797, DC-971 (sub-task), DC-972 (sub-task), DC-1213, DC-1795

Issue: DL-1780 updated 'participant_summary_requests' to not call the API. Instead, to just store data in target table.

The intent of this module is to check that GCR access token is generated properly, the list of
    deactivated participants returned contains `participantID`, `suspensionStatus`, and `suspensionTime`,
    and that the fetched deactivated participants data is stored properly in a BigQuery dataset.
"""

# Python imports
from unittest import TestCase
from unittest.mock import patch, MagicMock

# Third Party imports
import pandas
import pandas.testing
import numpy as np

# Project imports
import utils.participant_summary_requests as psr


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
                'participantId': 'P111',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14Z'
        }, {
                'participantId': 'P222',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14Z'
            }
        ]

        self.site_participant_info_data = [{
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
            {
                'participantId': 'P444',
                'firstName': 'bar_first',
                'lastName': 'bar_last'
            }
        ]

        self.json_response_entry = {
            'entry': [{
                    'participantId': 'P111',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14Z'
                },
                {
                    'participantId': 'P222',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14Z'
                }
            ]
        }
        # Used in test_process_digital_health_data_to_df. Mimics data from the RDR PS API.
        self.api_digital_health_data = [{
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
        }, {
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
        ]
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
