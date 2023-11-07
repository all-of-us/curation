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
import os
import time
from datetime import datetime, timezone
from unittest import mock

# Third party imports
from pandas import DataFrame
from google.cloud.bigquery import Table, TimePartitioning, TimePartitioningType

# Project imports
import utils.participant_summary_requests as psr
from app_identity import PROJECT_ID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DIGITAL_HEALTH_SHARING_STATUS


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
        self.bq_columns = [
            'person_id', 'suspension_status', 'deactivated_datetime'
        ]
        self.deactivated_participants = [[
            111, 'NO_CONTACT', '2018-12-7T08:21:14Z'
        ], [222, 'NO_CONTACT', '2018-12-7T08:21:14Z']]

        self.fake_dataframe = DataFrame(self.deactivated_participants,
                                        columns=self.bq_columns)

        self.url = 'www.fake_site.com'
        self.token = 'fake_token'
        self.headers = {
            'content-type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }

        self.participant_data = [{
            'fullUrl':
                'https//foo_project.appspot.com/rdr/v1/Participant/P111/Summary',
            'resource': {
                'participantId': 'P111',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14Z'
            }
        }, {
            'fullUrl':
                'https//foo_project.appspot.com/rdr/v1/Participant/P222/Summary',
            'resource': {
                'participantId': 'P222',
                'suspensionStatus': 'NO_CONTACT',
                'suspensionTime': '2018-12-07T08:21:14Z'
            }
        }]

        self.json_response_entry = {
            'entry': [{
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P111/Summary',
                'resource': {
                    'participantId': 'P111',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14Z'
                }
            }, {
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P222/Summary',
                'resource': {
                    'participantId': 'P222',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14Z'
                }
            }]
        }

        self.digital_health_data = [{
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

        super().setUp()

    @mock.patch('utils.participant_summary_requests.get_access_token')
    @mock.patch('utils.participant_summary_requests.Session')
    def test_get_participant_data(self, mock_get_session, mock_token):
        """
        Mocks calling the participant summary api.
        """
        # pre conditions
        mock_token.return_value = 'fake_token'
        mock_session = mock.MagicMock()
        mock_get_session.return_value = mock_session
        mock_resp = mock.MagicMock()
        mock_session.get.return_value = mock_resp
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.json_response_entry

        # test
        expected_response = psr.get_participant_data(self.client, self.url,
                                                     self.headers)

        # post conditions
        self.assertEqual(expected_response, self.participant_data)

    @mock.patch('utils.participant_summary_requests.get_access_token')
    @mock.patch('utils.participant_summary_requests.Session')
    def test_get_deactivated_participants(self, mock_get_session, mock_token):
        # Pre conditions
        mock_token.return_value = 'fake_token'
        mock_session = mock.MagicMock()
        mock_get_session.return_value = mock_session
        mock_resp = mock.MagicMock()
        mock_session.get.return_value = mock_resp
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.json_response_entry

        # Tests
        df = psr.get_deactivated_participants(self.client, self.project_id,
                                              self.columns)

        # Parameter check test
        self.assertRaises(RuntimeError, psr.store_participant_data,
                          self.fake_dataframe, None, self.destination_table)

        psr.store_participant_data(df, self.client,
                                   f'{self.dataset_id}.{self.tablename}')

        # Post conditions
        values = [(111, 'NO_CONTACT',
                   datetime(2018, 12, 7, 8, 21, 14, tzinfo=timezone.utc)),
                  (222, 'NO_CONTACT',
                   datetime(2018, 12, 7, 8, 21, 14, tzinfo=timezone.utc))]
        self.assertTableValuesMatch(
            '.'.join([self.project_id, self.destination_table]),
            self.bq_columns, values)

    def test_store_digital_health_status_data(self):
        # Pre conditions
        fq_table = f'{self.project_id}.{self.dataset_id}.{DIGITAL_HEALTH_SHARING_STATUS}'
        self.fq_table_names.append(fq_table)
        table = Table(
            fq_table,
            schema=self.client.get_table_schema(DIGITAL_HEALTH_SHARING_STATUS))
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.DAY)
        _ = self.client.create_table(table, exists_ok=True)

        # Tests
        _ = psr.store_digital_health_status_data(self.client,
                                                 self.digital_health_data,
                                                 fq_table)

        expected_values = [
            (123, 'fitbit', 'YES', [{
                'status':
                    'YES',
                'authored_time':
                    datetime(2020, 1, 1, 12, 1, 1, tzinfo=timezone.utc)
            }], datetime(2020, 1, 1, 12, 1, 1, tzinfo=timezone.utc)),
            (234, 'fitbit', 'YES', [{
                'status':
                    'YES',
                'authored_time':
                    datetime(2021, 1, 1, 12, 1, 1, tzinfo=timezone.utc)
            }], datetime(2021, 1, 1, 12, 1, 1, tzinfo=timezone.utc)),
            (234, 'appleHealthKit', 'YES', [{
                'status':
                    'YES',
                'authored_time':
                    datetime(2021, 2, 1, 12, 1, 1, tzinfo=timezone.utc)
            }, {
                'status':
                    'NO',
                'authored_time':
                    datetime(2020, 6, 1, 12, 1, 1, tzinfo=timezone.utc)
            }, {
                'status':
                    'YES',
                'authored_time':
                    datetime(2020, 3, 1, 12, 1, 1, tzinfo=timezone.utc)
            }], datetime(2021, 2, 1, 12, 1, 1, tzinfo=timezone.utc)),
        ]
        self.assertTableValuesMatch(fq_table,
                                    self.digital_health_data[0].keys(),
                                    expected_values)

    def tearDown(self):
        """
        Add a one second delay to teardown to make it less likely to fail due to rate limits.
        """
        time.sleep(1)
        super().tearDown()
