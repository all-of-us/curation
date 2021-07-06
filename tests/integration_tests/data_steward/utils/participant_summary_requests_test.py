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
from datetime import date
from unittest import mock

# Third party imports
from pandas import DataFrame

# Project imports
import utils.participant_summary_requests as psr
from app_identity import PROJECT_ID
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
        self.bq_columns = ['person_id', 'suspension_status', 'deactivated_date']
        self.deactivated_participants = [[111, 'NO_CONTACT',
                                          date(2018, 12, 7)],
                                         [222, 'NO_CONTACT',
                                          date(2018, 12, 7)]]

        self.fake_dataframe = DataFrame(self.deactivated_participants,
                                        columns=self.bq_columns)

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

    @mock.patch('utils.participant_summary_requests.requests.get')
    def test_get_deactivated_participants(self, mock_get):
        # Pre conditions
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        # Tests
        df = psr.get_deactivated_participants(self.project_id, self.columns)

        psr.store_participant_data(df, self.project_id,
                                   f'{self.dataset_id}.{self.tablename}')

        # Post conditions
        values = [(111, 'NO_CONTACT', date(2018, 12, 7)),
                  (222, 'NO_CONTACT', date(2018, 12, 7))]
        self.assertTableValuesMatch(
            '.'.join([self.project_id, self.destination_table]),
            self.bq_columns, values)

    def test_store_participant_data(self):
        # Parameter check test
        self.assertRaises(RuntimeError, psr.store_participant_data,
                          self.fake_dataframe, None, self.destination_table)

        # Test
        psr.store_participant_data(self.fake_dataframe, self.project_id,
                                   self.destination_table)

        # Post conditions
        values = [(111, 'NO_CONTACT', date(2018, 12, 7)),
                  (222, 'NO_CONTACT', date(2018, 12, 7))]
        self.assertTableValuesMatch(
            '.'.join([self.project_id, self.destination_table]),
            self.bq_columns, values)

    def tearDown(self):
        """
        Add a one second delay to teardown to make it less likely to fail due to rate limits.
        """
        time.sleep(1)
        super().tearDown()
