"""
Unit test for the remove_ehr_data_past_deactivation_date module

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import unittest
import mock

# Third Party imports
import google.cloud.bigquery as gbq
import pandas

# Project imports
import cdr_cleaner.cleaning_rules.remove_ehr_data_past_deactivation_date as red


class RemoveEhrDataPastDeactivationDateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # Input parameters expected by the class
        self.project_id = 'foo_project'
        self.api_project_id = 'foo_api_project'
        self.dataset_id = 'foo_dataset'
        self.fq_deact_pids = 'foo_project.foo_sandbox_dataset._deactivated_participants'
        self.sandbox_dataset_id = 'foo_sandbox_dataset'
        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']

        mock_bq_client_patcher = mock.patch('utils.bq.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()
        self.addCleanup(mock_bq_client_patcher.stop)

        self.deactivated_participants_data = {
            'person_id': [1, 2],
            'suspension_status': ['NO_CONTACT', 'NO_CONTACT'],
            'suspension_time': ['2018-12-07T08:21:14', '2019-12-07T08:21:14']
        }
        self.deactivated_participants_df = pandas.DataFrame(
            columns=self.columns, data=self.deactivated_participants_data)

    @mock.patch('retraction.retract_deactivated_pids.generate_queries')
    @mock.patch('utils.participant_summary_requests.store_participant_data')
    @mock.patch(
        'utils.participant_summary_requests.get_deactivated_participants')
    def test_queries(self, mock_get_data, mock_store_data, mock_query_creation):
        # test setup
        mock_get_data.return_value = pandas.DataFrame()
        mock_query_creation.return_value = []

        # test
        red.remove_ehr_data_queries(self.mock_bq_client, self.api_project_id,
                                    self.project_id, self.dataset_id,
                                    self.sandbox_dataset_id)

        # post conditions
        mock_query_creation.assert_called_once_with(
            self.mock_bq_client, self.project_id, self.dataset_id,
            self.sandbox_dataset_id,
            gbq.TableReference.from_string(self.fq_deact_pids))

        mock_store_data.assert_called_once_with(
            mock_get_data.return_value, self.project_id,
            '.'.join(self.fq_deact_pids.split('.')[1:]))

        mock_get_data.assert_called_once_with(
            self.api_project_id, red.DEACTIVATED_PARTICIPANTS_COLUMNS)
