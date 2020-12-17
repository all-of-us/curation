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
        self.dataset_id = 'foo_dataset'
        self.fq_deact_pids = 'foo_pid_project_id.foo_pid_dataset_id.foo_pid_table'
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

    @mock.patch('retraction.retract_deactivated_pids.get_table_cols_df')
    @mock.patch(
        'retraction.retract_deactivated_pids.sb.check_and_create_sandbox_dataset'
    )
    @mock.patch(
        'utils.participant_summary_requests.get_deactivated_participants')
    @mock.patch('utils.participant_summary_requests.store_participant_data')
    def test_remove_ehr_data_past_deactivation_date(
        self, mock_store_participant_data, mock_get_deactivated_participants,
        mock_check_sandbox, mock_date_info):
        mock_store_participant_data.return_value = self.deactivated_participants_df
        mock_get_deactivated_participants.return_value = self.deactivated_participants_df

        # Preconditions for retraction module mocks
        table_cols = {
            'project_id': [self.project_id] * 31,
            'dataset_id': [self.dataset_id] * 31,
            'table_name': ['condition_occurrence'] * 5 + ['drug_exposure'] * 6 +
                          ['measurement'] * 3 + ['observation'] * 3 +
                          ['procedure_occurrence'] * 3 +
                          ['visit_occurrence'] * 5 +
                          ['_mapping_condition_occurrence'] +
                          ['_mapping_drug_exposure'] +
                          ['_mapping_measurement'] + ['_mapping_observation'] +
                          ['_mapping_procedure_occurrence'] +
                          ['_mapping_visit_occurrence'],
            'column_name': [
                'condition_start_date', 'condition_start_datetime',
                'condition_end_date', 'condition_end_datetime', 'person_id',
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_exposure_end_date', 'drug_exposure_end_datetime',
                'verbatim_end_date', 'person_id', 'measurement_date',
                'measurement_datetime', 'person_id', 'observation_date',
                'observation_datetime', 'person_id', 'procedure_date',
                'procedure_datetime', 'person_id', 'visit_start_date',
                'visit_start_datetime', 'visit_end_date', 'visit_end_datetime',
                'person_id'
            ] + ['None'] * 6
        }
        table_cols_df = pandas.DataFrame(data=table_cols)
        mock_date_info.return_value = table_cols_df

        expected_tables_to_query = [
            'condition_occurrence', 'drug_exposure', 'measurement',
            'observation', 'procedure_occurrence', 'visit_occurrence'
        ]

        self.mock_bq_client.query.return_value.to_dataframe.return_value = self.deactivated_participants_df

        sandbox = mock_check_sandbox.return_value = 'foo_pid_dataset_id_sandbox'

        # test
        returned_queries = red.remove_ehr_data_queries(self.mock_bq_client,
                                                       self.project_id,
                                                       self.dataset_id, sandbox,
                                                       self.fq_deact_pids)

        self.assertEqual(len(returned_queries),
                         len(expected_tables_to_query) * 2)
