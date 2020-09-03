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
import pandas.testing

# Project imports
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
import retraction.retract_deactivated_pids as rdp
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
        self.pids_project_id = 'foo_pid_project_id'
        self.pids_dataset_id = 'foo_pid_dataset_id'
        self.tablename = 'foo_table'
        self.ticket_number = 'DC12345'
        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']

        mock_bq_client_patcher = mock.patch(
            'retraction.retract_deactivated_pids.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()
        self.addCleanup(mock_bq_client_patcher.stop)

        self.deactivated_participants_data = {
            'person_id': [1, 2],
            'suspension_status': ['NO_CONTACT', 'NO_CONTACT'],
            'suspension_time': ['2018-12-07T08:21:14', '2019-12-07T08:21:14']
        }

    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    @mock.patch(
        'retraction.retract_deactivated_pids.check_and_create_sandbox_dataset')
    @mock.patch('retraction.retract_deactivated_pids.get_client')
    @mock.patch('retraction.retract_deactivated_pids.check_pid_exist')
    @mock.patch(
        'utils.participant_summary_requests.get_deactivated_participants')
    def test_remove_ehr_data_past_deactivation_date(
        self, mock_get_deactivated_participants, mock_pid_exist, mock_client,
        mock_check_sandbox, mock_date_info):
        # Preconditions for participant summary module mocks
        deactivated_participants_df = pandas.DataFrame(
            columns=self.columns, data=self.deactivated_participants_data)
        mock_get_deactivated_participants.return_value = deactivated_participants_df

        # Preconditions for retraction module mocks
        d = {
            'project_id': [
                self.project_id, self.project_id, self.project_id,
                self.project_id, self.project_id, self.project_id
            ],
            'dataset_id': [
                self.pids_dataset_id, self.pids_dataset_id,
                self.pids_dataset_id, self.pids_dataset_id,
                self.pids_dataset_id, self.pids_dataset_id
            ],
            'table': [
                'fake_condition_occurrence', 'fake_drug_exposure',
                'fake_measurement', 'fake_observation',
                'fake_procedure_occurrence', 'fake_visit_occurrence'
            ],
            'date_column': [
                None, None, 'measurement_date', 'observation_date',
                'procedure_date', None
            ],
            'start_date_column': [
                'condition_start_date', 'drug_exposure_start_date', None, None,
                None, 'visit_start_date'
            ],
            'end_date_column': [
                'condition_end_date', 'drug_exposure_end_date', None, None,
                None, 'visit_end_date'
            ]
        }
        retraction_info = pandas.DataFrame(data=d)
        mock_date_info.return_value = retraction_info

        client = mock_client.return_value = self.mock_bq_client
        client.query.return_value.to_dataframe.return_value = deactivated_participants_df

        mock_pid_exist.return_value = 1
        sandbox = mock_check_sandbox.return_value = 'foo_pid_dataset_id_sandbox'

        # test
        returned_queries = red.remove_ehr_data_queries(self.project_id,
                                                       self.ticket_number,
                                                       self.pids_project_id,
                                                       self.pids_dataset_id,
                                                       self.tablename)

        # post conditions
        expected_queries = []

        for ehr_row in deactivated_participants_df.itertuples(index=False):
            for retraction_row in retraction_info.itertuples(index=False):
                sandbox_dataset = sandbox
                pid = ehr_row.person_id

                if pandas.isnull(retraction_row.date_column):
                    sandbox_query = rdp.SANDBOX_QUERY_END_DATE.render(
                        project=retraction_row.project_id,
                        sandbox_dataset=sandbox_dataset,
                        intermediary_table=self.ticket_number + '_' +
                        retraction_row.table,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.tablename,
                        end_date_column=retraction_row.end_date_column,
                        start_date_column=retraction_row.start_date_column)
                    clean_query = rdp.CLEAN_QUERY_END_DATE.render(
                        project=retraction_row.project_id,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.tablename,
                        end_date_column=retraction_row.end_date_column,
                        start_date_column=retraction_row.start_date_column)
                else:
                    sandbox_query = rdp.SANDBOX_QUERY_DATE.render(
                        project=retraction_row.project_id,
                        sandbox_dataset=sandbox_dataset,
                        intermediary_table=self.ticket_number + '_' +
                        retraction_row.table,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.tablename,
                        date_column=retraction_row.date_column)
                    clean_query = rdp.CLEAN_QUERY_DATE.render(
                        project=retraction_row.project_id,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.tablename,
                        date_column=retraction_row.date_column)
                expected_queries.append({
                    clean_consts.QUERY:
                        sandbox_query,
                    clean_consts.DESTINATION:
                        retraction_row.project_id + '.' + sandbox_dataset +
                        '.' + (self.ticket_number + '_' + retraction_row.table),
                    clean_consts.DESTINATION_DATASET:
                        retraction_row.dataset_id,
                    clean_consts.DESTINATION_TABLE:
                        retraction_row.table,
                    clean_consts.DISPOSITION:
                        bq_consts.WRITE_APPEND,
                    'type':
                        'sandbox'
                })
                expected_queries.append({
                    clean_consts.QUERY:
                        clean_query,
                    clean_consts.DESTINATION:
                        retraction_row.project_id + '.' +
                        retraction_row.dataset_id + '.' + retraction_row.table,
                    clean_consts.DESTINATION_DATASET:
                        retraction_row.dataset_id,
                    clean_consts.DESTINATION_TABLE:
                        retraction_row.table,
                    clean_consts.DISPOSITION:
                        bq_consts.WRITE_TRUNCATE,
                    'type':
                        'retraction'
                })

        self.assertEqual(returned_queries, expected_queries)
