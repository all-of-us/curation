# Python imports
import unittest

# Third party imports
import mock
from mock import patch
import pandas as pd
from pandas.util.testing import assert_frame_equal

# Project imports
from retraction import retract_deactivated_pids
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts


class RetractDeactivatedEHRDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project_id'
        self.ticket_number = 'DC_fake'
        self.pids_project_id = 'fake_pids_project_id'
        self.pids_dataset_id = 'fake_pids_dataset_id'
        self.pids_table = 'fake_pids_table'
        self.pids_dataset_list = [
            'fake_dataset_1', 'fake_dataset_2', 'fake_dataset_3'
        ]
        self.pids_table_list = ['fake_table_1', 'fake_table_2', 'fake_table_3']

        mock_bq_client_patcher = patch(
            'retraction.retract_deactivated_pids.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()
        self.addCleanup(mock_bq_client_patcher.stop)

    def test_get_pids_table_info(self):
        # preconditions
        data = {
            'table_catalog': ['project'] * 7,
            'table_schema': ['dataset'] * 7,
            'table_name': ['fake_observation'] * 5 + ['location'] * 2,
            'column_name': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_datetime', 'location_id',
                'city'
            ],
        }
        column_names = [
            'table_catalog', 'table_schema', 'table_name', 'column_name'
        ]
        data_frame = pd.DataFrame(data, columns=column_names)
        self.mock_bq_client.query.return_value.to_dataframe.return_value = data_frame

        # test
        result_df = retract_deactivated_pids.get_pids_table_info(
            self.project_id, 'fake_dataset_1', self.mock_bq_client)

        # post conditions
        expected_data = dict()
        for column in column_names:
            expected_data[column] = data.get(column)[:5]

        expected_df = pd.DataFrame(expected_data, columns=column_names)

        assert_frame_equal(result_df, expected_df)

    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    def test_get_date_info_for_pids_tables(self, mock_date_info):
        # preconditions
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
        retraction_info = pd.DataFrame(data=d)
        mock_date_info.return_value = retraction_info
        expected_df = mock_date_info.return_value

        # test
        returned_df = retract_deactivated_pids.get_date_info_for_pids_tables(
            self.project_id, self.mock_bq_client)

        # post conditions
        assert_frame_equal(expected_df, returned_df)

    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    @mock.patch(
        'retraction.retract_deactivated_pids.check_and_create_sandbox_dataset')
    @mock.patch('retraction.retract_deactivated_pids.get_client')
    @mock.patch('retraction.retract_deactivated_pids.check_pid_exist')
    def test_create_queries(self, mock_pid_exist, mock_client,
                            mock_check_sandbox, mock_date_info):
        # preconditions
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
        retraction_info = pd.DataFrame(data=d)
        mock_date_info.return_value = retraction_info

        deactivated_data = {
            'person_id': [1, 2],
            'deactivated_date': ['2019-06-01', '2020-01-01']
        }
        deactivated_ehr_pids_df = pd.DataFrame(
            columns=['person_id', 'deactivated_date'], data=deactivated_data)
        client = mock_client.return_value = self.mock_bq_client
        client.query.return_value.to_dataframe.return_value = deactivated_ehr_pids_df

        mock_pid_exist.return_value = 1
        sandbox = mock_check_sandbox.return_value = 'fake_pids_dataset_id_sandbox'

        # test
        returned_queries = retract_deactivated_pids.create_queries(
            self.project_id, self.ticket_number, self.pids_project_id,
            self.pids_dataset_id, self.pids_table)

        # post conditions
        expected_queries = []

        for ehr_row in deactivated_ehr_pids_df.itertuples(index=False):
            for retraction_row in retraction_info.itertuples(index=False):
                sandbox_dataset = sandbox
                pid = ehr_row.person_id

                if pd.isnull(retraction_row.date_column):
                    sandbox_query = retract_deactivated_pids.SANDBOX_QUERY_END_DATE.render(
                        project=retraction_row.project_id,
                        sandbox_dataset=sandbox_dataset,
                        intermediary_table=self.ticket_number + '_' +
                        retraction_row.table,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.pids_table,
                        end_date_column=retraction_row.end_date_column,
                        start_date_column=retraction_row.start_date_column)
                    clean_query = retract_deactivated_pids.CLEAN_QUERY_END_DATE.render(
                        project=retraction_row.project_id,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.pids_table,
                        end_date_column=retraction_row.end_date_column,
                        start_date_column=retraction_row.start_date_column)
                else:
                    sandbox_query = retract_deactivated_pids.SANDBOX_QUERY_DATE.render(
                        project=retraction_row.project_id,
                        sandbox_dataset=sandbox_dataset,
                        intermediary_table=self.ticket_number + '_' +
                        retraction_row.table,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.pids_table,
                        date_column=retraction_row.date_column)
                    clean_query = retract_deactivated_pids.CLEAN_QUERY_DATE.render(
                        project=retraction_row.project_id,
                        dataset=retraction_row.dataset_id,
                        table=retraction_row.table,
                        pid=pid,
                        deactivated_pids_project=self.pids_project_id,
                        deactivated_pids_dataset=self.pids_dataset_id,
                        deactivated_pids_table=self.pids_table,
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
