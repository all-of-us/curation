"""
Integration test for the remove_ehr_data_past_deactivation_date module

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import unittest
import mock
import os
import logging

# Third party imports
import pandas

# Project imports
import app_identity
import bq_utils
import gcs_utils
import utils.participant_summary_requests as psr
import retraction.retract_deactivated_pids as rdp
import tests.integration_tests.data_steward.retraction.retract_deactivated_pids_test as rdpt
import cdr_cleaner.cleaning_rules.remove_ehr_data_past_deactivation_date as red
from sandbox import get_sandbox_dataset_id
from constants.cdr_cleaner import clean_cdr as clean_consts
from utils import bq
from tests import test_util


class RemoveEhrDataPastDeactivationDateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_dataset_id()
        self.sandbox_id = get_sandbox_dataset_id(self.dataset_id)
        self.tablename = '_deactivated_participants'
        self.ticket_number = 'DC12345'

        self.deactivated_participants = [
            (1, 'NO_CONTACT', '2018-12-07T08:21:14'),
            (2, 'NO_CONTACT', '2019-12-07T08:21:14'),
            (3, 'NO_CONTACT', '2017-12-07T08:21:14')
        ]
        self.columns = ['participantId', 'suspensionStatus', 'suspensionTime']
        self.deactivated_participants_data = {
            'person_id': [1, 2, 3],
            'suspension_status': ['NO_CONTACT', 'NO_CONTACT', 'NO_CONTACT'],
            'deactivation_date': [
                '2018-12-07T08:21:14', '2019-12-07T08:21:14',
                '2017-12-07T08:21:14'
            ]
        }
        self.deactivated_participants_df = pandas.DataFrame(
            columns=self.columns, data=self.deactivated_participants_data)

        self.json_response_entry = {
            'entry': [{
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P1/Summary',
                'resource': {
                    'participantId': 'P1',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2018-12-07T08:21:14'
                }
            }, {
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P2/Summary',
                'resource': {
                    'participantId': 'P2',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2019-12-07T08:21:14'
                }
            }, {
                'fullUrl':
                    'https//foo_project.appspot.com/rdr/v1/Participant/P3/Summary',
                'resource': {
                    'participantId': 'P3',
                    'suspensionStatus': 'NO_CONTACT',
                    'suspensionTime': '2017-12-07T08:21:14'
                }
            }]
        }

        self.client = bq.get_client(self.project_id)

    @mock.patch('utils.participant_summary_requests.requests.get')
    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    def test_remove_ehr_data_past_deactivation_date(self, mock_retraction_info,
                                                    mock_get):
        # pre conditions for participant summary API module
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = self.json_response_entry

        # Ensure deactivated participants table is created and or updated
        psr.get_deactivated_participants(self.project_id, self.dataset_id,
                                         self.tablename, self.columns)

        # pre conditions for retraction module
        d = {
            'project_id': [
                self.project_id, self.project_id, self.project_id,
                self.project_id, self.project_id, self.project_id
            ],
            'dataset_id': [
                self.dataset_id, self.dataset_id, self.dataset_id,
                self.dataset_id, self.dataset_id, self.dataset_id
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
        mock_retraction_info.return_value = retraction_info

        job_ids = []
        dropped_row_count_queries = []
        kept_row_count_queries = []
        hpo_table_list = []

        # Load the cdm files into dataset
        for cdm_file in test_util.NYC_FIVE_PERSONS_FILES:
            cdm_file_name = os.path.basename(cdm_file)
            cdm_table = cdm_file_name.split('.')[0]
            hpo_table = bq_utils.get_table_id(self.hpo_id, cdm_table)
            # Do not process if person table
            if hpo_table == 'fake_person':
                continue
            hpo_table_list.append(hpo_table)
            logging.info(
                f'Preparing to load table {self.dataset_id}.{hpo_table}')
            with open(cdm_file, 'rb') as f:
                gcs_utils.upload_object(gcs_utils.get_hpo_bucket(self.hpo_id),
                                        cdm_file_name, f)
            result = bq_utils.load_cdm_csv(self.hpo_id,
                                           cdm_table,
                                           dataset_id=self.dataset_id)
            logging.info(f'Loading table {self.dataset_id}.{hpo_table}')
            job_id = result['jobReference']['jobId']
            job_ids.append(job_id)

        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        self.assertEqual(len(incomplete_jobs), 0,
                         'NYC five person load job did not complete')
        logging.info('All tables loaded successfully')

        # Store query for checking number of rows to delete
        for ehr in self.deactivated_participants:
            pid = ehr[0]
            for row in retraction_info.itertuples(index=False):
                if row.date_column is None:
                    dropped_query = rdpt.EXPECTED_DROPPED_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                    kept_query = rdpt.EXPECTED_KEPT_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                else:
                    dropped_query = rdpt.EXPECTED_DROPPED_ROWS_QUERY.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        date_column=row.date_column)
                    kept_query = rdpt.EXPECTED_KEPT_ROWS_QUERY.format(
                        dataset_id=self.dataset_id,
                        table_id=row.table,
                        pid_table_id=self.tablename,
                        pid=pid,
                        date_column=row.date_column)
                dropped_row_count_queries.append({
                    clean_consts.QUERY: dropped_query,
                    clean_consts.DESTINATION_DATASET: self.dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })
                kept_row_count_queries.append({
                    clean_consts.QUERY: kept_query,
                    clean_consts.DESTINATION_DATASET: self.dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })

        # Use query results to count number of expected dropped row deletions
        expected_dropped_row_count = {}
        for query_dict in dropped_row_count_queries:
            response = self.client.query(query_dict['query'])
            result = response.result()
            if query_dict['destination_table_id'] in expected_dropped_row_count:
                expected_dropped_row_count[
                    query_dict['destination_table_id']] += result.total_rows
            else:
                expected_dropped_row_count[
                    query_dict['destination_table_id']] = result.total_rows

        # Separate check to find number of actual deleted rows
        q = rdpt.TABLE_ROWS_QUERY.format(dataset_id=self.dataset_id)
        q_result = self.client.query(q)
        row_count_before_retraction = {}
        for row in q_result:
            row_count_before_retraction[row['table_id']] = row['row_count']

        # Use query results to count number of expected dropped row deletions
        expected_kept_row_count = {}
        for query_dict in kept_row_count_queries:
            response = self.client.query(query_dict['query'])
            result = response.result()
            if query_dict['destination_table_id'] in expected_kept_row_count:
                expected_kept_row_count[query_dict['destination_table_id']] -= (
                    (row_count_before_retraction[
                        query_dict['destination_table_id']] -
                     result.total_rows))
            else:
                expected_kept_row_count[query_dict['destination_table_id']] = (
                    row_count_before_retraction[
                        query_dict['destination_table_id']] -
                    (row_count_before_retraction[
                        query_dict['destination_table_id']] -
                     result.total_rows))

        # Perform retraction
        query_list = red.remove_ehr_data(self.project_id, self.ticket_number,
                                         self.project_id, self.dataset_id,
                                         self.tablename)
        rdp.run_queries(query_list, self.client)

        # Find actual deleted rows
        q_result = self.client.query(q)
        results = q_result.result()
        row_count_after_retraction = {}
        for row in results:
            row_count_after_retraction[row['table_id']] = row['row_count']

        for table in expected_dropped_row_count:
            self.assertEqual(
                expected_dropped_row_count[table],
                row_count_before_retraction[table] -
                row_count_after_retraction[table])

        for table in expected_kept_row_count:
            self.assertEqual(expected_kept_row_count[table],
                             row_count_after_retraction[table])

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
        test_util.delete_all_tables(self.sandbox_id)
