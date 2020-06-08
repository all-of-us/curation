#Python imports
import os
import unittest
import logging
from io import open

# Third party imports
import pandas as pd
import mock

# Project Imports
import app_identity
import bq_utils
from utils import bq
import gcs_utils
from tests import test_util
from retraction import retract_deactivated_pids
from constants.cdr_cleaner import clean_cdr as clean_consts
from sandbox import get_sandbox_dataset_id

TABLE_ROWS_QUERY = 'SELECT * FROM {dataset_id}.__TABLES__ '

EXPECTED_DROPPED_ROWS_QUERY = ('SELECT * '
                               'FROM {dataset_id}.{table_id} '
                               'WHERE person_id = {pid} '
                               'AND {date_column} >= (SELECT deactivated_date '
                               'FROM {dataset_id}.{pid_table_id} '
                               'WHERE person_id = {pid})')
EXPECTED_KEPT_ROWS_QUERY = ('SELECT * '
                            'FROM {dataset_id}.{table_id} '
                            'WHERE person_id != {pid} '
                            'OR(  person_id = {pid} '
                            'AND {date_column} < (SELECT deactivated_date '
                            'FROM {dataset_id}.{pid_table_id} '
                            'WHERE person_id = {pid}))')
EXPECTED_DROPPED_ROWS_QUERY_END_DATE = (
    'SELECT * '
    'FROM {dataset_id}.{table_id} '
    'WHERE person_id = {pid} '
    'AND (CASE WHEN {end_date_column} IS NOT NULL THEN {end_date_column} >= '
    '(SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid} ) ELSE CASE WHEN {end_date_column} IS NULL THEN '
    '{start_date_column} >= ( '
    'SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid}) END END)')
EXPECTED_KEPT_ROWS_QUERY_END_DATE = (
    'SELECT * '
    'FROM {dataset_id}.{table_id} '
    'WHERE person_id != {pid} '
    'OR (person_id = {pid} '
    'AND (CASE WHEN {end_date_column} IS NOT NULL THEN {end_date_column} < '
    '(SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid} ) ELSE CASE WHEN {end_date_column} IS NULL THEN '
    '{start_date_column} < ( '
    'SELECT deactivated_date '
    'FROM `{dataset_id}.{pid_table_id}` '
    'WHERE person_id = {pid}) END END))')
INSERT_PID_TABLE = ('INSERT INTO {dataset_id}.{pid_table_id} '
                    '(person_id, deactivated_date) '
                    'VALUES{person_research_ids}')


class RetractDeactivatedEHRDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = app_identity.get_application_id()
        if 'test' not in self.project_id:
            raise RuntimeError(
                f"Make sure the project_id is set to test.  project_id is {self.project_id}"
            )
        self.bq_dataset_id = bq_utils.get_dataset_id()
        self.bq_sandbox_dataset_id = get_sandbox_dataset_id(self.bq_dataset_id)
        self.ticket_number = 'DCXXX'
        self.pid_table_id = 'pid_table'
        self.pid_table_id_list = [
            self.project_id + '.' + self.bq_dataset_id + '.' + 'pid_table'
        ]
        self.deactivated_ehr_participants = [(1, '2010-01-01'),
                                             (2, '2010-01-01'),
                                             (5, '2010-01-01')]
        self.client = bq.get_client(self.project_id)

    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    def test_integration_queries_to_retract_from_fake_dataset(
        self, mock_retraction_info):
        d = {
            'project_id': [
                self.project_id, self.project_id, self.project_id,
                self.project_id, self.project_id, self.project_id
            ],
            'dataset_id': [
                self.bq_dataset_id, self.bq_dataset_id, self.bq_dataset_id,
                self.bq_dataset_id, self.bq_dataset_id, self.bq_dataset_id
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
        mock_retraction_info.return_value = retraction_info

        # Create and load person_ids and deactivated_date to pid table
        bq.create_tables(self.client,
                         self.project_id,
                         self.pid_table_id_list,
                         exists_ok=False,
                         fields=retract_deactivated_pids.PID_TABLE_FIELDS)
        bq_formatted_insert_values = ', '.join([
            '(%s, "%s")' % (person_id, deactivated_date)
            for (person_id,
                 deactivated_date) in self.deactivated_ehr_participants
        ])
        q = INSERT_PID_TABLE.format(
            dataset_id=self.bq_dataset_id,
            pid_table_id=self.pid_table_id,
            person_research_ids=bq_formatted_insert_values)
        self.client.query(q)

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
                f'Preparing to load table {self.bq_dataset_id}.{hpo_table}')
            with open(cdm_file, 'rb') as f:
                gcs_utils.upload_object(gcs_utils.get_hpo_bucket(self.hpo_id),
                                        cdm_file_name, f)
            result = bq_utils.load_cdm_csv(self.hpo_id,
                                           cdm_table,
                                           dataset_id=self.bq_dataset_id)
            logging.info(f'Loading table {self.bq_dataset_id}.{hpo_table}')
            job_id = result['jobReference']['jobId']
            job_ids.append(job_id)

        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        self.assertEqual(len(incomplete_jobs), 0,
                         'NYC five person load job did not complete')
        logging.info('All tables loaded successfully')

        # Store query for checking number of rows to delete
        for ehr in self.deactivated_ehr_participants:
            pid = ehr[0]
            for row in retraction_info.itertuples(index=False):
                if row.date_column is None:
                    dropped_query = EXPECTED_DROPPED_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                    kept_query = EXPECTED_KEPT_ROWS_QUERY_END_DATE.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        start_date_column=row.start_date_column,
                        end_date_column=row.end_date_column)
                else:
                    dropped_query = EXPECTED_DROPPED_ROWS_QUERY.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        date_column=row.date_column)
                    kept_query = EXPECTED_KEPT_ROWS_QUERY.format(
                        dataset_id=self.bq_dataset_id,
                        table_id=row.table,
                        pid_table_id=self.pid_table_id,
                        pid=pid,
                        date_column=row.date_column)
                dropped_row_count_queries.append({
                    clean_consts.QUERY: dropped_query,
                    clean_consts.DESTINATION_DATASET: self.bq_dataset_id,
                    clean_consts.DESTINATION_TABLE: row.table
                })
                kept_row_count_queries.append({
                    clean_consts.QUERY: kept_query,
                    clean_consts.DESTINATION_DATASET: self.bq_dataset_id,
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
        q = TABLE_ROWS_QUERY.format(dataset_id=self.bq_dataset_id)
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
        query_list = retract_deactivated_pids.create_queries(
            self.project_id, self.ticket_number, self.project_id,
            self.bq_dataset_id, self.pid_table_id)
        retract_deactivated_pids.run_queries(query_list, self.client)

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
        test_util.delete_all_tables(self.bq_dataset_id)
        test_util.delete_all_tables(self.bq_sandbox_dataset_id)
        tables = list(self.client.list_tables(self.bq_sandbox_dataset_id))
        if not tables:
            self.client.delete_dataset(self.bq_sandbox_dataset_id)
