#Python imports
import os
import unittest
import logging

# Third party imports
import app_identity
import pandas as pd
import mock

# Project Imports
import bq_utils
import gcs_utils
from tests import test_util
from retraction import retract_deactivated_pids
from io import open
from constants.cdr_cleaner import clean_cdr as clean_consts

TABLE_ROWS_QUERY = 'SELECT * FROM {dataset_id}.__TABLES__ '

EXPECTED_ROWS_QUERY = ('SELECT * '
                       'FROM {dataset_id}.{table_id} '
                       'WHERE person_id = {pid} '
                       'AND {date_column} >= (SELECT deactivated_date '
                       'FROM {dataset_id}.{pid_table_id} '
                       'WHERE person_id = {pid})')

INSERT_PID_TABLE = ('INSERT INTO {dataset_id}.{pid_table_id} '
                    '(person_id, deactivated_date) '
                    'VALUES{person_research_ids}')


class RetractDataBqTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = app_identity.get_application_id()
        self.fake_project_id = 'fake-project-id'
        self.bq_dataset_id = bq_utils.get_dataset_id()
        self.ticket_number = 'DCXXX'
        self.pid_table_id = 'pid_table'
        self.deactivated_ehr_participants = [(1, '2007-01-01'),
                                             (2, '2008-02-01'),
                                             (1234567, '2019-01-01')]

    @mock.patch(
        'retraction.retract_deactivated_pids.get_date_info_for_pids_tables')
    @mock.patch('bq_utils.list_datasets')
    def test_integration_queries_to_retract_from_fake_dataset(
        self, mock_list_datasets, mock_retraction_info):
        mock_list_datasets.return_value = [self.bq_dataset_id]
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
                'condition_end_date', 'drug_exposure_end_date',
                'measurement_date', 'observation_date', 'procedure_date',
                'visit_end_date'
            ]
        }
        retraction_info = pd.DataFrame(data=d)
        mock_retraction_info.return_value = retraction_info

        # Create and load person_ids and deactivated_date to pid table
        bq_utils.create_table(self.pid_table_id,
                              retract_deactivated_pids.PID_TABLE_FIELDS,
                              drop_existing=True,
                              dataset_id=self.bq_dataset_id)
        bq_formatted_insert_values = ', '.join([
            '(%s, "%s")' % (person_id, deactivated_date)
            for (person_id,
                 deactivated_date) in self.deactivated_ehr_participants
        ])
        q = INSERT_PID_TABLE.format(
            dataset_id=self.bq_dataset_id,
            pid_table_id=self.pid_table_id,
            person_research_ids=bq_formatted_insert_values)
        bq_utils.query(q)

        job_ids = []
        row_count_queries = []
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
            logging.info('Preparing to load table %s.%s' %
                         (self.bq_dataset_id, hpo_table))
            with open(cdm_file, 'rb') as f:
                gcs_utils.upload_object(gcs_utils.get_hpo_bucket(self.hpo_id),
                                        cdm_file_name, f)
            result = bq_utils.load_cdm_csv(self.hpo_id,
                                           cdm_table,
                                           dataset_id=self.bq_dataset_id)
            logging.info('Loading table %s.%s' %
                         (self.bq_dataset_id, hpo_table))
            job_id = result['jobReference']['jobId']
            job_ids.append(job_id)

        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        self.assertEqual(len(incomplete_jobs), 0,
                         'NYC five person load job did not complete')
        logging.info('All tables loaded successfully')

        # Store query for checking number of rows to delete
        for ehr in self.deactivated_ehr_participants:
            pid = ehr[0]
            for hpo in hpo_table_list:
                date_column = retraction_info.loc[
                    retraction_info['table'].str.contains(hpo),
                    'date_column'].item()
                query = EXPECTED_ROWS_QUERY.format(
                    dataset_id=self.bq_dataset_id,
                    table_id=hpo,
                    pid_table_id=self.pid_table_id,
                    pid=pid,
                    date_column=date_column)
                row_count_queries.append({
                    clean_consts.QUERY: query,
                    clean_consts.DESTINATION_DATASET: self.bq_dataset_id,
                    clean_consts.DESTINATION_TABLE: hpo
                })

        # Use query results to count number of expected row deletions
        expected_row_count = {}
        for query_dict in row_count_queries:
            result = bq_utils.query(query_dict['query'])
            expected_row_count[query_dict['destination_table_id']] = int(
                result['totalRows'])

        # Separate check to find number of actual deleted rows
        q = TABLE_ROWS_QUERY.format(dataset_id=self.bq_dataset_id)
        q_result = bq_utils.query(q)
        result = bq_utils.response2rows(q_result)
        row_count_before_retraction = {}
        for row in result:
            row_count_before_retraction[row['table_id']] = row['row_count']

        # Perform retraction
        query_list = retract_deactivated_pids.create_queries(
            self.project_id, self.ticket_number, self.project_id,
            self.bq_dataset_id, self.pid_table_id)
        retract_deactivated_pids.run_queries(query_list)

        # Find actual deleted rows
        q_result = bq_utils.query(q)
        result = bq_utils.response2rows(q_result)
        row_count_after_retraction = {}
        for row in result:
            row_count_after_retraction[row['table_id']] = row['row_count']
        for table in expected_row_count:
            self.assertEqual(
                expected_row_count[table], row_count_before_retraction[table] -
                row_count_after_retraction[table])

    def tearDown(self):
        test_util.delete_all_tables(self.bq_dataset_id)
