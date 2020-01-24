import os
import unittest
import mock

import app_identity

import bq_utils
import gcs_utils
from tests import test_util
from tools import retract_data_bq
from io import open


TABLE_ROWS_QUERY = (
    'SELECT * '
    'FROM {dataset_id}.__TABLES__ '
)

EXPECTED_ROWS_QUERY = (
    'SELECT * '
    'FROM {dataset_id}.{table_id} '
    'WHERE person_id IN '
    '(SELECT person_id '
    'FROM {dataset_id}.{pid_table_id})'
)

INSERT_PID_TABLE = (
    'INSERT INTO {dataset_id}.{pid_table_id} '
    '(person_id, research_id) '
    'VALUES{person_research_ids}'
)


class RetractDataBqTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = 'fake-project-id'
        self.test_project_id = app_identity.get_application_id()
        self.pid_table_id = 'pid_table'
        self.bq_dataset_id = bq_utils.get_unioned_dataset_id()
        self.dataset_ids = 'all_datasets'
        self.person_research_ids = [(1, 6890173), (2, 858761), (1234567, 4589763)]

    @mock.patch('tools.retract_data_bq.is_deid_dataset')
    @mock.patch('tools.retract_data_bq.is_combined_dataset')
    @mock.patch('tools.retract_data_bq.is_unioned_dataset')
    @mock.patch('tools.retract_data_bq.is_ehr_dataset')
    @mock.patch('bq_utils.list_datasets')
    def test_integration_queries_to_retract_from_fake_dataset(self,
                                                              mock_list_datasets,
                                                              mock_is_ehr_dataset,
                                                              mock_is_unioned_dataset,
                                                              mock_is_combined_dataset,
                                                              mock_is_deid_dataset):
        mock_list_datasets.return_value = [{'id': self.project_id + ':' + self.bq_dataset_id}]
        mock_is_deid_dataset.return_value = False
        mock_is_combined_dataset.return_value = False
        mock_is_unioned_dataset.return_value = False
        mock_is_ehr_dataset.return_value = True

        # create and load person_ids to pid table
        bq_utils.create_table(self.pid_table_id, retract_data_bq.PID_TABLE_FIELDS, drop_existing=True,
                              dataset_id=self.bq_dataset_id)
        bq_formatted_insert_values = ', '.join(['(%s, %s)' % (person_id, research_id)
                                                for (person_id, research_id) in self.person_research_ids])
        q = INSERT_PID_TABLE.format(dataset_id=self.bq_dataset_id,
                                    pid_table_id=self.pid_table_id,
                                    person_research_ids=bq_formatted_insert_values)
        bq_utils.query(q)

        job_ids = []
        row_count_queries = {}
        # load the cdm files into dataset
        for cdm_file in test_util.NYC_FIVE_PERSONS_FILES:
            cdm_file_name = os.path.basename(cdm_file)
            cdm_table = cdm_file_name.split('.')[0]
            hpo_table = bq_utils.get_table_id(self.hpo_id, cdm_table)
            # store query for checking number of rows to delete
            row_count_queries[hpo_table] = EXPECTED_ROWS_QUERY.format(dataset_id=self.bq_dataset_id,
                                                                      table_id=hpo_table,
                                                                      pid_table_id=self.pid_table_id)
            retract_data_bq.logger.info('Preparing to load table %s.%s' % (self.bq_dataset_id,
                                                                            hpo_table))
            with open(cdm_file, 'rb') as f:
                gcs_utils.upload_object(gcs_utils.get_hpo_bucket(self.hpo_id), cdm_file_name, f)
            result = bq_utils.load_cdm_csv(self.hpo_id, cdm_table, dataset_id=self.bq_dataset_id)
            retract_data_bq.logger.info('Loading table %s.%s' % (self.bq_dataset_id,
                                                                  hpo_table))
            job_id = result['jobReference']['jobId']
            job_ids.append(job_id)
        incomplete_jobs = bq_utils.wait_on_jobs(job_ids)
        self.assertEqual(len(incomplete_jobs), 0, 'NYC five person load job did not complete')
        retract_data_bq.logger.info('All tables loaded successfully')

        # use query results to count number of expected row deletions
        expected_row_count = {}
        for table in row_count_queries:
            result = bq_utils.query(row_count_queries[table])
            expected_row_count[table] = retract_data_bq.to_int(result['totalRows'])

        # separate check to find number of actual deleted rows
        q = TABLE_ROWS_QUERY.format(dataset_id=self.bq_dataset_id)
        q_result = bq_utils.query(q)
        result = bq_utils.response2rows(q_result)
        row_count_before_retraction = {}
        for row in result:
            row_count_before_retraction[row['table_id']] = row['row_count']

        # perform retraction
        retract_data_bq.run_retraction(self.test_project_id, self.bq_dataset_id, self.pid_table_id, self.hpo_id,
                                       self.dataset_ids)

        # find actual deleted rows
        q_result = bq_utils.query(q)
        result = bq_utils.response2rows(q_result)
        row_count_after_retraction = {}
        for row in result:
            row_count_after_retraction[row['table_id']] = row['row_count']
        for table in expected_row_count:
            self.assertEqual(expected_row_count[table],
                             row_count_before_retraction[table] - row_count_after_retraction[table])

    def tearDown(self):
        test_util.delete_all_tables(self.bq_dataset_id)
