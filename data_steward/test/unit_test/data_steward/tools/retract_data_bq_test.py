from __future__ import print_function
import os
import unittest
import mock

import app_identity

import bq_utils
import cdm
import common
import gcs_utils
import resources
from test.unit_test import test_util
from tools import retract_data_bq
from validation import ehr_union
from io import open


TABLE_ROWS_QUERY = """
SELECT *
FROM {dataset_id}.__TABLES__
"""

EXPECTED_ROWS_QUERY = """
SELECT *
FROM {dataset_id}.{table_id}
WHERE person_id IN
(SELECT person_id
FROM {dataset_id}.{pid_table_id})
"""

INSERT_PID_TABLE = """
INSERT INTO {dataset_id}.{pid_table_id}
(person_id, research_id)
VALUES{person_research_ids}
"""


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
        self.ehr_dataset_id = 'ehr20190801_fake'
        self.unioned_dataset_id = 'unioned_ehr20190801'
        self.combined_dataset_id = 'combined20190801'
        self.sandbox_dataset_id = 'sandbox_dataset'
        self.pid_table_id = 'pid_table'
        self.bq_dataset_id = bq_utils.get_unioned_dataset_id()
        self.person_research_ids = [(1, 6890173), (2, 858761), (1234567, 4589763)]
        self.tables_to_retract_unioned = retract_data_bq.TABLES_FOR_RETRACTION | {common.FACT_RELATIONSHIP, common.PERSON}
        self.tables_to_retract_combined = retract_data_bq.TABLES_FOR_RETRACTION | {common.FACT_RELATIONSHIP}
        self.all_tables = resources.CDM_TABLES

    def test_is_combined_dataset(self):
        self.assertTrue(retract_data_bq.is_combined_dataset('combined20190801'))
        self.assertFalse(retract_data_bq.is_combined_dataset('combined20190801_deid'))
        self.assertTrue(retract_data_bq.is_combined_dataset('combined20190801_base'))
        self.assertTrue(retract_data_bq.is_combined_dataset('combined20190801_clean'))
        self.assertFalse(retract_data_bq.is_combined_dataset('combined20190801_deid_v1'))

    def test_is_deid_dataset(self):
        self.assertFalse(retract_data_bq.is_deid_dataset('combined20190801'))
        self.assertTrue(retract_data_bq.is_deid_dataset('combined20190801_deid'))
        self.assertFalse(retract_data_bq.is_deid_dataset('combined20190801_base'))
        self.assertFalse(retract_data_bq.is_deid_dataset('combined20190801_clean'))
        self.assertTrue(retract_data_bq.is_deid_dataset('combined20190801_deid_v1'))

    @mock.patch('bq_utils.get_dataset_id')
    def test_is_ehr_dataset(self, mock_get_dataset_id):
        mock_get_dataset_id.return_value = self.bq_dataset_id
        self.assertTrue(retract_data_bq.is_ehr_dataset('ehr20190801'))
        self.assertTrue(retract_data_bq.is_ehr_dataset('ehr_20190801'))
        self.assertFalse(retract_data_bq.is_ehr_dataset('unioned_ehr_20190801_base'))
        self.assertFalse(retract_data_bq.is_ehr_dataset('unioned_ehr20190801_clean'))
        self.assertTrue(retract_data_bq.is_ehr_dataset(self.bq_dataset_id))

    def test_is_unioned_dataset(self):
        self.assertFalse(retract_data_bq.is_unioned_dataset('ehr20190801'))
        self.assertFalse(retract_data_bq.is_unioned_dataset('ehr_20190801'))
        self.assertTrue(retract_data_bq.is_unioned_dataset('unioned_ehr_20190801_base'))
        self.assertTrue(retract_data_bq.is_unioned_dataset('unioned_ehr20190801_clean'))

    @mock.patch('tools.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_ehr_dataset(self, mock_list_existing_tables):
        hpo_person = bq_utils.get_table_id(self.hpo_id, common.PERSON)
        hpo_death = bq_utils.get_table_id(self.hpo_id, common.DEATH)

        # hpo tables
        existing_table_ids = [hpo_person, hpo_death]
        for table in self.tables_to_retract_unioned:
            table_id = bq_utils.get_table_id(self.hpo_id, table)
            existing_table_ids.append(table_id)

        # unioned tables
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            unioned_table_id = retract_data_bq.UNIONED_EHR + cdm_table
            existing_table_ids.append(unioned_table_id)

            if cdm_table not in self.tables_to_retract_unioned:
                ignored_tables.append(unioned_table_id)

        mapped_tables = cdm.tables_to_map()

        # fact_relationship does not have pid, is handled separate from other mapped tables
        for mapped_table in mapped_tables:
            mapping_table = ehr_union.mapping_table_for(mapped_table)
            existing_table_ids.append(mapping_table)
            legacy_mapping_table = retract_data_bq.UNIONED_EHR + mapping_table
            existing_table_ids.append(legacy_mapping_table)
            if mapped_table not in self.tables_to_retract_unioned:
                ignored_tables.append(mapping_table)
                ignored_tables.append(legacy_mapping_table)

        mock_list_existing_tables.return_value = existing_table_ids
        mqs, qs = retract_data_bq.queries_to_retract_from_ehr_dataset(self.project_id,
                                                                      self.ehr_dataset_id,
                                                                      self.sandbox_dataset_id,
                                                                      self.hpo_id,
                                                                      self.pid_table_id)
        actual_dest_tables = set(q[retract_data_bq.DEST_TABLE] for q in qs+mqs)
        expected_dest_tables = set(existing_table_ids) - set(hpo_person) - set(ignored_tables)
        self.assertSetEqual(expected_dest_tables, actual_dest_tables)

    @mock.patch('tools.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_combined_or_deid_dataset(self, mock_list_existing_tables):
        existing_table_ids = []
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            existing_table_ids.append(cdm_table)
            if cdm_table not in self.tables_to_retract_combined:
                ignored_tables.append(cdm_table)

        mapped_tables = cdm.tables_to_map()
        for mapped_table in mapped_tables:
            mapping_table = ehr_union.mapping_table_for(mapped_table)
            existing_table_ids.append(mapping_table)
            if mapped_table not in self.tables_to_retract_combined:
                ignored_tables.append(mapping_table)

        mock_list_existing_tables.return_value = existing_table_ids
        mqs, qs = retract_data_bq.queries_to_retract_from_combined_or_deid_dataset(self.project_id,
                                                                                   self.combined_dataset_id,
                                                                                   self.sandbox_dataset_id,
                                                                                   self.pid_table_id,
                                                                                   deid_flag=False)
        actual_dest_tables = set(q[retract_data_bq.DEST_TABLE] for q in qs+mqs)
        expected_dest_tables = set(existing_table_ids) - set(ignored_tables)
        self.assertSetEqual(expected_dest_tables, actual_dest_tables)

        # death query should use person_id as-is (no constant factor)
        constant_factor = common.RDR_ID_CONSTANT + common.ID_CONSTANT_FACTOR
        for q in qs:
            if q[retract_data_bq.DEST_TABLE] is common.DEATH:
                self.assertNotIn(str(constant_factor), q[retract_data_bq.QUERY])

    @mock.patch('tools.retract_data_bq.list_existing_tables')
    def test_queries_to_retract_from_unioned_dataset(self, mock_list_existing_tables):
        existing_table_ids = []
        ignored_tables = []
        for cdm_table in resources.CDM_TABLES:
            existing_table_ids.append(cdm_table)
            if cdm_table not in self.tables_to_retract_unioned:
                ignored_tables.append(cdm_table)

        mapped_tables = cdm.tables_to_map()
        for mapped_table in mapped_tables:
            mapping_table = ehr_union.mapping_table_for(mapped_table)
            existing_table_ids.append(mapping_table)
            if mapped_table not in self.tables_to_retract_unioned:
                ignored_tables.append(mapping_table)

        mock_list_existing_tables.return_value = existing_table_ids
        mqs, qs = retract_data_bq.queries_to_retract_from_unioned_dataset(self.project_id,
                                                                          self.unioned_dataset_id,
                                                                          self.sandbox_dataset_id,
                                                                          self.pid_table_id)
        actual_dest_tables = set(q[retract_data_bq.DEST_TABLE] for q in qs+mqs)
        expected_dest_tables = set(existing_table_ids) - set(ignored_tables)
        self.assertSetEqual(expected_dest_tables, actual_dest_tables)

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
        mock_list_datasets.return_value = [{'id': self.project_id+':'+self.bq_dataset_id}]
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
        retract_data_bq.run_retraction(self.test_project_id, self.bq_dataset_id, self.pid_table_id, self.hpo_id)

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
