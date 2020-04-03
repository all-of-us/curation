"""
Integration Test for the rdr_observation_source_concept_id_suppression module.

Remove three irrelevant observation_source_concept_ids from the RDR dataset.

Original Issue:  DC-734 implements integration tests for DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.  It should
also ensure that records that have null values or do not match the specified
ids are not removed.
"""
# Python imports
import os
import unittest

# Third party imports
from google.cloud import bigquery
import google.cloud.exceptions as gc_exc
from jinja2 import Template

# Project imports
import resources
from app_identity import PROJECT_ID
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner import clean_cdr_engine as engine
from utils import bq


class BaseTest:

    class CleaningRulesTestBase(unittest.TestCase):

        sql_load_statements = []
        dataset_ids = []
        sandbox_ids = []
        datasets = []
        sandbox_datasets = []
        fq_table_names = []
        fq_sandbox_table_names = []
        insert_fake_participants_tmpls = []
        project_id = ''

        @classmethod
        def setUpClass(cls):
            # get the test project
            if 'test' not in cls.project_id:
                raise RuntimeError(
                    'This should only be run on a test environment')

            # get or create datasets, most cleaning rules assume the datasets exist
            for dataset_id in cls.dataset_ids:
                cls.datasets.append(
                    bq.get_or_create_dataset(cls.project_id, dataset_id))

            for dataset_id in cls.sandbox_ids:
                cls.sandbox_datasets.append(
                    bq.get_or_create_dataset(cls.project_id, dataset_id))

            # create empty table in test environment
            cls.client = bq.get_client(cls.project_id)

            table_info = {
                # list of fully qualified table names to create with schemas
                'fq_table_names': cls.fq_table_names,
                'project_id': cls.project_id,
                'exists_ok': True,
            }
            bq.create_tables(**table_info)

        @classmethod
        def tearDownClass(cls):
            """
            Remove the test daaset table(s).
            """
            for table in cls.fq_table_names + cls.fq_sandbox_table_names:
                cls.client.delete_table(table)

        def drop_rows(self, tablename):
            drop_rows_job = self.client.query(
                f"delete from {tablename} where observation_id > -1")
            job_status = bq.wait_on_jobs(self.project_id,
                                         [drop_rows_job.job_id])

            self.assertFalse(
                job_status, f"The contents of {tablename} could not be cleared")

        def tearDown(self):
            """
            Ensure the data is dropped from the table(s).
            """
            for table in self.fq_table_names + self.fq_sandbox_table_names:
                self.drop_rows(table)

        def setUp(self):
            """
            Add data to the tables for the rule to run on.
            """
            # load the data from the sql strings and validate it loaded
            for query in self.sql_load_statements:
                response = bq.query(query, self.project_id)
                self.assertIsNotNone(response.result())
                self.assertIsNone(response.exception())

            print(f"{self.__class__.__name__} test setup from base class")

        def test_rule_parameters(self):
            """
            Test getting the query specifications.

            This should test that the specifications for the query perform
            as designed.  The rule should drop only what it is designed to
            drop.  No more and no less.
            """
            #        # pre-conditions
            #        # validate sandbox table doesn't exist yet
            #        response = bq.query(f"select count(*) from `{self.fq_sandbox_table}`",
            #                            self.project_id)
            #        self.assertRaises(gc_exc.GoogleCloudError, response.result, timeout=15)
            #
            #        # validate only 5 records exist before starting
            #        response = bq.query(
            #            f"select observation_id, observation_source_concept_id from `{self.fq_table_name}`",
            #            self.project_id)
            #        result_list = list(response.result())
            #        self.assertEqual(
            #            5, len(result_list),
            #            "The pre-condition query did not return expected number of rows")
            #        # start the job and wait for it to complete
            #        for item in result_list:
            #            self.assertIn(item[0], self.sandboxed_ids + self.output_ids,
            #                          "The test data did not load as expected")
            #
            #        # test
            #        query_list = self.query_class.get_query_specs()
            #
            #        # run the queries
            #        engine.clean_dataset(self.project_id, query_list)
            #
            #        # post conditions
            #        # validate three records are dropped
            #        response = bq.query(
            #            f"select observation_id from `{self.fq_table_name}`",
            #            self.project_id)
            #        result_list = list(response.result())
            #        self.assertEqual(2, len(result_list))
            #
            #        rows_kept = []
            #        for row in result_list:
            #            rows_kept.append(row[0])
            #
            #        # assert the contents are equal regardless of the order
            #        self.assertCountEqual(self.output_ids, rows_kept)
            #
            #        for row_id in self.sandboxed_ids:
            #            self.assertNotIn(row_id, rows_kept)
            #
            #        # validate three records are saved
            #        response = bq.query(
            #            f"select observation_id from `{self.fq_sandbox_table}`",
            #            self.project_id)
            #        result_list = list(response.result())
            #        self.assertEqual(3, len(result_list))
            #
            #        rows_kept = []
            #        for row in result_list:
            #            rows_kept.append(row[0])
            #
            #        # assert the contents are equal regardless of the order
            #        self.assertCountEqual(self.sandboxed_ids, rows_kept)
            #
            #        for row_id in self.output_ids:
            #            self.assertNotIn(row_id, rows_kept)
            print(f"{self.__class__.__name__} test")
