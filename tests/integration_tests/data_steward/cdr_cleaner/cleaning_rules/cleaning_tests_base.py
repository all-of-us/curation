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
import unittest

# Third party imports
import google.cloud.exceptions as gc_exc

# Project imports
from cdr_cleaner import clean_cdr_engine as engine
from utils import bq


class BaseTest:

    class CleaningRulesTestBase(unittest.TestCase):

        project_id = ''
        test_dataset_ids = []
        sandbox_dataset_ids = []
        fq_table_names = []
        fq_sandbox_table_names = []
        tables_and_counts = []
        sql_load_statements = []
        query_class = None

        @classmethod
        def setUpClass(cls):
            # get the test project
            if 'test' not in cls.project_id:
                raise RuntimeError(
                    'This should only be run on a test environment')

            # get or create datasets, most cleaning rules assume the datasets exist
            for dataset_id in cls.test_dataset_ids:
                bq.get_or_create_dataset(cls.project_id, dataset_id)

            for dataset_id in cls.sandbox_dataset_ids:
                bq.get_or_create_dataset(cls.project_id, dataset_id)

            # create empty table in test environment
            cls.client = bq.get_client(cls.project_id)

            # the base class will try to determine this from the list of
            # tables and counts if the list is empty
            if not cls.fq_table_names:
                for table_info in cls.tables_and_counts:
                    cls.fq_table_names.append(
                        table_info.get('fq_table_name', ''))

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
            Remove the test dataset table(s).
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

    class DropRowsTestBase(CleaningRulesTestBase):

        def test_execution_of_get_query_specs(self):
            """
            Test passing the query specifications to the clean engine module.

            This should test that the specifications for the query perform
            as designed.  The rule should drop only what it is designed to
            drop.  No more and no less.  This expects the rule to create the
            required sandbox tables.  Because it is using the clean_dataset
            function, special setup features do not need to be accounted for
            in this test because they will be executed by the engine.
            """
            # pre-conditions
            # validate sandbox table doesn't exist yet
            for fq_table_name in self.fq_sandbox_table_names:
                response = bq.query(f"select count(*) from `{fq_table_name}`",
                                    self.project_id)
                self.assertRaises(gc_exc.GoogleCloudError,
                                  response.result,
                                  timeout=15)

            # validate only anticipated input records exist before starting
            for table_info in self.tables_and_counts:
                table_name = table_info.get('name')
                query = f"select {table_name}_id from `{table_info.get('fq_table_name', '')}`"
                response = bq.query(query, self.project_id)
                # start the job and wait for it to complete
                result_list = list(response.result())
                self.assertEqual(
                    len(table_info.get('loaded_ids', [])), len(result_list),
                    f"The pre-condition query did not return expected number of rows.  Selection query is:\n{query}"
                )

                for item in result_list:
                    self.assertIn(item[0], table_info.get('loaded_ids', []),
                                  "The test data did not load as expected")

            # test:  get the queryies
            query_list = self.query_class.get_query_specs()

            # test: run the queries
            engine.clean_dataset(self.project_id, query_list)

            # post conditions
            for table_info in self.tables_and_counts:
                # validate three records are dropped
                table_name = table_info.get('name')
                query = f"select {table_name}_id from `{table_info.get('fq_table_name', '')}`"
                response = bq.query(query, self.project_id)
                result_list = list(response.result())

                output_ids = table_info.get('cleaned_ids', [])
                self.assertEqual(len(output_ids), len(result_list))

                rows_kept = []
                for row in result_list:
                    rows_kept.append(row[0])

                # assert the contents are equal regardless of the order
                self.assertCountEqual(output_ids, rows_kept)

                # validate three records are sandboxed
                query = (
                    f"select {table_name}_id "
                    f"from `{table_info.get('fq_sandbox_table_name', '')}`")
                response = bq.query(query, self.project_id)
                result_list = list(response.result())

                sandboxed_ids = table_info.get('sandboxed_ids', [])
                self.assertEqual(len(sandboxed_ids), len(result_list))

                rows_kept = []
                for row in result_list:
                    rows_kept.append(row[0])

                # assert the contents are equal regardless of the order
                self.assertCountEqual(sandboxed_ids, rows_kept)
