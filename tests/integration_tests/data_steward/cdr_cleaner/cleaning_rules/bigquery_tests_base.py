"""
Integration Test base classes for cleaning rules.

Implements a base class that will ensure test datasets and empty tables with
schemas are created before beginning the test.  It will remove tables and test
datasets at cleanup.  It will provide optional classes that extend this
base class for simple rules that only modify data or drop rows.  These special
classes can be extended to use their test execution parameters.  If a more
detailed implementation is needed, the developer can still extend the base class
only and implement their own tests.
"""
# Python imports
import unittest

# Third party imports
import google.cloud.exceptions as gc_exc

# Project imports
from cdr_cleaner import clean_cdr_engine as engine
from utils import bq


class BaseTest:

    class BigQueryTestBase(unittest.TestCase):
        """
        A base class for big query integration tests.

        This class implements setUp, setUpClass, tearDown, and tearDownClass
        methods that can be inherited and used by all extending classes.  This
        allows developers to focus more fully on creating a working test rather
        than setting up or tearing down their test environment.

        The base class ensures test and sandbox datasets are created.  It also
        creates empty test tables with correct schemas to further validate the
         sql.  The base class ensures the test tables are reloaded with the
        defined sample data between tests.  It also ensures tables are deleted
        when the test is finished.
        """

        @classmethod
        def initialize_class_vars(cls):
            # this should always be for a test project.  a check is implemented to
            # make sure this is true
            cls.project_id = ''
            # a list of fully qualified table names the cleaning rule is targeting.
            # fq = {project_id}.{dataset_id}.{table_name}.  required for
            # test setup and cleanup.
            cls.fq_table_names = []
            # a list of fully qualified sandbox tables the rule may create.
            # required for cleanup
            cls.fq_sandbox_table_names = []
            # a list of load statements to execute before each test.  This can be
            # defined in the extending class's 'setUp(self)' function.
            cls.sql_load_statements = []
            # bq client object responsible for creating and tearing down BigQuery resources
            cls.client = None

        @classmethod
        def setUpClass(cls):
            # get the test project
            if 'test' not in cls.project_id:
                raise RuntimeError(
                    f'Tests should only run in a test environment.  '
                    f'Current environment is {cls.project_id} .')

            if not cls.fq_table_names:
                raise RuntimeError(
                    f'Provide a list of fully qualified table names the '
                    f'test will manipulate.')

            cls.client = bq.get_client(cls.project_id)

            # get or create datasets, cleaning rules can assume the datasets exist
            required_datasets = []
            for table_name in cls.fq_table_names + cls.fq_sandbox_table_names:
                dataset_id = table_name.split('.')[1]
                required_datasets.append(dataset_id)

            desc = (f"dataset created by {cls.__name__} to test a "
                    f"cleaning rule.  deletion candidate.")
            for dataset_id in set(required_datasets):
                bq.get_or_create_dataset(cls.client, cls.project_id, dataset_id,
                                         desc)

            bq.create_tables(cls.client, cls.project_id, cls.fq_table_names,
                             True)

        @classmethod
        def tearDownClass(cls):
            """
            Remove the test dataset table(s).
            """
            for table in cls.fq_table_names + cls.fq_sandbox_table_names:
                cls.client.delete_table(table)

        def drop_rows(self, fq_table_name):
            """
            Helper function to drop table rows and assert the drop finished.

            Raises an assertion error if the table records are not dropped.
            Drops all rows.

            :param fq_table_name: a fully qualified table name to drop all
                records for
            """
            query = f"delete from {fq_table_name} where true"

            response = self.client.query(query)

            # start the job and wait for it to complete
            self.assertIsNotNone(response.result())
            self.assertIsNone(response.exception())

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
                response = self.client.query(query)
                self.assertIsNotNone(response.result())
                self.assertIsNone(response.exception())

    class DropRowsTestBase(BigQueryTestBase):
        """
        Class that can be extended and used to test cleaning rules that drop row data.

        This class defines a basic test that can be used by extending classes
        in lieu of writing the test function.  The test function can be overridden
        by extending classes, if desired.  This test will minimally ensure
        that sandbox tables are empty and all data is loaded prior to test
        execution.  It will then ensure only expected cleaned data exists in
        the cleaned tables, and defined sandboxed data exists in the sandbox
        table(s).  This class is optional and is not required.  It is here to
        make testing easier.  All assertions are based on {tablename}_id fields.
        """

        @classmethod
        def initialize_class_vars(cls):
            super().initialize_class_vars()
            # a list of dictionaries where each dictionary defines table
            # expectations for each table to validate with rule execution, such as:
            # 'name':  common name of the table being cleaned and defining
            #          execution expectations for
            # 'fq_table_name':  the fully qualified name of the table being cleaned
            # 'fq_sanbox_table_name':  the fully qualified name of the sandbox
            #                          table the rule will create if one exists
            # 'loaded_ids':  The list of ids loaded by the sql insert statement
            # 'sandboxed_ids':  the list of ids that will be in the sandbox if
            #                   the rule sandboxes information
            # 'cleaned_ids':  the list of ids that will continue to exist in the
            #                 input table after running the cleaning rule
            cls.tables_and_test_values = []
            # The query class that is being executed.
            cls.query_class = None

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
                response = self.client.query(
                    f"select count(*) from `{fq_table_name}`")
                self.assertRaises(gc_exc.GoogleCloudError,
                                  response.result,
                                  timeout=15)

            # validate only anticipated input records exist before starting
            for table_info in self.tables_and_test_values:
                table_name = table_info.get('name')
                query = f"select {table_name}_id from `{table_info.get('fq_table_name', '')}`"
                response = self.client.query(query)
                # start the job and wait for it to complete
                result_list = list(response.result())
                self.assertEqual(
                    len(table_info.get('loaded_ids', [])), len(result_list),
                    (f"The pre-condition query did not return expected number "
                     f"of rows.  Selection query is:\n{query}"))

                for item in result_list:
                    self.assertIn(item[0], table_info.get('loaded_ids', []),
                                  "The test data did not load as expected")

            # test:  get the queries
            query_list = self.query_class.get_query_specs()

            # test: run the queries
            engine.clean_dataset(self.project_id, query_list)

            # post conditions
            for table_info in self.tables_and_test_values:
                # validate three records are dropped
                table_name = table_info.get('name')
                query = f"select {table_name}_id from `{table_info.get('fq_table_name', '')}`"
                response = self.client.query(query)
                result_list = list(response.result())

                output_ids = table_info.get('cleaned_ids', [])
                self.assertEqual(len(output_ids), len(result_list))

                rows_kept = []
                for row in result_list:
                    rows_kept.append(row[0])

                # assert the contents are equal regardless of the order
                self.assertCountEqual(output_ids, rows_kept)

                # validate three records are sandboxed
                fq_sandbox_name = table_info.get('fq_sandbox_table_name')
                if fq_sandbox_name:
                    query = f"select {table_name}_id from `{fq_sandbox_name}`"
                    response = self.client.query(query)
                    result_list = list(response.result())

                    sandboxed_ids = table_info.get('sandboxed_ids', [])
                    self.assertEqual(len(sandboxed_ids), len(result_list))

                    rows_kept = []
                    for row in result_list:
                        rows_kept.append(row[0])

                    # assert the contents are equal regardless of the order
                    self.assertCountEqual(sandboxed_ids, rows_kept)
