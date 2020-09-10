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
from google.api_core.retry import Retry
from google.cloud import bigquery
from jinja2 import Environment

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
            cls.jinja_env = Environment(
                # help protect against cross-site scripting vulnerabilities
                autoescape=True,
                # block tags on their own lines
                # will not cause extra white space
                trim_blocks=True,
                lstrip_blocks=True,
                # syntax highlighting should be better
                # with these comment delimiters
                comment_start_string='--',
                comment_end_string=' --')

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
                dataset = bq.define_dataset(cls.project_id, dataset_id, desc,
                                            {'test': ''})
                cls.client.create_dataset(dataset, exists_ok=True)

        def setUp(self):
            bq.create_tables(self.client, self.project_id, self.fq_table_names,
                             True)

        @classmethod
        def tearDownClass(cls):
            """
            Remove the test dataset table(s).
            """
            for table in cls.fq_table_names + cls.fq_sandbox_table_names:
                cls.client.delete_table(table, not_found_ok=True)

        def drop_rows(self, fq_table_name):
            """
            Helper function to drop table rows and assert the drop finished.

            Raises an assertion error if the table records are not dropped.
            Drops all rows.

            :param fq_table_name: a fully qualified table name to drop all
                records for
            """
            query = f"delete from {fq_table_name} where true"

            query_retry = Retry()
            response = self.client.query(query, retry=query_retry, timeout=30)

            # start the job and wait for it to complete
            self.assertIsNotNone(response.result())
            self.assertIsNone(response.exception())

        def tearDown(self):
            """
            Ensure the data is dropped from the table(s).
            """
            for table in self.fq_table_names + self.fq_sandbox_table_names:
                self.drop_rows(table)
                self.client.delete_table(table)

        def load_test_data(self, sql_statements=None):
            """
            Add data to the tables for the rule to run on.

            :param sql_statements: a list of sql statements to load for testing.
            """

            if not sql_statements or not isinstance(sql_statements, list):
                raise RuntimeError("Provide parameter sql_statements.  The "
                                   "parameter must be list of sql strings.")

            for query in sql_statements:
                response = self.client.query(query)
                self.assertIsNotNone(response.result())
                self.assertIsNone(response.exception())

        def assertTableDoesNotExist(self, fq_table_name):
            """
            Helper function to assert a table doesn't exist.

            Asserts trying to get a row count from a table that does not
            exists raises an exception.

            :param fq_table_name: a fully qualified table name
            """
            response = self.client.query(
                f"select count(*) from `{fq_table_name}`")
            self.assertRaises(gc_exc.GoogleCloudError,
                              response.result,
                              timeout=15)

        def assertRowIDsMatch(self, fq_table_name, fields, expected_values):
            """
            Helper function to assert a table's single field values.

            This method should only be used to compare unique identifier
            fields, e.g. primary key fields.

            :param fq_table_name: table to query
            :param fields: a list of fields to query from the table.  For this
                method, this should be a list of one.
            :param expected_values: a list of values to expect back from the
                query.  This is turned into a list of tuple values for this use
                case.  This list is then passed to assertTableValuesMatch

            :raises:  RuntimeError if the length of fields is greater than 1
            """
            if len(fields) > 1:
                raise RuntimeError('Using too many fields to check identifiers')

            expected_tuples = [(value,) for value in expected_values]

            self.assertTableValuesMatch(fq_table_name, fields, expected_tuples)

        def assertTableValuesMatch(self, fq_table_name, fields,
                                   expected_values):
            """
            Helper function to assert a tables contents.

            This method assumes the first value in each row is a uniquely
            identifiable value, e.g. a primary key.  It relies on this
            value being unique when performing the assertEquals check.  So, if
            two expected_values[0] are the same and the rest of the row is
            different, this function will likely fail when iterating the list
            of returned tuples.

            :param fq_table_name: table to query
            :param fields: a list of fields to query from the table, the first
                field should be a uniquely identifying field
            :param expected_values: a list of values to expect back from the
                query.  values should be defined in the same order as the
                listed fields.
            """
            fields_str = ', '.join(fields)
            query = f"select {fields_str} from `{fq_table_name}`"
            response = self.client.query(query)
            # start the job and wait for it to complete
            response_list = list(response.result())

            message = (f"Assertion for table {fq_table_name} failed.\n"
                       f"Response returned these values {response_list}")
            self.assertEqual(len(expected_values), len(response_list), message)

            # assert matches for lists of tuple data.  e.g. list of returned fields
            for value_tuple in expected_values:
                for result in response_list:
                    if value_tuple[0] == result[0]:
                        # have to get the response in this format.  else,
                        # a comparison of a tuple and a Row object fails
                        result_tuple = result[:]
                        message = (
                            f"Assertion for table {fq_table_name} failed.\n"
                            f"The result tuple {result_tuple} doesn't match the "
                            f"expected value tuple {value_tuple}\n{response_list}\n"
                        )
                        self.assertEqual(value_tuple, result_tuple, message)

    class CleaningRulesTestBase(BigQueryTestBase):
        """
        Class that can be extended and used to test cleaning rules.

        This class defines basic tests that can be used by extending classes
        to support integration testing.  The test functions can be overridden
        by extending classes, if desired.  These tests will minimally ensure
        that sandbox tables are empty and all data is loaded prior to test
        execution.  They will then ensure only expected cleaned data exists in
        the cleaned tables, and defined sandboxed data exists in the sandbox
        table(s).  This class is optional and is not required.  It is here to
        support testing efforts.  All assertions are based on {tablename}_id fields.
        """

        @classmethod
        def initialize_class_vars(cls):
            super().initialize_class_vars()
            # The query class that is being executed.
            cls.query_class = None

        def setUp(self):
            """
            Add data to the tables for the rule to run on.
            """
            super().setUp()

        def default_test(self, tables_and_test_values):
            """
            Test passing the query specifications to the clean engine module.

            This should test that the specifications for the query perform
            as designed.  The rule should drop only what it is designed to
            drop.  No more and no less.  This expects the rule to create the
            required sandbox tables.  Because it is using the clean_dataset
            function, special setup features do not need to be accounted for
            in this test because they will be executed by the engine.

            :param tables_and_test_values: a list of dictionaries where each
                dictionary defines table expectations for each OMOP table to
                validate with rule execution.  The dictionaries require:
             'fq_table_name':  the fully qualified name of the table being cleaned
             'fq_sanbox_table_name':  the fully qualified name of the sandbox
                                      table the rule will create if one is
                                      expected.
             'loaded_ids':  The list of ids loaded by the sql insert statement
             'sandboxed_ids':  the list of ids that will be in the sandbox if
                               the rule sandboxes information
             'cleaned_values':  the list of tupled ids and expected values that will
                                exist in the cleaned table after
                                running the cleaning rule.  the order of the
                                expected values must match the order of the fields
                                defined in fields.
             'fields': a list of fields to select from the table after it has
                       been cleaned. the listed order should match the expected
                       order of the cleaned_values tuples.  the first item in
                       the list should be a unique identifier, e.g. primary key field
            """
            # pre-conditions
            # validate sandbox tables don't exist yet
            for fq_table_name in self.fq_sandbox_table_names:
                self.assertTableDoesNotExist(fq_table_name)

            # validate only anticipated input records exist before starting
            for table_info in tables_and_test_values:
                fq_table_name = table_info.get('fq_table_name', 'UNSET')
                values = table_info.get('loaded_ids', [])
                # this is assuming the uniquely identifiable field name is specified
                # first in the fields list.  this check verifies by id field
                # that the table data loaded correctly.
                fields = [table_info.get('fields', [])[0]]
                self.assertRowIDsMatch(fq_table_name, fields, values)

            if self.query_class:
                # test:  get the queries
                query_list = self.query_class.get_query_specs()

                # test: run the queries
                engine.clean_dataset(self.project_id, query_list)
            else:
                raise RuntimeError(f"Cannot use the default_test method for "
                                   f"{self.__class__.__name__} because "
                                   f"query_class is undefined.")

            # post conditions
            for table_info in tables_and_test_values:
                # validate records are dropped
                fq_table_name = table_info.get('fq_table_name', 'UNSET')
                values = table_info.get('cleaned_values', [])
                fields = table_info.get('fields', [])
                self.assertTableValuesMatch(fq_table_name, fields, values)

                # validate records are sandboxed
                fq_sandbox_name = table_info.get('fq_sandbox_table_name')
                if fq_sandbox_name:
                    values = table_info.get('sandboxed_ids', [])
                    # this is assuming the uniquely identifiable field name is specified
                    # first in the fields list.  this check verifies by id field
                    # that the table data loaded correctly.
                    fields = [table_info.get('fields', [])[0]]
                    self.assertRowIDsMatch(fq_sandbox_name, fields, values)

    class DeidRulesTestBase(CleaningRulesTestBase):
        """
        Class that can be extended and used to test deid cleaning rules.

        This class adds a helper to create a mapping table and a tearDown
        to remove the named deid map table.
        """

        @classmethod
        def initialize_class_vars(cls):
            super().initialize_class_vars()
            cls.fq_mapping_tablename = ''
            cls.fq_questionnaire_tablename = ''

        def create_mapping_table(self):
            """
            Create a mapping table with a mapping table schema.
            """

            # create a false mapping table
            schema = [
                bigquery.SchemaField("person_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("research_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("shift", "INTEGER", mode="REQUIRED"),
            ]

            table = bigquery.Table(self.fq_mapping_tablename, schema=schema)
            table = self.client.create_table(table)  # Make an API request.

        def create_questionnaire_response_mapping_table(self):
            """
            Create a mapping table with a mapping table schema.

            Similar to the deid mapping table, but for questionnaire response ids.
            """

            # create a false mapping table
            schema = [
                bigquery.SchemaField("questionnaire_response_id",
                                     "INTEGER",
                                     mode="REQUIRED"),
                bigquery.SchemaField("research_response_id",
                                     "INTEGER",
                                     mode="REQUIRED"),
            ]

            table = bigquery.Table(self.fq_questionnaire_tablename,
                                   schema=schema)
            table = self.client.create_table(table)  # Make an API request.

        def tearDown(self):
            """
            Clear and drop the mapping table between each test.
            """
            # delete the mapping table
            for table in [
                    self.fq_mapping_tablename, self.fq_questionnaire_tablename
            ]:
                if table:
                    self.client.delete_table(table)

            super().tearDown()
