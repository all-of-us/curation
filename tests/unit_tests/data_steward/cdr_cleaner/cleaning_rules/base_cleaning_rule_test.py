"""
Testing the base cleaning rule class with some dummy classes.

The point is to make it explicitly clear what is and is not required.
"""
# Python imports
import unittest

# Third party imports
import googleapiclient
import oauth2client
from mock import patch

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, \
    get_delete_empty_sandbox_tables_queries, DROP_EMPTY_SANDBOX_TABLES_QUERY
from constants.cdr_cleaner import clean_cdr as cdr_consts


class Inheritance(BaseCleaningRule):
    """
    Class that correctly sets all required parameters and methods.

    Extends the BaseCleaningRule class correctly.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = ('Basic test case without depends_on parameter')
        super().__init__(issue_numbers=['AA-000'],
                         description=desc,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class BadIssueNumbers(BaseCleaningRule):
    """
    Class that incorrectly sets issue_numbers as a string parameter.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = (
            'Defines issue_numbers as something other than a list of strings')
        super().__init__(issue_numbers='AA-000',
                         issue_urls=['www.fake_url.com'],
                         description=desc,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class BadIssueUrls(BaseCleaningRule):
    """
    Class that incorrectly sets issue_urls as a string parameter.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = ('Defines issue_urls as something other than a list of strings')
        super().__init__(issue_numbers=['AA-000'],
                         issue_urls='www.fake_url.com',
                         description=desc,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class BadDescription(BaseCleaningRule):
    """
    Class that incorrectly sets description as something other than a string.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        super().__init__(issue_numbers=['AA-000'],
                         issue_urls=['www.fake_url.com'],
                         description=88,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class BadAffectedDatasets(BaseCleaningRule):
    """
    Class that incorrectly sets affected datasets with a non-string list item.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = (
            'Defines affected_datasets as something other than a list of strings'
        )
        super().__init__(issue_numbers=['AA-000'],
                         issue_urls=['www.fake_url.com'],
                         description=desc,
                         affected_datasets=['yo_mama', 88],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class InheritanceWithDependency(BaseCleaningRule):
    """
    Class that shows how to set the optional dependency list.

    It is setting the correctly extending Inheritance class as a dependency class.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = ('Test class with depends_on parameter set')
        super().__init__(issue_numbers=['AA-000'],
                         issue_urls=['www.fake_url.com'],
                         description=desc,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[Inheritance])

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class NoInheritance(object):
    """
    Class that is not inheriting from the BaseCleaningRule.
    """

    def __init__(self):
        pass


class NoAbstractMethodDefinitions(BaseCleaningRule):
    """
    Class that does not correctly override abstractmethods from BaseCleaningRule.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = ('Does not fully implement BaseCleaningRule and is not abstract')
        super().__init__(issue_numbers=['AA-000'],
                         issue_urls=['www.fake_url.com'],
                         description=desc,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[Inheritance])


class InheritanceWithBadDependencyClass(BaseCleaningRule):
    """
    Example of using a non-cleaning rule class as a dependency.  Incorrect usage.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = ('Setting a dependency on a non-cleaning rule class')
        super().__init__(issue_numbers=['AA-000'],
                         description=desc,
                         issue_urls=['www.fake_url.com'],
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[Inheritance, NoInheritance])

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class InheritanceWithBadDependencyType(BaseCleaningRule):
    """
    Class that sets a non class parameter for depends_on.  Incorrect usage.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        desc = ('Sets depends_on to a non-class object')
        super().__init__(issue_numbers=['AA-00'],
                         issue_urls=['www.fake_url.com'],
                         description=desc,
                         affected_datasets=['yo_mama'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[Inheritance, 99])

    def get_query_specs(self):
        pass

    def setup_rule(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class NoSuperInitialization(BaseCleaningRule):
    """
    Class that does not call super during initialization.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.sandbox_dataset_id = sandbox_dataset_id

    def get_query_specs(self):
        _ = self.project_id
        _ = self.issue_numbers

    def setup_rule(self):
        _ = self.project_id
        _ = self.dataset_id
        _ = self.sandbox_dataset_id
        _ = self.issue_numbers
        _ = self.affected_datasets
        _ = self.issue_urls

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


class BaseCleaningRuleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo'
        self.dataset_id = 'bar'
        self.sandbox_dataset_id = 'baz'
        self.issue_numbers = ['AA-000']
        self.description = 'phony cleaning rule'
        self.affected_datasets = ['yo_mama']
        self.depends_on = [Inheritance]

    def test_instantiating_base_class(self):
        """
        Prove that the base class can't be instantiated.
        """

        self.assertRaises(TypeError, BaseCleaningRule, self.issue_numbers,
                          self.description, self.affected_datasets,
                          self.project_id, self.dataset_id,
                          self.sandbox_dataset_id)

    def test_instantiating_inheriting_class_correctly(self):
        """
        Prove how to correctly inherit from the base class.
        """
        # tests
        alpha = Inheritance(self.project_id, self.dataset_id,
                            self.sandbox_dataset_id)
        beta = InheritanceWithDependency(self.project_id, self.dataset_id,
                                         self.sandbox_dataset_id)

        # post conditions
        for clazz in [alpha, beta]:
            self.assertEqual(self.project_id, clazz.project_id)
            self.assertEqual(self.issue_numbers, clazz.issue_numbers)
            self.assertEqual(self.affected_datasets, clazz.affected_datasets)
            self.assertEqual(self.dataset_id, clazz.dataset_id)
            self.assertEqual(self.sandbox_dataset_id, clazz.sandbox_dataset_id)

        self.assertEqual([], alpha.depends_on_classes)
        self.assertEqual(self.depends_on, beta.depends_on_classes)

    def test_instantiating_inheriting_class_incorrectly(self):
        """
        Test forgetting to define some abstract class methods.
        """
        # No defined abstract methods
        self.assertRaises(TypeError, NoAbstractMethodDefinitions,
                          self.project_id, self.dataset_id,
                          self.sandbox_dataset_id)

        # Not using super in initialization
        self.assertRaises(AttributeError, NoSuperInitialization,
                          self.project_id, self.dataset_id,
                          self.sandbox_dataset_id)

    def test_passing_bad_constructor_arguments(self):
        """
        Test that bad constructor arguments raise errors.
        """
        # These bad constructor arguments are explicitly passed to
        # the class under test
        self.assertRaises(TypeError, Inheritance, 99, self.dataset_id,
                          self.sandbox_dataset_id)

        self.assertRaises(TypeError, Inheritance, self.project_id, 99,
                          self.sandbox_dataset_id)

        self.assertRaises(TypeError, Inheritance, self.project_id,
                          self.dataset_id, 99)

        self.assertRaises(TypeError, InheritanceWithBadDependencyClass,
                          self.project_id, self.dataset_id,
                          self.sandbox_dataset_id)

        self.assertRaises(TypeError, InheritanceWithBadDependencyType,
                          self.project_id, self.dataset_id,
                          self.sandbox_dataset_id)

        # These constructor arguments being tested, are defined when
        # the class is written
        self.assertRaises(TypeError, BadAffectedDatasets, self.project_id,
                          self.dataset_id, self.sandbox_dataset_id)

        self.assertRaises(TypeError, BadDescription, self.project_id,
                          self.dataset_id, self.sandbox_dataset_id)

        self.assertRaises(TypeError, BadIssueNumbers, self.project_id,
                          self.dataset_id, self.sandbox_dataset_id)

        self.assertRaises(TypeError, BadIssueUrls, self.project_id,
                          self.dataset_id, self.sandbox_dataset_id)

    @patch.object(Inheritance, 'get_query_specs')
    def test_log_queries(self, mock_query_list):
        """
        Test logging queries and its error handling.
        """
        mock_query_list.return_value = [{
            'query': 'select count(*) from `foo.bar.baz`'
        }, {}]

        alpha = Inheritance(self.project_id, self.dataset_id,
                            self.sandbox_dataset_id)

        with self.assertLogs(level='INFO') as cm:
            # test
            alpha.log_queries()

            # post conditions
            self.assertEqual(cm.output, [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                'select count(*) from `foo.bar.baz`',
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                'NO QUERY FOUND'
            ])

    @patch.object(Inheritance, 'get_query_specs')
    def test_log_queries_all_errors(self, mock_query_list):
        """
        Test logging queries and its error handling.
        """
        error_list = [
            KeyError('bad fake key'),
            oauth2client.client.HttpAccessTokenRefreshError(
                'bad refresh token'),
            googleapiclient.errors.HttpError(b'404', b'bad http error')
        ]
        mock_query_list.side_effect = error_list

        alpha = Inheritance(self.project_id, self.dataset_id,
                            self.sandbox_dataset_id)

        for err in error_list:
            with self.assertLogs(level='INFO') as cm:
                # test
                self.assertRaises(err.__class__, alpha.log_queries)

                # post conditions
                expected_msg = (
                    'ERROR:cdr_cleaner.cleaning_rules.base_cleaning_rule:'
                    'Cannot list queries for Inheritance')
                self.assertEqual([cm.output[0][:len(expected_msg)]],
                                 [expected_msg])

    def test_get_delete_empty_sandbox_tables_queries(self):

        sandbox_condition_table = 'condition'
        sandbox_procedure_table = 'procedure'
        sandbox_tablenames = [sandbox_condition_table, sandbox_procedure_table]

        expected_table_ids = f'"{sandbox_condition_table}","{sandbox_procedure_table}"'
        expected_query = [{
            cdr_consts.QUERY:
                DROP_EMPTY_SANDBOX_TABLES_QUERY.render(
                    project=self.project_id,
                    dataset=self.sandbox_dataset_id,
                    table_ids=expected_table_ids),
            cdr_consts.DESTINATION_DATASET:
                self.sandbox_dataset_id
        }]

        actual_query = get_delete_empty_sandbox_tables_queries(
            self.project_id, self.sandbox_dataset_id, sandbox_tablenames)
        self.assertEqual(actual_query, expected_query)

        # Return an empty list if the sandbox_names is empty
        actual_query = get_delete_empty_sandbox_tables_queries(
            self.project_id, self.sandbox_dataset_id, [])

        self.assertEqual(actual_query, [])
